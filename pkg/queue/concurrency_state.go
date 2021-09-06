/*
Copyright 2021 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package queue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

const ConcurrencyStateTokenVolumeMountPath = "/var/run/secrets/tokens"
const FreezeMaxRetryTimes = 3 // If pause/resume failed 5 times, it should relaunch a pod

// error code for exec Pause/Resume function
const (
	noError                        = 0 // everything works
	internalError                  = 1 // some internal error happen, like json decode failed, should try again
	responseStatusConflictError    = 2 // like exec pause when the container is in pause state, should forget
	responseExecError              = 3 // the command exec failed in runtime level, should try again
)

type RetryElement struct {
	op        string
	endpoint  string
	timesNow  int8
}

// ConcurrencyStateHandler tracks the in flight requests for the pod. When the requests
// drop to zero, it runs the `pause` function, and when requests scale up from zero, it
// runs the `resume` function. If either of `pause` or `resume` are not passed, it runs
// the respective local function(s). The local functions are the expected behavior; the
// function parameters are enabled primarily for testing purposes.
func ConcurrencyStateHandler(logger *zap.SugaredLogger, h http.Handler, pause, resume func(string, *Token) (int8, error), endpoint string, tokenMountPath string) http.HandlerFunc {
	logger.Info("Concurrency state endpoint set, tracking request counts, using endpoint: ", endpoint)

	var tokenCfg Token
	refreshToken(logger, &tokenCfg, tokenMountPath)
	go func() {
		for {
			time.Sleep(1 * time.Minute)
			refreshToken(logger, &tokenCfg, tokenMountPath)
		}
	}()

	retryChannel := make(chan RetryElement, 1)
	retryDoneChannel := make(chan struct{}, 1)
	go FreezePodRetry(logger, retryChannel, retryDoneChannel, &tokenCfg, pause, resume)

	type req struct {
		w http.ResponseWriter
		r *http.Request

		done chan struct{}
	}

	reqCh := make(chan req)
	doneCh := make(chan struct{})
	go func() {
		inFlight := 0

		// This loop is entirely synchronous, so there's no cleverness needed in
		// ensuring open and close dont run at the same time etc. Only the
		// delegated ServeHTTP is done in a goroutine.
		for {
			select {
			case <-doneCh:
				inFlight--
				if inFlight == 0 {
					logger.Info("Requests dropped to zero")
					errCode, err := pause(endpoint, &tokenCfg)
					switch errCode {
					case internalError, responseExecError:
						logger.Errorf("Error handling pause request: %v, we will try it again", err)
						retryChannel <- RetryElement{"pause", endpoint, 0}
						<-retryDoneChannel
					case responseStatusConflictError:
						logger.Info("Error handling pause request: %v, it will be ignored", err)
					case noError:
						logger.Info("To-Zero request successfully processed")
					}
				}

			case r := <-reqCh:
				inFlight++
				if inFlight == 1 {
					logger.Info("Requests increased from zero")
					errCode, err := resume(endpoint, &tokenCfg)
					switch errCode {
					case internalError, responseExecError:
						logger.Errorf("Error handling resume request: %v, we will try it again", err)
						retryChannel <- RetryElement{"resume", endpoint, 0}
						<-retryDoneChannel
					case responseStatusConflictError:
						logger.Info("Error handling resume request: %v, it will be ignored", err)
					case noError:
						logger.Info("From-Zero request successfully processed")
					}
				}

				go func(r req) {
					h.ServeHTTP(r.w, r.r)
					close(r.done) // Return from ServeHTTP
					doneCh <- struct{}{}
				}(r)
			}
		}
	}()

	return func(w http.ResponseWriter, r *http.Request) {
		done := make(chan struct{})
		reqCh <- req{w, r, done}
		// Block till we've processed the request
		<-done
	}
}

// FreezePodRetry handle the failed request when call Resume/Pause
func FreezePodRetry(logger *zap.SugaredLogger, ch chan RetryElement, retryDoneChannel chan struct{}, token *Token, pause, resume func(string, *Token) (int8, error)) {
	for {
		elementNow := <-ch
		elementNow.timesNow += 1
		if elementNow.timesNow > FreezeMaxRetryTimes {
			// TODO: relaunch a new pod
			panic("Relaunch a pod")
		}
		switch elementNow.op {
		case "pause":
			errCode, err := pause(elementNow.endpoint, token)
			if errCode != responseStatusConflictError && errCode != noError {
				logger.Info("Error handling pause request: %v", err)
				ch <- elementNow
				time.Sleep(100 * time.Millisecond)
			} else {
				retryDoneChannel <- struct{}{}
			}
		case "resume":
			errCode, err := resume(elementNow.endpoint, token)
			if errCode != responseStatusConflictError && errCode != noError {
				logger.Info("Error handling resume request: %v", err)
				ch <- elementNow
				time.Sleep(100 * time.Millisecond)
			} else {
				retryDoneChannel <- struct{}{}
			}
		}
	}
}

// Pause sends to an endpoint when request counts drop to zero
func Pause(endpoint string, token *Token) (int8, error) {
	action := ConcurrencyStateMessageBody{Action: "pause"}
	bodyText, err := json.Marshal(action)
	if err != nil {
		return internalError, fmt.Errorf("unable to create request body: %w", err)
	}
	body := bytes.NewBuffer(bodyText)
	req, err := http.NewRequest(http.MethodPost, endpoint, body)
	if err != nil {
		return internalError, fmt.Errorf("unable to create request: %w", err)
	}
	req.Header.Add("Token", token.Get())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return internalError, fmt.Errorf("unable to post request: %w", err)
	}
	if resp.StatusCode == http.StatusConflict {
		return responseStatusConflictError, fmt.Errorf("expected container status is in running state, but actually not")
	}
	if resp.StatusCode != http.StatusOK {
		return responseExecError, fmt.Errorf("expected 200 response, got: %d: %s", resp.StatusCode, resp.Status)
	}

	return noError, nil
}

// Resume sends to an endpoint when request counts increase from zero
func Resume(endpoint string, token *Token) (int8, error) {
	action := ConcurrencyStateMessageBody{Action: "resume"}
	bodyText, err := json.Marshal(action)
	if err != nil {
		return internalError, fmt.Errorf("unable to create request body: %w", err)
	}
	body := bytes.NewBuffer(bodyText)
	req, err := http.NewRequest(http.MethodPost, endpoint, body)
	if err != nil {
		return internalError, fmt.Errorf("unable to create request: %w", err)
	}
	req.Header.Add("Token", token.Get())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return internalError, fmt.Errorf("unable to post request: %w", err)
	}
	if resp.StatusCode == http.StatusConflict {
		return responseStatusConflictError, fmt.Errorf("expected container status is in paused state, but actually not")
	}
	if resp.StatusCode != http.StatusOK {
		return responseExecError, fmt.Errorf("expected 200 response, got: %d: %s", resp.StatusCode, resp.Status)
	}

	return noError, nil
}

type Token struct {
	sync.RWMutex
	token string
}

func (t *Token) Set(token string) {
	t.Lock()
	defer t.Unlock()

	t.token = token
}

func (t *Token) Get() string {
	t.RLock()
	defer t.RUnlock()

	return t.token
}

func refreshToken(logger *zap.SugaredLogger, tokenCfg *Token, tokenMountPath string) {
	token, err := ioutil.ReadFile(tokenMountPath)
	if err != nil {
		logger.Fatal("could not read token: ", err)
	}
	tokenCfg.Set(string(token))
	logger.Info("refresh token...")
}

type ConcurrencyStateMessageBody struct {
	Action string `json:"action"`
}