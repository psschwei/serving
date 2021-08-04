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
	"net/http"

	"go.uber.org/zap"
)

// ConcurrencyStateHandler tracks the in flight requests for the pod. When the requests
// drop to zero, it runs the `pause` function, and when requests scale up from zero, it
// runs the `resume` function. If either of `pause` or `resume` are not passed, it runs
// the respective local function(s). The local functions are the expected behavior; the
// function parameters are enabled primarily for testing purposes.
func ConcurrencyStateHandler(logger *zap.SugaredLogger, h http.Handler, pause, resume func(string) error, endpoint string) http.HandlerFunc {
	logger.Info("Concurrency state endpoint set, tracking request counts, using endpoint: ", endpoint)

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
					if err := pause(endpoint); err != nil {
						logger.Errorf("Error handling pause request: %v", err)
						panic(err)
					}
					logger.Info("To-Zero request successfully processed")
				}

			case r := <-reqCh:
				inFlight++
				if inFlight == 1 {
					logger.Info("Requests increased from zero")
					if err := resume(endpoint); err != nil {
						logger.Errorf("Error handling resume request: %v", err)
						panic(err)
					}
					logger.Info("From-Zero request successfully processed")
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

// Pause sends to an endpoint when request counts drop to zero
func Pause(endpoint string) error {
	action := ConcurrencyStateMessageBody{Action: "pause"}
	bodyText, err := json.Marshal(action)
	if err != nil {
		return fmt.Errorf("unable to create request body: %w", err)
	}
	body := bytes.NewBuffer(bodyText)
	req, err := http.NewRequest(http.MethodPost, endpoint, body)
	if err != nil {
		return fmt.Errorf("unable to create request: %w", err)
	}
	req.Header.Add("Token", "nil") // TODO: use serviceaccountToken from projected volume
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("unable to post request: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("expected 200 response, got: %d: %s", resp.StatusCode, resp.Status)
	}

	return nil
}

// Resume sends to an endpoint when request counts increase from zero
func Resume(endpoint string) error {
	action := ConcurrencyStateMessageBody{Action: "resume"}
	bodyText, err := json.Marshal(action)
	if err != nil {
		return fmt.Errorf("unable to create request body: %w", err)
	}
	body := bytes.NewBuffer(bodyText)
	req, err := http.NewRequest(http.MethodPost, endpoint, body)
	if err != nil {
		return fmt.Errorf("unable to create request: %w", err)
	}
	req.Header.Add("Token", "nil") // TODO: use serviceaccountToken from projected volume
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("unable to post request: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("expected 200 response, got: %d: %s", resp.StatusCode, resp.Status)
	}

	return nil
}

type ConcurrencyStateMessageBody struct {
	Action string `json:"action"`
}
