// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

var errBackgroundNoWait = errors.New("background Response identity saved")

func isTerminalResponseStatus(status string) bool {
	switch status {
	case "completed", "failed", "incomplete", "cancelled":
		return true
	default:
		return false
	}
}

func guardNoActiveBackgroundResponse(
	ctx context.Context,
	store responseStateStore,
	agentKey string,
) error {
	record, err := store.Get(ctx, agentKey)
	if err != nil {
		return fmt.Errorf("check current background Response: %w", err)
	}
	if record == nil || isTerminalResponseStatus(record.Status) {
		return nil
	}
	return fmt.Errorf(
		"background Response %s is still active; follow it with `azd ai agent invoke --continue` or stop it with "+
			"`azd ai agent invoke --cancel`",
		record.ResponseID,
	)
}

func (a *InvokeAction) responsesContinueRemote(ctx context.Context) error {
	rc, store, record, err := a.resolveSavedBackgroundResponse(ctx)
	if err != nil {
		return err
	}
	defer rc.azdClient.Close()

	fmt.Printf("Agent:        %s (remote)\n", rc.name)
	fmt.Printf("Response:     %s\n", record.ResponseID)
	fmt.Printf("Session:      %s\n", record.SessionID)
	fmt.Printf("Conversation: %s\n\n", record.ConversationID)

	const maxReconnectAttempts = 5
	for attempt := range maxReconnectAttempts {
		rc.bearerToken, err = a.acquireBearerToken(ctx)
		if err != nil {
			return err
		}
		followURL := buildResponseLifecycleURL(
			rc.projectEndpoint,
			rc.name,
			record.ResponseID,
			rc.apiVersion,
			true,
			record.Cursor,
		)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, followURL, nil)
		if err != nil {
			return fmt.Errorf("create Response follow request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+rc.bearerToken)
		req.Header.Set("Accept", "text/event-stream")
		applyCustomHeaders(req, a.clientHeaders)
		applyRemoteUserIdentityHeader(req, &a.flags.userIdentityFlags)

		resp, err := backgroundHTTPClient().Do(req) //nolint:gosec // endpoint is validated by remote context resolution
		if err != nil {
			if attempt+1 == maxReconnectAttempts {
				return fmt.Errorf("follow background Response %s: %w", record.ResponseID, err)
			}
			if err := sleepWithContext(ctx, reconnectDelay(attempt)); err != nil {
				return err
			}
			continue
		}

		if resp.StatusCode >= 400 {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if isRetryableResponseStatus(resp.StatusCode) && attempt+1 < maxReconnectAttempts {
				if err := sleepWithContext(ctx, retryDelay(resp, attempt)); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("GET %s failed with HTTP %d: %s\n%s", followURL, resp.StatusCode, resp.Status, body)
		}

		onProgress := func(progress responsesStreamProgress) error {
			if progress.ResponseID != "" {
				record.ResponseID = progress.ResponseID
			}
			if progress.Cursor != nil {
				record.Cursor = progress.Cursor
			}
			if progress.Status != "" {
				record.Status = progress.Status
			}
			return store.Save(ctx, rc.agentKey, *record)
		}
		streamErr := readResponsesSSE(ctx, resp.Body, os.Stdout, rc.name, true, onProgress)
		_ = resp.Body.Close()
		if streamErr == nil {
			return nil
		}
		latest, loadErr := store.Get(ctx, rc.agentKey)
		if loadErr != nil {
			return errors.Join(streamErr, loadErr)
		}
		if latest != nil {
			record = latest
		}
		if isTerminalResponseStatus(record.Status) {
			return streamErr
		}
		if attempt+1 == maxReconnectAttempts {
			if fallbackErr := a.retrieveResponseSnapshot(ctx, rc, store, record); fallbackErr == nil {
				return nil
			} else {
				return errors.Join(streamErr, fallbackErr)
			}
		}
		if err := sleepWithContext(ctx, reconnectDelay(attempt)); err != nil {
			return err
		}
	}
	return nil
}

func (a *InvokeAction) responsesCancelRemote(ctx context.Context) error {
	rc, store, record, err := a.resolveSavedBackgroundResponse(ctx)
	if err != nil {
		return err
	}
	defer rc.azdClient.Close()

	if isTerminalResponseStatus(record.Status) {
		fmt.Printf("Response %s is already %s.\n", record.ResponseID, record.Status)
		return nil
	}
	rc.bearerToken, err = a.acquireBearerToken(ctx)
	if err != nil {
		return err
	}
	cancelURL := buildResponseCancelURL(rc.projectEndpoint, rc.name, record.ResponseID, rc.apiVersion)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cancelURL, nil)
	if err != nil {
		return fmt.Errorf("create Response cancel request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+rc.bearerToken)
	applyCustomHeaders(req, a.clientHeaders)
	applyRemoteUserIdentityHeader(req, &a.flags.userIdentityFlags)

	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req) //nolint:gosec // validated Foundry endpoint
	if err != nil {
		return fmt.Errorf("cancel background Response %s: %w", record.ResponseID, err)
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return fmt.Errorf("read cancel response: %w", readErr)
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("POST %s failed with HTTP %d: %s\n%s", cancelURL, resp.StatusCode, resp.Status, body)
	}

	snapshot, decodeErr := decodeResponseSnapshot(body)
	if decodeErr == nil && snapshot.Status != "" {
		record.Status = snapshot.Status
	} else {
		record.Status = "cancelled"
	}
	if err := store.Save(ctx, rc.agentKey, *record); err != nil {
		return fmt.Errorf("save cancelled Response state: %w", err)
	}
	fmt.Printf("Response %s is %s.\n", record.ResponseID, record.Status)
	return nil
}

func (a *InvokeAction) resolveSavedBackgroundResponse(
	ctx context.Context,
) (*remoteContext, responseStateStore, *savedBackgroundResponse, error) {
	rc, err := a.resolveRemoteContext(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	if rc.azdClient == nil || rc.agentKey == "" {
		if rc.azdClient != nil {
			rc.azdClient.Close()
		}
		return nil, nil, nil, fmt.Errorf("background Responses require project-backed local state")
	}
	store := newUserConfigResponseStateStore(rc.azdClient)
	record, err := store.Get(ctx, rc.agentKey)
	if err != nil {
		rc.azdClient.Close()
		return nil, nil, nil, fmt.Errorf("load current background Response: %w", err)
	}
	if record == nil || record.ResponseID == "" {
		rc.azdClient.Close()
		return nil, nil, nil, fmt.Errorf(
			"no saved background Response found; start one with `azd ai agent invoke --background \"<message>\"`",
		)
	}
	return rc, store, record, nil
}

func (a *InvokeAction) retrieveResponseSnapshot(
	ctx context.Context,
	rc *remoteContext,
	store responseStateStore,
	record *savedBackgroundResponse,
) error {
	token, err := a.acquireBearerToken(ctx)
	if err != nil {
		return err
	}
	snapshotURL := buildResponseLifecycleURL(
		rc.projectEndpoint,
		rc.name,
		record.ResponseID,
		rc.apiVersion,
		false,
		nil,
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, snapshotURL, nil)
	if err != nil {
		return fmt.Errorf("create Response snapshot request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	applyCustomHeaders(req, a.clientHeaders)
	applyRemoteUserIdentityHeader(req, &a.flags.userIdentityFlags)
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req) //nolint:gosec // validated Foundry endpoint
	if err != nil {
		return fmt.Errorf("retrieve Response snapshot: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read Response snapshot: %w", err)
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("GET %s failed with HTTP %d: %s\n%s", snapshotURL, resp.StatusCode, resp.Status, body)
	}
	snapshot, err := decodeResponseSnapshot(body)
	if err != nil {
		return fmt.Errorf("decode Response snapshot: %w", err)
	}
	if snapshot.ID != "" && snapshot.ID != record.ResponseID {
		return fmt.Errorf("Response snapshot ID %q does not match saved ID %q", snapshot.ID, record.ResponseID)
	}
	if snapshot.Status != "" {
		record.Status = snapshot.Status
	}
	if err := store.Save(ctx, rc.agentKey, *record); err != nil {
		return err
	}
	return writeResponsesSnapshot(os.Stdout, rc.name, snapshot, body)
}

func backgroundHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 30 * time.Second
	return &http.Client{Transport: transport}
}

func isRetryableResponseStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
}

func reconnectDelay(attempt int) time.Duration {
	return min(time.Second<<attempt, 30*time.Second)
}

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
		if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds > 0 {
			return min(time.Duration(seconds)*time.Second, 30*time.Second)
		}
		if retryAt, err := http.ParseTime(retryAfter); err == nil {
			return min(max(time.Until(retryAt), time.Second), 30*time.Second)
		}
	}
	return reconnectDelay(attempt)
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func persistAndPrintBackgroundProgress(
	ctx context.Context,
	store responseStateStore,
	agentKey string,
	record savedBackgroundResponse,
	printedResponseID *string,
	writer io.Writer,
) error {
	if err := store.Save(ctx, agentKey, record); err != nil {
		return err
	}
	if *printedResponseID == "" {
		if _, err := fmt.Fprintf(writer, "Response:     %s\n", record.ResponseID); err != nil {
			return err
		}
		*printedResponseID = record.ResponseID
	}
	return nil
}

func decodeResponseSnapshot(body []byte) (responsesSnapshot, error) {
	var envelope struct {
		Response json.RawMessage `json:"response"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil && len(envelope.Response) > 0 {
		body = envelope.Response
	}
	var snapshot responsesSnapshot
	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&snapshot); err != nil {
		return responsesSnapshot{}, err
	}
	return snapshot, nil
}
