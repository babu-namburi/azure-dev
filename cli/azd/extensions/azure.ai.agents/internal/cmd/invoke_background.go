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
	"strings"
	"time"
)

const (
	backgroundCursorPersistInterval   = 3 * time.Second
	backgroundCursorPersistEventCount = 64
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

func isResponseLifecycleEvent(eventType string) bool {
	switch eventType {
	case "response.created", "response.queued", "response.in_progress",
		"response.completed", "response.failed", "response.incomplete", "response.cancelled":
		return true
	default:
		return false
	}
}

type backgroundProgressPersister struct {
	store              responseStateStore
	agentKey           string
	sessionID          string
	conversationID     string
	writer             io.Writer
	now                func() time.Time
	latest             savedBackgroundResponse
	persistedResponse  string
	persistedStatus    string
	lastPersistedAt    time.Time
	eventsSincePersist int
	printedResponseID  bool
	dirty              bool
}

func newBackgroundProgressPersister(
	store responseStateStore,
	agentKey string,
	sessionID string,
	conversationID string,
	writer io.Writer,
) *backgroundProgressPersister {
	return &backgroundProgressPersister{
		store:          store,
		agentKey:       agentKey,
		sessionID:      sessionID,
		conversationID: conversationID,
		writer:         writer,
		now:            time.Now,
	}
}

func (p *backgroundProgressPersister) Resume(record savedBackgroundResponse) {
	p.latest = record
	p.persistedResponse = record.ResponseID
	p.persistedStatus = record.Status
	p.lastPersistedAt = p.now()
	p.printedResponseID = true
	p.dirty = false
}

func (p *backgroundProgressPersister) Apply(ctx context.Context, progress responsesStreamProgress) error {
	if progress.ResponseID == "" {
		return nil
	}

	p.latest = savedBackgroundResponse{
		ResponseID:         progress.ResponseID,
		LastSequenceNumber: progress.Cursor,
		Status:             progress.Status,
		SessionID:          p.sessionID,
		ConversationID:     p.conversationID,
	}
	p.dirty = true
	p.eventsSincePersist++
	now := p.now()
	shouldPersist := p.persistedResponse == "" ||
		isResponseLifecycleEvent(progress.EventType) ||
		progress.Status != p.persistedStatus ||
		progress.Terminal ||
		p.eventsSincePersist >= backgroundCursorPersistEventCount ||
		(!p.lastPersistedAt.IsZero() && now.Sub(p.lastPersistedAt) >= backgroundCursorPersistInterval)
	if !shouldPersist {
		return nil
	}
	return p.persist(ctx, now)
}

func (p *backgroundProgressPersister) Flush(ctx context.Context) error {
	if !p.dirty {
		return nil
	}
	return p.persist(ctx, p.now())
}

func (p *backgroundProgressPersister) persist(ctx context.Context, now time.Time) error {
	if err := p.store.Save(ctx, p.agentKey, p.latest); err != nil {
		return err
	}
	if !p.printedResponseID {
		if _, err := fmt.Fprintf(p.writer, "Response:     %s\n", p.latest.ResponseID); err != nil {
			return err
		}
		p.printedResponseID = true
	}
	p.persistedResponse = p.latest.ResponseID
	p.persistedStatus = p.latest.Status
	p.lastPersistedAt = now
	p.eventsSincePersist = 0
	p.dirty = false
	return nil
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

func printTerminalResponseStatus(writer io.Writer, record *savedBackgroundResponse) (bool, error) {
	if !isTerminalResponseStatus(record.Status) {
		return false, nil
	}
	_, err := fmt.Fprintf(writer, "Status: %s\n", record.Status)
	return true, err
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
	if terminal, err := printTerminalResponseStatus(os.Stdout, record); terminal || err != nil {
		return err
	}

	progressPersister := newBackgroundProgressPersister(
		store,
		rc.agentKey,
		record.SessionID,
		record.ConversationID,
		os.Stdout,
	)
	progressPersister.Resume(*record)

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
			record.LastSequenceNumber,
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
			if progress.ResponseID == "" {
				progress.ResponseID = record.ResponseID
			}
			if progress.Status == "" {
				progress.Status = record.Status
			}
			return progressPersister.Apply(ctx, progress)
		}
		streamErr := readResponsesSSEWithInitialState(
			ctx,
			resp.Body,
			os.Stdout,
			rc.name,
			true,
			&responsesStreamInitialState{
				ResponseID: record.ResponseID,
				Cursor:     record.LastSequenceNumber,
				Status:     record.Status,
			},
			onProgress,
		)
		_ = resp.Body.Close()
		var flushErr error
		if ctx.Err() == nil {
			flushErr = progressPersister.Flush(ctx)
		}
		if progressPersister.latest.ResponseID != "" {
			latest := progressPersister.latest
			record = &latest
		}
		if streamErr == nil && flushErr == nil {
			return nil
		}
		if flushErr != nil {
			streamErr = errors.Join(streamErr, flushErr)
		}
		if errors.Is(streamErr, errResponsesStreamEndedBeforeIdentity) {
			if snapshotErr := a.retrieveResponseSnapshot(ctx, rc, store, record); snapshotErr != nil {
				return errors.Join(
					fmt.Errorf("Response %s returned no new stream events", record.ResponseID),
					snapshotErr,
				)
			}
			if terminal, statusErr := printTerminalResponseStatus(os.Stdout, record); terminal || statusErr != nil {
				return statusErr
			}
			return fmt.Errorf(
				"Response %s is still %s, but no new stream events were available; try `azd ai agent invoke --continue` again",
				record.ResponseID,
				record.Status,
			)
		}
		latest, loadErr := store.Get(ctx, rc.agentKey)
		if loadErr != nil {
			return errors.Join(streamErr, loadErr)
		}
		if latest != nil {
			record = latest
		}
		if terminal, statusErr := printTerminalResponseStatus(os.Stdout, record); terminal || statusErr != nil {
			return statusErr
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
		handled, handleErr := handleCompletedResponseCancelError(
			ctx,
			store,
			rc.agentKey,
			record,
			resp.StatusCode,
			body,
			os.Stdout,
		)
		if handleErr != nil {
			return handleErr
		}
		if handled {
			return nil
		}
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

func isRetryableBackgroundStreamError(err error) bool {
	return errors.Is(err, errResponsesStreamDisconnected)
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

func handleCompletedResponseCancelError(
	ctx context.Context,
	store responseStateStore,
	agentKey string,
	record *savedBackgroundResponse,
	statusCode int,
	body []byte,
	writer io.Writer,
) (bool, error) {
	if statusCode != http.StatusBadRequest {
		return false, nil
	}
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
			Param   string `json:"param"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil ||
		envelope.Error.Code != "invalid_request_error" ||
		envelope.Error.Param != "response_id" ||
		!strings.EqualFold(strings.TrimSpace(envelope.Error.Message), "Cannot cancel a completed response.") {
		return false, nil
	}

	record.Status = "completed"
	if err := store.Save(ctx, agentKey, *record); err != nil {
		return true, fmt.Errorf("save completed Response state: %w", err)
	}
	if _, err := fmt.Fprintf(
		writer,
		"Response %s has already completed; nothing to cancel.\n",
		record.ResponseID,
	); err != nil {
		return true, err
	}
	return true, nil
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
