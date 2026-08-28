// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package cmd

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvokeCommandBackgroundFlagRegistered(t *testing.T) {
	t.Parallel()

	flag := newInvokeCommand(nil).Flags().Lookup("background")
	require.NotNil(t, flag)
	assert.Equal(t, "false", flag.DefValue)
}

type orderingResponseStore struct {
	saved   bool
	saveErr error
	saves   []savedBackgroundResponse
	record  savedBackgroundResponse
}

func (s *orderingResponseStore) Get(context.Context, string) (*savedBackgroundResponse, error) {
	return nil, nil
}

func (s *orderingResponseStore) Save(_ context.Context, _ string, record savedBackgroundResponse) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	s.saved = true
	s.saves = append(s.saves, record)
	s.record = record
	return nil
}

func (s *orderingResponseStore) Delete(context.Context, string) error {
	return nil
}

type afterSaveWriter struct {
	store  *orderingResponseStore
	output strings.Builder
}

func (w *afterSaveWriter) Write(p []byte) (int, error) {
	if !w.store.saved {
		return 0, errors.New("response ID printed before state was saved")
	}
	return w.output.Write(p)
}

func newTestProgressPersister(
	store *orderingResponseStore,
	writer io.Writer,
	now func() time.Time,
) *backgroundProgressPersister {
	persister := newBackgroundProgressPersister(store, "agent-key", "sess_123", "conv_123", writer)
	persister.now = now
	return persister
}

func TestBackgroundProgressPersisterSavesBeforePrinting(t *testing.T) {
	t.Parallel()

	store := &orderingResponseStore{}
	writer := &afterSaveWriter{store: store}
	now := time.Unix(1_000, 0)
	persister := newTestProgressPersister(store, writer, func() time.Time { return now })
	err := persister.Apply(t.Context(), responsesStreamProgress{
		ResponseID: "resp_123",
		Cursor:     new(int64(0)),
		Status:     "in_progress",
		EventType:  "response.created",
	})

	require.NoError(t, err)
	require.Len(t, store.saves, 1)
	assert.Equal(t, "resp_123", store.saves[0].ResponseID)
	assert.Equal(t, "Response:     resp_123\n", writer.output.String())
}

func TestBackgroundProgressPersisterDoesNotPrintWhenSaveFails(t *testing.T) {
	t.Parallel()

	store := &orderingResponseStore{saveErr: errors.New("write failed")}
	writer := &afterSaveWriter{store: store}
	persister := newTestProgressPersister(store, writer, time.Now)
	err := persister.Apply(t.Context(), responsesStreamProgress{
		ResponseID: "resp_123",
		Cursor:     new(int64(0)),
		Status:     "in_progress",
		EventType:  "response.created",
	})

	require.EqualError(t, err, "write failed")
	assert.Empty(t, writer.output.String())
}

func TestBackgroundProgressPersisterThrottlesOrdinaryEvents(t *testing.T) {
	t.Parallel()

	store := &orderingResponseStore{}
	writer := &afterSaveWriter{store: store}
	now := time.Unix(1_000, 0)
	persister := newTestProgressPersister(store, writer, func() time.Time { return now })
	require.NoError(t, persister.Apply(t.Context(), responsesStreamProgress{
		ResponseID: "resp_123",
		Cursor:     new(int64(0)),
		Status:     "in_progress",
		EventType:  "response.created",
	}))

	for sequenceNumber := int64(1); sequenceNumber < backgroundCursorPersistEventCount; sequenceNumber++ {
		require.NoError(t, persister.Apply(t.Context(), responsesStreamProgress{
			ResponseID: "resp_123",
			Cursor:     new(sequenceNumber),
			Status:     "in_progress",
			EventType:  "response.output_text.delta",
		}))
	}
	assert.Len(t, store.saves, 1)

	require.NoError(t, persister.Apply(t.Context(), responsesStreamProgress{
		ResponseID: "resp_123",
		Cursor:     new(int64(backgroundCursorPersistEventCount)),
		Status:     "in_progress",
		EventType:  "response.output_text.delta",
	}))
	require.Len(t, store.saves, 2)
	assert.Equal(t, int64(backgroundCursorPersistEventCount), *store.saves[1].LastSequenceNumber)
}

func TestBackgroundProgressPersisterPersistsAfterInterval(t *testing.T) {
	t.Parallel()

	store := &orderingResponseStore{}
	writer := &afterSaveWriter{store: store}
	now := time.Unix(1_000, 0)
	persister := newTestProgressPersister(store, writer, func() time.Time { return now })
	require.NoError(t, persister.Apply(t.Context(), responsesStreamProgress{
		ResponseID: "resp_123", Cursor: new(int64(0)), Status: "in_progress", EventType: "response.created",
	}))

	now = now.Add(backgroundCursorPersistInterval - time.Millisecond)
	require.NoError(t, persister.Apply(t.Context(), responsesStreamProgress{
		ResponseID: "resp_123", Cursor: new(int64(1)), Status: "in_progress", EventType: "response.output_text.delta",
	}))
	assert.Len(t, store.saves, 1)

	now = now.Add(time.Millisecond)
	require.NoError(t, persister.Apply(t.Context(), responsesStreamProgress{
		ResponseID: "resp_123", Cursor: new(int64(2)), Status: "in_progress", EventType: "response.output_text.delta",
	}))
	assert.Len(t, store.saves, 2)
}

func TestBackgroundProgressPersisterPersistsLifecycleAndTerminalEvents(t *testing.T) {
	t.Parallel()

	store := &orderingResponseStore{}
	writer := &afterSaveWriter{store: store}
	now := time.Unix(1_000, 0)
	persister := newTestProgressPersister(store, writer, func() time.Time { return now })
	require.NoError(t, persister.Apply(t.Context(), responsesStreamProgress{
		ResponseID: "resp_123", Cursor: new(int64(0)), Status: "in_progress", EventType: "response.created",
	}))
	require.NoError(t, persister.Apply(t.Context(), responsesStreamProgress{
		ResponseID: "resp_123", Cursor: new(int64(1)), Status: "in_progress", EventType: "response.in_progress",
	}))
	require.NoError(t, persister.Apply(t.Context(), responsesStreamProgress{
		ResponseID: "resp_123", Cursor: new(int64(2)), Status: "completed", EventType: "response.completed", Terminal: true,
	}))

	require.Len(t, store.saves, 3)
	assert.Equal(t, int64(0), *store.saves[0].LastSequenceNumber)
	assert.Equal(t, int64(1), *store.saves[1].LastSequenceNumber)
	assert.Equal(t, int64(2), *store.saves[2].LastSequenceNumber)
	assert.Equal(t, "completed", store.saves[2].Status)
}

func TestBackgroundProgressPersisterFlushesPendingCursor(t *testing.T) {
	t.Parallel()

	store := &orderingResponseStore{}
	writer := &afterSaveWriter{store: store}
	now := time.Unix(1_000, 0)
	persister := newTestProgressPersister(store, writer, func() time.Time { return now })
	require.NoError(t, persister.Apply(t.Context(), responsesStreamProgress{
		ResponseID: "resp_123", Cursor: new(int64(0)), Status: "in_progress", EventType: "response.created",
	}))
	require.NoError(t, persister.Apply(t.Context(), responsesStreamProgress{
		ResponseID: "resp_123", Cursor: new(int64(1)), Status: "in_progress", EventType: "response.output_text.delta",
	}))
	assert.Len(t, store.saves, 1)

	require.NoError(t, persister.Flush(t.Context()))
	require.Len(t, store.saves, 2)
	assert.Equal(t, int64(1), *store.saves[1].LastSequenceNumber)
}

func TestHandleCompletedResponseCancelError(t *testing.T) {
	t.Parallel()

	store := &orderingResponseStore{}
	record := &savedBackgroundResponse{ResponseID: "resp_123", Status: "in_progress"}
	var output strings.Builder
	handled, err := handleCompletedResponseCancelError(
		t.Context(),
		store,
		"agent-key",
		record,
		400,
		[]byte(`{"error":{"code":"invalid_request_error","message":"Cannot cancel a completed response.","param":"response_id","type":"invalid_request_error"}}`),
		&output,
	)

	require.NoError(t, err)
	assert.True(t, handled)
	assert.True(t, store.saved)
	assert.Equal(t, "completed", store.record.Status)
	assert.Equal(t, "completed", record.Status)
	assert.Equal(t, "Response resp_123 has already completed; nothing to cancel.\n", output.String())
}

func TestHandleCompletedResponseCancelErrorDoesNotHandleOtherErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
		body       string
	}{
		{name: "different status code", statusCode: 409, body: `{"error":{"code":"invalid_request_error","message":"Cannot cancel a completed response.","param":"response_id"}}`},
		{name: "different message", statusCode: 400, body: `{"error":{"code":"invalid_request_error","message":"Cannot cancel a failed response.","param":"response_id"}}`},
		{name: "different parameter", statusCode: 400, body: `{"error":{"code":"invalid_request_error","message":"Cannot cancel a completed response.","param":"conversation_id"}}`},
		{name: "malformed body", statusCode: 400, body: `not-json`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := &orderingResponseStore{}
			record := &savedBackgroundResponse{ResponseID: "resp_123", Status: "in_progress"}
			var output strings.Builder
			handled, err := handleCompletedResponseCancelError(
				t.Context(), store, "agent-key", record, tt.statusCode, []byte(tt.body), &output,
			)
			require.NoError(t, err)
			assert.False(t, handled)
			assert.False(t, store.saved)
			assert.Empty(t, output.String())
			assert.Equal(t, "in_progress", record.Status)
		})
	}
}

func TestPrintTerminalResponseStatus(t *testing.T) {
	t.Parallel()

	for _, status := range []string{"completed", "failed", "incomplete", "cancelled"} {
		t.Run(status, func(t *testing.T) {
			t.Parallel()
			var output strings.Builder
			terminal, err := printTerminalResponseStatus(&output, &savedBackgroundResponse{
				ResponseID: "resp_123",
				Status:     status,
			})
			require.NoError(t, err)
			assert.True(t, terminal)
			assert.Equal(t, "Status: "+status+"\n", output.String())
		})
	}
}

func TestPrintTerminalResponseStatusIgnoresActiveResponse(t *testing.T) {
	t.Parallel()

	var output strings.Builder
	terminal, err := printTerminalResponseStatus(&output, &savedBackgroundResponse{
		ResponseID: "resp_123",
		Status:     "in_progress",
	})
	require.NoError(t, err)
	assert.False(t, terminal)
	assert.Empty(t, output.String())
}

func TestInvokeCommandBackgroundValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "rejects local",
			args: []string{"--background", "--local", "hello"},
			want: "supported only for remote Responses agents",
		},
		{
			name: "rejects explicit invocations protocol",
			args: []string{"--background", "--protocol", "invocations", "hello"},
			want: "background lifecycle operations are not supported with the invocations protocol",
		},
		{
			name: "rejects explicit timeout",
			args: []string{"--background", "--timeout", "60", "hello"},
			want: "--timeout is not supported with background lifecycle operations",
		},
		{
			name: "rejects empty session override",
			args: []string{"--continue", "--session-id="},
			want: "use the saved session and conversation",
		},
		{
			name: "rejects false new-session override",
			args: []string{"--continue", "--new-session=false"},
			want: "use the saved session and conversation",
		},
		{
			name: "rejects empty conversation override",
			args: []string{"--cancel", "--conversation-id="},
			want: "use the saved session and conversation",
		},
		{
			name: "rejects false new-conversation override",
			args: []string{"--cancel", "--new-conversation=false"},
			want: "use the saved session and conversation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cmd := newInvokeCommand(nil)
			cmd.SetArgs(tt.args)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			err := cmd.Execute()
			require.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), tt.want), "error %q should contain %q", err, tt.want)
		})
	}
}
