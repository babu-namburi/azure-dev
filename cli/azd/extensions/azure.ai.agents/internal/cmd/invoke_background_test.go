// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package cmd

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

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

func TestPersistAndPrintBackgroundProgressSavesBeforePrinting(t *testing.T) {
	t.Parallel()

	store := &orderingResponseStore{}
	writer := &afterSaveWriter{store: store}
	printedID := ""
	err := persistAndPrintBackgroundProgress(
		t.Context(),
		store,
		"agent-key",
		savedBackgroundResponse{ResponseID: "resp_123", Status: "in_progress"},
		&printedID,
		writer,
	)

	require.NoError(t, err)
	assert.True(t, store.saved)
	assert.Equal(t, "resp_123", printedID)
	assert.Equal(t, "Response:     resp_123\n", writer.output.String())
}

func TestPersistAndPrintBackgroundProgressDoesNotPrintWhenSaveFails(t *testing.T) {
	t.Parallel()

	store := &orderingResponseStore{saveErr: errors.New("write failed")}
	writer := &afterSaveWriter{store: store}
	printedID := ""
	err := persistAndPrintBackgroundProgress(
		t.Context(),
		store,
		"agent-key",
		savedBackgroundResponse{ResponseID: "resp_123"},
		&printedID,
		writer,
	)

	require.EqualError(t, err, "write failed")
	assert.Empty(t, writer.output.String())
	assert.Empty(t, printedID)
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
		{
			name:       "different status code",
			statusCode: 409,
			body:       `{"error":{"code":"invalid_request_error","message":"Cannot cancel a completed response.","param":"response_id"}}`,
		},
		{
			name:       "different message",
			statusCode: 400,
			body:       `{"error":{"code":"invalid_request_error","message":"Cannot cancel a failed response.","param":"response_id"}}`,
		},
		{
			name:       "different parameter",
			statusCode: 400,
			body:       `{"error":{"code":"invalid_request_error","message":"Cannot cancel a completed response.","param":"conversation_id"}}`,
		},
		{
			name:       "malformed body",
			statusCode: 400,
			body:       `not-json`,
		},
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
