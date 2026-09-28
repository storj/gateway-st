// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"storj.io/gateway/api"
	"storj.io/minio/cmd"
)

func TestErrorLogsRequestID(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	objectAPI := &fakeObjectLayer{
		listBuckets: func(context.Context) ([]cmd.BucketInfo, error) {
			return nil, errors.New("unexpected")
		},
	}
	router := mux.NewRouter()
	api.New(objectAPI, testCredentialsProvider{}, api.Config{}, api.WithLogger(api.NewZapLogger(zap.New(core)))).RegisterHandlers(router)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	signV4(req, nil, time.Now())
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())

	requestID := rec.Header().Get("X-Amz-Request-Id")
	require.NotEmpty(t, requestID)
	require.NotZero(t, logs.Len())
	for _, entry := range logs.All() {
		require.Equal(t, requestID, entry.ContextMap()["amz_request_id"], entry.Message)
	}
}
