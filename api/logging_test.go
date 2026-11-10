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
	"storj.io/gateway/api/apierr"
	"storj.io/minio/cmd"
	"storj.io/minio/cmd/logger"
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
		require.Equal(t, "ListBuckets", entry.ContextMap()["operation"], entry.Message)
	}
}

// TestReqInfoIsShared checks that middleware outside the API can read what the handlers fill in of
// the request's ReqInfo, which the object layer is called with too, like edge's logging does.
func TestReqInfoIsShared(t *testing.T) {
	var layerReqInfo *logger.ReqInfo
	objectAPI := &fakeObjectLayer{
		getBucketLocation: func(ctx context.Context, bucket string) (string, error) {
			layerReqInfo = logger.GetReqInfo(ctx)
			layerReqInfo.SetTags("error", "from the layer")
			return "", cmd.BucketNotFound{Bucket: bucket}
		},
	}
	// Middleware that limits PutObject, like edge's concurrency limiter.
	var s3API *api.API
	s3API = api.New(objectAPI, testCredentialsProvider{}, api.Config{}, api.WithMiddleware(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if api.Operation(r) == "PutObject" {
				s3API.WriteErrorResponse(w, r, apierr.Response{Code: "SlowDown", HTTPStatusCode: http.StatusTooManyRequests})
				return
			}
			next.ServeHTTP(w, r)
		})
	}))
	router := mux.NewRouter()
	s3API.RegisterHandlers(router)

	serve := func(req *http.Request) (*logger.ReqInfo, *httptest.ResponseRecorder) {
		reqInfo := &logger.ReqInfo{API: "unknown"}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req.WithContext(logger.SetReqInfo(req.Context(), reqInfo)))
		require.Equal(t, rec.Header().Get("X-Amz-Request-Id"), reqInfo.RequestID)
		return reqInfo, rec
	}
	tag := func(reqInfo *logger.ReqInfo, key string) any {
		for _, kv := range reqInfo.GetTags() {
			if kv.Key == key {
				return kv.Val
			}
		}
		return nil
	}

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/bucket?location", nil)
	signV4(req, nil, time.Now())
	reqInfo, rec := serve(req)
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	require.Same(t, reqInfo, layerReqInfo)
	require.Equal(t, "GetBucketLocation", reqInfo.API)
	require.Equal(t, "bucket", reqInfo.BucketName)
	require.Equal(t, "NoSuchBucket", tag(reqInfo, api.ErrorCodeTag))
	require.Equal(t, "from the layer", tag(reqInfo, "error"))

	// An unsigned request fails before the object layer is called.
	reqInfo, rec = serve(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/bucket/key", nil))
	require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
	require.Equal(t, "GetObject", reqInfo.API)
	require.Equal(t, "bucket", reqInfo.BucketName)
	require.Equal(t, "key", reqInfo.ObjectName)
	require.Equal(t, "AccessDenied", tag(reqInfo, api.ErrorCodeTag))

	// A request that middleware answers is filled in before it runs.
	req = httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/bucket/key", nil)
	req.Header.Set("Content-Length", "0")
	signV4(req, nil, time.Now())
	reqInfo, rec = serve(req)
	require.Equal(t, http.StatusTooManyRequests, rec.Code, rec.Body.String())
	require.Equal(t, "PutObject", reqInfo.API)
	require.Equal(t, "bucket", reqInfo.BucketName)
	require.Equal(t, "key", reqInfo.ObjectName)
	require.Equal(t, "SlowDown", tag(reqInfo, api.ErrorCodeTag))

	// An unrouted request has no operation.
	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/bucket/key?torrent", nil)
	signV4(req, nil, time.Now())
	reqInfo, rec = serve(req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code, rec.Body.String())
	require.Equal(t, "unknown", reqInfo.API)
	require.Equal(t, "MethodNotAllowed", tag(reqInfo, api.ErrorCodeTag))
}
