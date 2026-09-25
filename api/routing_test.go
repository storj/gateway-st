// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"

	"storj.io/gateway/api"
	"storj.io/minio/cmd"
)

// routingObjectLayer extends fakeObjectLayer with bucket-level methods.
type routingObjectLayer struct {
	*fakeObjectLayer

	deleteBucket func(ctx context.Context, bucket string, forceDelete bool) error
}

func (b *routingObjectLayer) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	return b.deleteBucket(ctx, bucket, forceDelete)
}

func TestUnsupportedSubresourceDoesNotDeleteBucket(t *testing.T) {
	objectAPI := &routingObjectLayer{
		fakeObjectLayer: &fakeObjectLayer{},
		deleteBucket: func(context.Context, string, bool) error {
			t.Error("DeleteBucket must not be called")
			return nil
		},
	}

	for _, query := range []string{"cors", "metadataTable", "metadataConfiguration"} {
		resp := serve(t, objectAPI, http.MethodDelete, "/bucket?"+query, nil, nil)
		require.Equal(t, http.StatusNotImplemented, resp.StatusCode, query+": "+resp.Body)
	}
}

func TestUnsupportedSubresourceOnObject(t *testing.T) {
	var deleted string
	objectAPI := &fakeObjectLayer{
		deleteObject: func(_ context.Context, _, object string, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
			deleted = object
			return cmd.ObjectInfo{}, nil
		},
	}

	// Bucket subresource names on an object URL don't select the unsupported bucket operation.
	resp := serve(t, objectAPI, http.MethodDelete, "/bucket/key?cors", nil, nil)
	require.Equal(t, http.StatusNoContent, resp.StatusCode, resp.Body)
	require.Equal(t, "key", deleted)
}

func TestRequestPathIsNotCleaned(t *testing.T) {
	var gotKey string
	objectAPI := &fakeObjectLayer{
		deleteObject: func(_ context.Context, _, object string, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
			gotKey = object
			return cmd.ObjectInfo{}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodDelete, "/bucket/a//b/../c", nil, nil)
	require.Equal(t, http.StatusNoContent, resp.StatusCode, resp.Body)
	require.Equal(t, "a//b/../c", gotKey)
}

func TestObjectKeyIsNotCleaned(t *testing.T) {
	var gotKey string
	objectAPI := &fakeObjectLayer{
		deleteObject: func(_ context.Context, _, object string, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
			gotKey = object
			return cmd.ObjectInfo{}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodDelete, "/bucket//a//b/../c", nil, nil)
	require.Equal(t, http.StatusNoContent, resp.StatusCode, resp.Body)
	require.Equal(t, "/a//b/../c", gotKey)
}

func TestBucketRoutesDoNotMatchObjectPaths(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		deleteObjects: func(context.Context, string, []cmd.ObjectToDelete, cmd.ObjectOptions) ([]cmd.DeletedObject, []cmd.DeleteObjectsError, error) {
			t.Error("DeleteObjects must not be called")
			return nil, nil, nil
		},
	}

	body := []byte(`<Delete><Object><Key>key</Key></Object></Delete>`)
	resp := serve(t, objectAPI, http.MethodPost, "/bucket/key?delete", nil, body)
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode, resp.Body)

	resp = serve(t, objectAPI, http.MethodPost, "/bucket/key", http.Header{"Content-Type": {"multipart/form-data; boundary=x"}}, nil)
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode, resp.Body)
}

func TestVirtualHostedRequestsDoNotFallThroughToPathStyle(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		deleteObjects: func(context.Context, string, []cmd.ObjectToDelete, cmd.ObjectOptions) ([]cmd.DeletedObject, []cmd.DeleteObjectsError, error) {
			t.Error("DeleteObjects must not be called")
			return nil, nil, nil
		},
	}
	router := mux.NewRouter()
	api.New(objectAPI, testCredentialsProvider{}, api.Config{Domains: []string{"example.com"}}).RegisterHandlers(router)

	// The path would address another bucket if the request fell through to path-style routing.
	for _, host := range []string{"mybucket.example.com", "mybucket.EXAMPLE.com"} {
		body := []byte(`<Delete><Object><Key>victim</Key></Object></Delete>`)
		sum := md5.Sum(body)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+host+"/other-bucket?delete", bytes.NewReader(body))
		req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(sum[:]))
		signV4(req, body, time.Now())

		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		require.NotEqual(t, http.StatusOK, rec.Code, host)
	}
}

func TestResponseWriterWithoutFlusher(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		listBuckets: func(context.Context) ([]cmd.BucketInfo, error) {
			return []cmd.BucketInfo{{Name: "bucket"}}, nil
		},
	}
	router := mux.NewRouter()
	api.New(objectAPI, testCredentialsProvider{}, api.Config{}).RegisterHandlers(router)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	signV4(req, nil, time.Now())
	rec := httptest.NewRecorder()
	// Hide the recorder's Flush method, like a middleware wrapping the writer would.
	router.ServeHTTP(struct{ http.ResponseWriter }{rec}, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func TestRequestIDsAreRandom(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		listBuckets: func(context.Context) ([]cmd.BucketInfo, error) { return nil, nil },
	}

	first := serve(t, objectAPI, http.MethodGet, "/", nil, nil).Header
	second := serve(t, objectAPI, http.MethodGet, "/", nil, nil).Header
	require.Regexp(t, "^[0-9A-F]{16}$", first.Get("X-Amz-Request-Id"))
	require.NotEqual(t, first.Get("X-Amz-Request-Id"), second.Get("X-Amz-Request-Id"))
	require.NotEmpty(t, first.Get("X-Amz-Id-2"))
	require.NotEqual(t, first.Get("X-Amz-Id-2"), second.Get("X-Amz-Id-2"))
}

func TestHeadErrorHasNoBody(t *testing.T) {
	router := mux.NewRouter()
	api.New(&fakeObjectLayer{}, testCredentialsProvider{}, api.Config{}).RegisterHandlers(router)

	// The request is unsigned, so authentication fails.
	req := httptest.NewRequestWithContext(t.Context(), http.MethodHead, "/bucket", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Empty(t, rec.Body.String())
}

func TestInvalidUTF8Key(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		deleteObject: func(context.Context, string, string, cmd.ObjectOptions) (cmd.ObjectInfo, error) {
			t.Error("DeleteObject must not be called")
			return cmd.ObjectInfo{}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodDelete, "/bucket/%FF", nil, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>InvalidRequest</Code>")
}

func TestOverlappingDomains(t *testing.T) {
	var gotBucket string
	objectAPI := &routingObjectLayer{
		fakeObjectLayer: &fakeObjectLayer{
			listBuckets: func(context.Context) ([]cmd.BucketInfo, error) {
				return []cmd.BucketInfo{{Name: "listed"}}, nil
			},
		},
		deleteBucket: func(_ context.Context, bucket string, _ bool) error {
			gotBucket = bucket
			return nil
		},
	}
	router := mux.NewRouter()
	api.New(objectAPI, testCredentialsProvider{}, api.Config{
		Domains: []string{"example.com", "s3.example.com"},
	}).RegisterHandlers(router)

	do := func(method, target string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(t.Context(), method, target, nil)
		signV4(req, nil, time.Now())
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}

	rec := do(http.MethodDelete, "http://b.s3.example.com/")
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
	require.Equal(t, "b", gotBucket)

	rec = do(http.MethodGet, "http://s3.example.com/")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "<Name>listed</Name>")

	// Host names are case-insensitive.
	gotBucket = ""
	rec = do(http.MethodDelete, "http://b.S3.Example.com/")
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
	require.Equal(t, "b", gotBucket)

	gotBucket = ""
	rec = do(http.MethodDelete, "http://S3.example.com/")
	require.NotEqual(t, http.StatusNoContent, rec.Code, rec.Body.String())
	require.Empty(t, gotBucket)
	// The port is ignored.
	gotBucket = ""
	rec = do(http.MethodDelete, "http://b.s3.example.com:7777/")
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
	require.Equal(t, "b", gotBucket)

	gotBucket = ""
	rec = do(http.MethodDelete, "http://b.S3.Example.com:7777/")
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
	require.Equal(t, "b", gotBucket)

	rec = do(http.MethodGet, "http://s3.example.com:7777/")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "<Name>listed</Name>")
}

func TestUnmatchedRoutes(t *testing.T) {
	for _, tt := range []struct {
		method, target string
		status         int
		code           string
	}{
		{http.MethodPost, "/bucket/key?restore", http.StatusNotImplemented, "NotImplemented"},
		{http.MethodPost, "/bucket/key?select&select-type=2", http.StatusNotImplemented, "NotImplemented"},
		{http.MethodPost, "/bucket/key", http.StatusMethodNotAllowed, "MethodNotAllowed"},
		{http.MethodDelete, "/", http.StatusMethodNotAllowed, "MethodNotAllowed"},
	} {
		t.Run(tt.method+" "+tt.target, func(t *testing.T) {
			resp := serve(t, &fakeObjectLayer{}, tt.method, tt.target, nil, nil)
			require.Equal(t, tt.status, resp.StatusCode, resp.Body)
			require.Contains(t, resp.Body, "<Code>"+tt.code+"</Code>")
			require.NotEmpty(t, resp.Header.Get("X-Amz-Request-Id"))
		})
	}
}
