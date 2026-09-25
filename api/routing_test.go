// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"runtime/debug"
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

// TestSubresourceRouting checks which handler serves each method, path and subresource. Every
// object layer call panics, so a request that reaches storage is identified by the handler on the
// panicking stack, and a rejected request is known to have touched no storage.
func TestMalformedQueryIsRejected(t *testing.T) {
	objectAPI := &routingObjectLayer{
		fakeObjectLayer: &fakeObjectLayer{
			deleteObject: func(context.Context, string, string, cmd.ObjectOptions) (cmd.ObjectInfo, error) {
				t.Error("DeleteObject must not be called")
				return cmd.ObjectInfo{}, nil
			},
		},
		deleteBucket: func(context.Context, string, bool) error {
			t.Error("DeleteBucket must not be called")
			return nil
		},
	}

	// The router treats ';' as a separator, so these name a subresource that
	// r.URL.Query() doesn't see.
	for _, target := range []string{"/bucket/key?x=1;retention", "/bucket/key?retention;x=1", "/bucket?x=1;versioning", "/bucket/key?versionId=%zz"} {
		resp := serve(t, objectAPI, http.MethodDelete, target, nil, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, target)
		require.Contains(t, resp.Body, "<Code>InvalidURI</Code>", target)
	}
}

func TestSubresourceRouting(t *testing.T) {
	handlerName := regexp.MustCompile(`\(\*API\)\.(\w+Handler)\(`)
	errorCode := regexp.MustCompile(`<Code>(\w+)</Code>`)
	router := newTestRouter(&fakeObjectLayer{})

	serveRecover := func(req *http.Request) (got string) {
		defer func() {
			if recover() != nil {
				got = handlerName.FindStringSubmatch(string(debug.Stack()))[1]
			}
		}()
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		got = fmt.Sprint(rec.Code)
		if m := errorCode.FindStringSubmatch(rec.Body.String()); m != nil {
			got += " " + m[1]
		}
		return got
	}

	const (
		notAllowed  = "405 MethodNotAllowed"
		keyRequired = "400 InvalidRequest"
	)
	for _, tt := range []struct {
		method, target string
		copySource     bool
		want           string // handler name, or status and error code
	}{
		// Object paths: supported.
		{method: http.MethodPut, target: "/bucket/key", want: "PutObjectHandler"},
		{method: http.MethodPut, target: "/bucket/key?x-id=PutObject", want: "PutObjectHandler"},
		{method: http.MethodPut, target: "/bucket/key", copySource: true, want: "CopyObjectHandler"},
		{method: http.MethodPut, target: "/bucket/key?partNumber=1&uploadId=x", want: "UploadPartHandler"},
		{method: http.MethodPut, target: "/bucket/key?partNumber=1&uploadId=x", copySource: true, want: "UploadPartCopyHandler"},
		{method: http.MethodGet, target: "/bucket/key", want: "GetObjectHandler"},
		{method: http.MethodGet, target: "/bucket/key?partNumber=1&versionId=00000000000000000000000000000001&response-content-type=a%2Fb", want: "GetObjectHandler"},
		{method: http.MethodGet, target: "/bucket/key?X-Amz-Expires=60&X-Amz-SignedHeaders=host&x-id=GetObject", want: "GetObjectHandler"},
		{method: http.MethodGet, target: "/bucket/key?tagging", want: "GetObjectTaggingHandler"},
		{method: http.MethodGet, target: "/bucket/key?uploadId=x", want: "ListPartsHandler"},
		{method: http.MethodHead, target: "/bucket/key", want: "HeadObjectHandler"},
		{method: http.MethodHead, target: "/bucket/key?partNumber=1", want: "HeadObjectHandler"},
		{method: http.MethodDelete, target: "/bucket/key", want: "DeleteObjectHandler"},
		{method: http.MethodDelete, target: "/bucket/key?versionId=00000000000000000000000000000001", want: "DeleteObjectHandler"},
		{method: http.MethodDelete, target: "/bucket/key?tagging", want: "DeleteObjectTaggingHandler"},
		{method: http.MethodDelete, target: "/bucket/key?uploadId=x", want: "AbortMultipartUploadHandler"},
		{method: http.MethodPost, target: "/bucket/key?uploads", want: "CreateMultipartUploadHandler"},

		// Object paths: unrouted subresources must not reach PutObject, CopyObject, GetObject or DeleteObject.
		{method: http.MethodPut, target: "/bucket/key?attributes", want: notAllowed},
		{method: http.MethodPut, target: "/bucket/key?uploads", want: notAllowed},
		{method: http.MethodPut, target: "/bucket/key?uploadId=x", want: notAllowed},
		{method: http.MethodPut, target: "/bucket/key?partNumber=1", want: notAllowed},
		{method: http.MethodPut, target: "/bucket/key?annotation", want: notAllowed},
		{method: http.MethodPut, target: "/bucket/key?renameObject", want: notAllowed},
		{method: http.MethodPut, target: "/bucket/key?attributes", copySource: true, want: notAllowed},
		{method: http.MethodGet, target: "/bucket/key?uploads", want: notAllowed},
		{method: http.MethodGet, target: "/bucket/key?versions", want: notAllowed},
		{method: http.MethodGet, target: "/bucket/key?torrent", want: notAllowed},
		{method: http.MethodGet, target: "/bucket/key?annotation", want: notAllowed},
		{method: http.MethodDelete, target: "/bucket/key?retention", want: notAllowed},
		{method: http.MethodDelete, target: "/bucket/key?legal-hold", want: notAllowed},
		{method: http.MethodDelete, target: "/bucket/key?acl", want: notAllowed},
		{method: http.MethodDelete, target: "/bucket/key?attributes", want: notAllowed},
		{method: http.MethodDelete, target: "/bucket/key?uploads", want: notAllowed},
		{method: http.MethodDelete, target: "/bucket/key?annotation", want: notAllowed},

		// Bucket paths: supported.
		{method: http.MethodPut, target: "/bucket", want: "CreateBucketHandler"},
		{method: http.MethodHead, target: "/bucket", want: "HeadBucketHandler"},
		{method: http.MethodGet, target: "/bucket", want: "ListObjectsHandler"},
		{method: http.MethodGet, target: "/bucket?list-type=2", want: "ListObjectsV2Handler"},
		{method: http.MethodGet, target: "/bucket?versions", want: "ListObjectVersionsHandler"},
		{method: http.MethodGet, target: "/bucket?uploads", want: "ListMultipartUploadsHandler"},
		{method: http.MethodGet, target: "/bucket?location", want: "GetBucketLocationHandler"},
		{method: http.MethodDelete, target: "/bucket", want: "DeleteBucketHandler"},

		// Bucket paths: unrouted subresources must not reach CreateBucket, ListObjects or DeleteBucket.
		{method: http.MethodPut, target: "/bucket?location", want: notAllowed},
		{method: http.MethodPut, target: "/bucket?policyStatus", want: notAllowed},
		{method: http.MethodPut, target: "/bucket?uploads", want: notAllowed},
		{method: http.MethodGet, target: "/bucket?session", want: notAllowed},
		{method: http.MethodGet, target: "/bucket?abac", want: notAllowed},
		{method: http.MethodGet, target: "/bucket?metadataConfiguration", want: notAllowed},
		{method: http.MethodGet, target: "/bucket?list-type=1", want: "400 InvalidArgument"},
		{method: http.MethodGet, target: "/bucket?list-type=3", want: "400 InvalidArgument"},
		{method: http.MethodGet, target: "/bucket?list-type=20", want: "400 InvalidArgument"},
		{method: http.MethodDelete, target: "/bucket?versioning", want: notAllowed},
		{method: http.MethodDelete, target: "/bucket?notification", want: notAllowed},
		{method: http.MethodDelete, target: "/bucket?object-lock", want: notAllowed},
		{method: http.MethodDelete, target: "/bucket?acl", want: notAllowed},
		{method: http.MethodDelete, target: "/bucket?location", want: notAllowed},
		{method: http.MethodDelete, target: "/bucket?uploads", want: notAllowed},
		{method: http.MethodDelete, target: "/bucket?metadataTable", want: "501 NotImplemented"},
		{method: http.MethodDelete, target: "/bucket?metadataConfiguration", want: "501 NotImplemented"},
		{method: http.MethodPost, target: "/bucket?uploads", want: "400 InvalidURI"},
		{method: http.MethodPost, target: "/bucket?uploadId=x", want: "400 InvalidURI"},

		// Bucket paths: multipart parameters need a key.
		{method: http.MethodPut, target: "/bucket?partNumber=1&uploadId=x", want: keyRequired},
		{method: http.MethodGet, target: "/bucket?uploadId=x", want: keyRequired},
		{method: http.MethodDelete, target: "/bucket?uploadId=x", want: keyRequired},
		{method: http.MethodDelete, target: "/bucket?partNumber=1", want: keyRequired},
		{method: http.MethodHead, target: "/bucket?uploadId=x", want: "400"},
	} {
		name := tt.method + " " + tt.target
		if tt.copySource {
			name += " copy"
		}
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), tt.method, tt.target, nil)
			if tt.method == http.MethodPut {
				req.Header.Set("Content-Length", "0")
			}
			if tt.copySource {
				req.Header.Set("X-Amz-Copy-Source", "src/key")
			}
			signV4(req, nil, time.Now())
			require.Equal(t, tt.want, serveRecover(req))
		})
	}
}
