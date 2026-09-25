// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/minio/minio-go/v7/pkg/signer"
	"github.com/stretchr/testify/require"

	"storj.io/gateway/api"
	"storj.io/gateway/api/apierr"
	"storj.io/minio/cmd"
)

const (
	testAccessKeyID     = "access"
	testSecretAccessKey = "secret"
)

type testCredentialsProvider struct{}

func (testCredentialsProvider) Provide(_ context.Context, accessKeyID string) (string, api.AuthData, error) {
	return testSecretAccessKey, api.AuthData{AccessKeyID: accessKeyID}, nil
}

// fakeObjectLayer is a cmd.ObjectLayer whose methods panic unless overridden
// by a test.
type fakeObjectLayer struct {
	cmd.ObjectLayer

	listBuckets          func(ctx context.Context) ([]cmd.BucketInfo, error)
	listMultipartUploads func(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (cmd.ListMultipartsInfo, error)
}

func (f *fakeObjectLayer) ListBuckets(ctx context.Context) ([]cmd.BucketInfo, error) {
	return f.listBuckets(ctx)
}

func (f *fakeObjectLayer) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (cmd.ListMultipartsInfo, error) {
	return f.listMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}

type response struct {
	StatusCode int
	Header     http.Header
	Body       string
}

// serve sends a signed request to an API backed by objectAPI and returns the response.
func serve(t *testing.T, objectAPI cmd.ObjectLayer, method, target string, header http.Header, body []byte) response {
	t.Helper()

	router := mux.NewRouter()
	api.New(objectAPI, testCredentialsProvider{}, api.Config{}).RegisterHandlers(router)

	req := httptest.NewRequestWithContext(t.Context(), method, target, bytes.NewReader(body))
	for k, v := range header {
		req.Header[k] = v
	}
	sum := sha256.Sum256(body)
	req.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(sum[:]))
	req = signer.SignV4(*req, testAccessKeyID, testSecretAccessKey, "", "us-east-1")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	// Canonicalize header keys like an HTTP client would. Handlers may set non-canonical keys.
	canonical := make(http.Header, len(rec.Header()))
	for k, v := range rec.Header() {
		canonical[http.CanonicalHeaderKey(k)] = append(canonical[http.CanonicalHeaderKey(k)], v...)
	}

	return response{
		StatusCode: rec.Code,
		Header:     canonical,
		Body:       rec.Body.String(),
	}
}

func TestRequestID(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		listBuckets: func(context.Context) ([]cmd.BucketInfo, error) {
			return []cmd.BucketInfo{{Name: "bucket"}}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodGet, "/", nil, nil)
	body := resp.Body
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	require.Contains(t, body, "<Name>bucket</Name>")
	require.NotEmpty(t, resp.Header.Get("X-Amz-Request-Id"))
}

func TestListMultipartUploadsRouting(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		listMultipartUploads: func(_ context.Context, bucket, _, _, _, _ string, _ int) (cmd.ListMultipartsInfo, error) {
			return cmd.ListMultipartsInfo{
				Uploads: []cmd.MultipartInfo{{Bucket: bucket, Object: "key", UploadID: "upload-id"}},
			}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodGet, "/bucket?uploads", nil, nil)
	body := resp.Body
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	require.Contains(t, body, "<ListMultipartUploadsResult")
	require.Contains(t, body, "<UploadId>upload-id</UploadId>")
}

func TestErrorResponse(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		listMultipartUploads: func(context.Context, string, string, string, string, string, int) (cmd.ListMultipartsInfo, error) {
			return cmd.ListMultipartsInfo{}, apierr.CodeAccessDenied
		},
	}

	resp := serve(t, objectAPI, http.MethodGet, "/bucket?uploads", nil, nil)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	var errResp struct {
		XMLName    xml.Name `xml:"Error"`
		Code       string
		Message    string
		BucketName string
		Resource   string
		RequestID  string `xml:"RequestId"`
	}
	require.NoError(t, xml.Unmarshal([]byte(resp.Body), &errResp))
	require.Equal(t, "AccessDenied", errResp.Code)
	require.Equal(t, "Access Denied", errResp.Message)
	require.Equal(t, "bucket", errResp.BucketName)
	require.Equal(t, "/bucket", errResp.Resource)
	require.Equal(t, resp.Header.Get("X-Amz-Request-Id"), errResp.RequestID)
	require.NotEmpty(t, errResp.RequestID)

	// Virtual-hosted-style requests take the bucket from the host.
	router := mux.NewRouter()
	api.New(objectAPI, testCredentialsProvider{}, api.Config{Domains: []string{"example.com"}}).RegisterHandlers(router)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://bucket.example.com/key?tagging", nil))
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	require.Equal(t, "/bucket/key", errResp.Resource)
}
