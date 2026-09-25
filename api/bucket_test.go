// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/stretchr/testify/require"

	"storj.io/gateway/api"
	"storj.io/minio/cmd"
)

// bucketObjectLayer extends fakeObjectLayer with the bucket-level methods used by these tests.
type bucketObjectLayer struct {
	*fakeObjectLayer

	makeBucketWithLocation func(ctx context.Context, bucket string, opts cmd.BucketOptions) error
	setBucketTagging       func(ctx context.Context, bucket string, t *tags.Tags) error
	listObjectsV2          func(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (cmd.ListObjectsV2Info, error)
}

func (f *bucketObjectLayer) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (cmd.ListObjectsV2Info, error) {
	return f.listObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
}

func (f *bucketObjectLayer) MakeBucketWithLocation(ctx context.Context, bucket string, opts cmd.BucketOptions) error {
	return f.makeBucketWithLocation(ctx, bucket, opts)
}

func TestCreateBucketInvalidObjectLockHeader(t *testing.T) {
	var called bool
	objectAPI := &bucketObjectLayer{
		fakeObjectLayer: &fakeObjectLayer{},
		makeBucketWithLocation: func(context.Context, string, cmd.BucketOptions) error {
			called = true
			return nil
		},
	}

	header := http.Header{"X-Amz-Bucket-Object-Lock-Enabled": {"bogus"}}
	resp := serve(t, objectAPI, http.MethodPut, "/bucket", header, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>InvalidRequest</Code>")
	require.False(t, called)
}

func (f *bucketObjectLayer) SetBucketTagging(ctx context.Context, bucket string, t *tags.Tags) error {
	return f.setBucketTagging(ctx, bucket, t)
}

func TestPutBucketTaggingUnknownContentLength(t *testing.T) {
	var got *tags.Tags
	objectAPI := &bucketObjectLayer{
		fakeObjectLayer: &fakeObjectLayer{},
		setBucketTagging: func(_ context.Context, _ string, t *tags.Tags) error {
			got = t
			return nil
		},
	}

	router := mux.NewRouter()
	api.New(objectAPI, testCredentialsProvider{}, api.Config{}).RegisterHandlers(router)

	body := []byte(`<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>`)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/bucket?tagging", bytes.NewReader(body))
	req.ContentLength = -1
	signV4(req, body, time.Now())

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NotNil(t, got)
	require.Equal(t, "k=v", got.String())
}

func TestListObjectsV2(t *testing.T) {
	var gotToken string
	objectAPI := &bucketObjectLayer{
		fakeObjectLayer: &fakeObjectLayer{},
		listObjectsV2: func(_ context.Context, _, _, continuationToken, _ string, _ int, _ bool, _ string) (cmd.ListObjectsV2Info, error) {
			gotToken = continuationToken
			return cmd.ListObjectsV2Info{}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodGet, "/bucket?list-type=2", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Empty(t, gotToken)

	resp = serve(t, objectAPI, http.MethodGet, "/bucket?list-type=2&continuation-token=dG9rZW4%3D", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, "token", gotToken)

	resp = serve(t, objectAPI, http.MethodGet, "/bucket?list-type=2&continuation-token=%21", nil, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
}
