// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

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
