// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/minio/cmd"
)

func TestPreconditionPrecedence(t *testing.T) {
	modTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	objInfo := cmd.ObjectInfo{Bucket: "bucket", Name: "src", ModTime: modTime, Size: 10, ETag: "etag"}
	objectAPI := singleObjectLayer(objInfo, "0123456789")

	before := modTime.Add(-time.Hour).Format(http.TimeFormat)
	after := modTime.Add(time.Hour).Format(http.TimeFormat)

	for _, tt := range []struct {
		name                         string
		ifMatch, ifUnmodifiedSince   string
		ifNoneMatch, ifModifiedSince string
		expGetStatus                 int
	}{
		{name: "if-match true overrides if-unmodified-since false", ifMatch: `"etag"`, ifUnmodifiedSince: before, expGetStatus: http.StatusOK},
		{name: "if-match true overrides if-modified-since false", ifMatch: `"etag"`, ifModifiedSince: after, expGetStatus: http.StatusOK},
		{name: "if-match false wins over if-modified-since false", ifMatch: `"other"`, ifModifiedSince: after, expGetStatus: http.StatusPreconditionFailed},
		{name: "if-none-match true overrides if-modified-since false", ifNoneMatch: `"other"`, ifModifiedSince: after, expGetStatus: http.StatusOK},
		{name: "if-none-match false with if-modified-since true", ifNoneMatch: `"etag"`, ifModifiedSince: before, expGetStatus: http.StatusNotModified},
		{name: "if-match wildcard", ifMatch: "*", expGetStatus: http.StatusOK},
		{name: "if-none-match wildcard", ifNoneMatch: "*", expGetStatus: http.StatusNotModified},
		{name: "if-match list", ifMatch: `"a", "etag"`, expGetStatus: http.StatusOK},
		{name: "if-match weak etag", ifMatch: `W/"etag"`, expGetStatus: http.StatusPreconditionFailed},
		{name: "if-none-match weak etag in list", ifNoneMatch: `"a", W/"etag"`, expGetStatus: http.StatusNotModified},
	} {
		t.Run(tt.name, func(t *testing.T) {
			header := http.Header{}
			for name, value := range map[string]string{
				"If-Match":            tt.ifMatch,
				"If-Unmodified-Since": tt.ifUnmodifiedSince,
				"If-None-Match":       tt.ifNoneMatch,
				"If-Modified-Since":   tt.ifModifiedSince,
			} {
				if value != "" {
					header.Set(name, value)
				}
			}

			resp := serve(t, objectAPI, http.MethodHead, "/bucket/src", header, nil)
			require.Equal(t, tt.expGetStatus, resp.StatusCode, "HEAD")
		})
	}
}

func TestGetEmptyObjectSuffixRange(t *testing.T) {
	objInfo := cmd.ObjectInfo{
		Bucket: "bucket", Name: "key", ModTime: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), ETag: "etag",
		ContentEncoding: "gzip",
		UserDefined:     map[string]string{"X-Amz-Meta-Foo": "bar"},
	}
	objectAPI := singleObjectLayer(objInfo, "")
	// Like the real object layer, don't check the range against the object size.
	objectAPI.getObjectNInfo = func(_ context.Context, _, _ string, _ *cmd.HTTPRangeSpec, _ http.Header, _ cmd.LockType, opts cmd.ObjectOptions) (*cmd.GetObjectReader, error) {
		return cmd.NewGetObjectReaderFromReader(strings.NewReader(""), objInfo, opts)
	}

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		resp := serve(t, objectAPI, method, "/bucket/key", http.Header{"Range": {"bytes=-5"}}, nil)
		require.Equal(t, http.StatusRequestedRangeNotSatisfiable, resp.StatusCode, method)
		// Errors are sent without the object's headers.
		for _, k := range []string{"Content-Range", "Content-Encoding", "ETag", "X-Amz-Meta-Foo"} {
			require.Empty(t, resp.Header.Get(k), method+" "+k)
		}
		if method == http.MethodGet {
			require.Contains(t, resp.Body, "<Code>InvalidRange</Code>")
		}
	}
}

func TestGetInvalidPartNumber(t *testing.T) {
	// Without a modification time the conditional headers are ignored, but the part number isn't.
	objectAPI := singleObjectLayer(cmd.ObjectInfo{Bucket: "bucket", Name: "key", Size: 4}, "data")

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		resp := serve(t, objectAPI, method, "/bucket/key?partNumber=2", nil, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, method)
	}

	// GetObject rejects the part before the object layer starts a download.
	objectAPI.getObjectNInfo = func(context.Context, string, string, *cmd.HTTPRangeSpec, http.Header, cmd.LockType, cmd.ObjectOptions) (*cmd.GetObjectReader, error) {
		t.Fatal("the object layer was called")
		return nil, nil
	}
	resp := serve(t, objectAPI, http.MethodGet, "/bucket/key?partNumber=2", nil, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestGetObjectRouting(t *testing.T) {
	objectAPI := singleObjectLayer(cmd.ObjectInfo{Bucket: "bucket", Name: "key", Size: 4}, "data")

	// ListParts isn't served as GetObject.
	resp := serve(t, objectAPI, http.MethodGet, "/bucket/key?uploadId=abc", nil, nil)
	require.Equal(t, http.StatusNotImplemented, resp.StatusCode, resp.Body)
}

func TestGetObjectFirstReadClientAbort(t *testing.T) {
	objInfo := cmd.ObjectInfo{Bucket: "bucket", Name: "key", Size: 10, ETag: "etag"}
	objectAPI := &fakeObjectLayer{
		getObjectNInfo: func(_ context.Context, _, _ string, _ *cmd.HTTPRangeSpec, _ http.Header, _ cmd.LockType, opts cmd.ObjectOptions) (*cmd.GetObjectReader, error) {
			return cmd.NewGetObjectReaderFromReader(iotest.ErrReader(context.Canceled), objInfo, opts)
		},
	}

	// The client went away, so nothing is written, which a recorder reports as 200.
	resp := serve(t, objectAPI, http.MethodGet, "/bucket/key", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Empty(t, resp.Body)
}

func TestPreconditionsWithoutModTime(t *testing.T) {
	// Dates are ignored without a modification time, but ETags aren't.
	objectAPI := singleObjectLayer(cmd.ObjectInfo{Bucket: "bucket", Name: "key", Size: 4, ETag: "etag"}, "data")
	future := time.Now().Add(time.Hour).Format(http.TimeFormat)

	for _, tt := range []struct {
		header    http.Header
		expStatus int
	}{
		{http.Header{"If-Match": {`"other"`}}, http.StatusPreconditionFailed},
		{http.Header{"If-None-Match": {`"etag"`}}, http.StatusNotModified},
		{http.Header{"If-Modified-Since": {future}}, http.StatusOK},
		{http.Header{"If-Unmodified-Since": {"Mon, 02 Jan 2006 15:04:05 GMT"}}, http.StatusOK},
	} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			resp := serve(t, objectAPI, method, "/bucket/key", tt.header, nil)
			require.Equal(t, tt.expStatus, resp.StatusCode, "%s %v", method, tt.header)
			require.Empty(t, resp.Header.Get("Last-Modified"), "%s %v", method, tt.header)
		}
	}
}
