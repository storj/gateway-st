// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"

	"storj.io/gateway/api"
	"storj.io/minio/cmd"
)

func TestPreconditionPrecedence(t *testing.T) {
	modTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	objInfo := cmd.ObjectInfo{Bucket: "bucket", Name: "src", ModTime: modTime, Size: 10, ETag: "etag"}
	objectAPI := singleObjectLayer(objInfo, "0123456789")
	objectAPI.copyObject = func(_ context.Context, _, _, dstBucket, dstObject string, _ cmd.ObjectInfo, _, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
		return cmd.ObjectInfo{Bucket: dstBucket, Name: dstObject, ETag: "newetag", ModTime: modTime}, nil
	}

	before := modTime.Add(-time.Hour).Format(http.TimeFormat)
	after := modTime.Add(time.Hour).Format(http.TimeFormat)

	for _, tt := range []struct {
		name                         string
		ifMatch, ifUnmodifiedSince   string
		ifNoneMatch, ifModifiedSince string
		expGetStatus, expCopyStatus  int
	}{
		{name: "if-match true overrides if-unmodified-since false", ifMatch: `"etag"`, ifUnmodifiedSince: before, expGetStatus: http.StatusOK, expCopyStatus: http.StatusOK},
		{name: "if-match true overrides if-modified-since false", ifMatch: `"etag"`, ifModifiedSince: after, expGetStatus: http.StatusOK, expCopyStatus: http.StatusOK},
		{name: "if-match false wins over if-modified-since false", ifMatch: `"other"`, ifModifiedSince: after, expGetStatus: http.StatusPreconditionFailed, expCopyStatus: http.StatusPreconditionFailed},
		{name: "if-none-match true overrides if-modified-since false", ifNoneMatch: `"other"`, ifModifiedSince: after, expGetStatus: http.StatusOK, expCopyStatus: http.StatusOK},
		{name: "if-none-match false with if-modified-since true", ifNoneMatch: `"etag"`, ifModifiedSince: before, expGetStatus: http.StatusNotModified, expCopyStatus: http.StatusPreconditionFailed},
		{name: "if-match wildcard", ifMatch: "*", expGetStatus: http.StatusOK, expCopyStatus: http.StatusOK},
		{name: "if-none-match wildcard", ifNoneMatch: "*", expGetStatus: http.StatusNotModified, expCopyStatus: http.StatusPreconditionFailed},
		{name: "if-match list", ifMatch: `"a", "etag"`, expGetStatus: http.StatusOK, expCopyStatus: http.StatusOK},
		{name: "if-match weak etag", ifMatch: `W/"etag"`, expGetStatus: http.StatusPreconditionFailed, expCopyStatus: http.StatusPreconditionFailed},
		{name: "if-none-match weak etag in list", ifNoneMatch: `"a", W/"etag"`, expGetStatus: http.StatusNotModified, expCopyStatus: http.StatusPreconditionFailed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			header, copyHeader := http.Header{}, http.Header{"X-Amz-Copy-Source": {"/bucket/src"}}
			for name, value := range map[string]string{
				"If-Match":            tt.ifMatch,
				"If-Unmodified-Since": tt.ifUnmodifiedSince,
				"If-None-Match":       tt.ifNoneMatch,
				"If-Modified-Since":   tt.ifModifiedSince,
			} {
				if value != "" {
					header.Set(name, value)
					copyHeader.Set("X-Amz-Copy-Source-"+name, value)
				}
			}

			resp := serve(t, objectAPI, http.MethodHead, "/bucket/src", header, nil)
			require.Equal(t, tt.expGetStatus, resp.StatusCode, "HEAD")

			resp = serve(t, objectAPI, http.MethodPut, "/bucket/dst", copyHeader, nil)
			require.Equal(t, tt.expCopyStatus, resp.StatusCode, "CopyObject: %s", resp.Body)
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
	objectAPI.copyObject = func(_ context.Context, _, _, dstBucket, dstObject string, _ cmd.ObjectInfo, _, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
		return cmd.ObjectInfo{Bucket: dstBucket, Name: dstObject, ETag: "newetag"}, nil
	}
	future := time.Now().Add(time.Hour).Format(http.TimeFormat)

	for _, tt := range []struct {
		header                   http.Header
		expStatus, expCopyStatus int
	}{
		{http.Header{"If-Match": {`"other"`}}, http.StatusPreconditionFailed, http.StatusPreconditionFailed},
		{http.Header{"If-None-Match": {`"etag"`}}, http.StatusNotModified, http.StatusPreconditionFailed},
		{http.Header{"If-Modified-Since": {future}}, http.StatusOK, http.StatusOK},
		{http.Header{"If-Unmodified-Since": {"Mon, 02 Jan 2006 15:04:05 GMT"}}, http.StatusOK, http.StatusOK},
	} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			resp := serve(t, objectAPI, method, "/bucket/key", tt.header, nil)
			require.Equal(t, tt.expStatus, resp.StatusCode, "%s %v", method, tt.header)
			require.Empty(t, resp.Header.Get("Last-Modified"), "%s %v", method, tt.header)
		}

		copyHeader := http.Header{"X-Amz-Copy-Source": {"/bucket/key"}}
		for k, v := range tt.header {
			copyHeader["X-Amz-Copy-Source-"+k] = v
		}
		resp := serve(t, objectAPI, http.MethodPut, "/bucket/dst", copyHeader, nil)
		require.Equal(t, tt.expCopyStatus, resp.StatusCode, "CopyObject %v: %s", tt.header, resp.Body)
		require.Empty(t, resp.Header.Get("Last-Modified"), "CopyObject %v", tt.header)
	}
}

func TestCompleteMultipartUploadDuplicatePart(t *testing.T) {
	resp := serve(t, &fakeObjectLayer{}, http.MethodPost, "/bucket/key?uploadId=upload-id", nil, []byte(`<CompleteMultipartUpload>`+
		`<Part><PartNumber>1</PartNumber><ETag>a</ETag></Part>`+
		`<Part><PartNumber>1</PartNumber><ETag>b</ETag></Part>`+
		`</CompleteMultipartUpload>`))
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>InvalidPartOrder</Code>")

	// Comparing part numbers must not overflow.
	resp = serve(t, &fakeObjectLayer{}, http.MethodPost, "/bucket/key?uploadId=upload-id", nil, []byte(`<CompleteMultipartUpload>`+
		`<Part><PartNumber>9223372036854775807</PartNumber><ETag>a</ETag></Part>`+
		`<Part><PartNumber>-1</PartNumber><ETag>b</ETag></Part>`+
		`</CompleteMultipartUpload>`))
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>InvalidPartOrder</Code>")
}

func TestCopyObjectStorageClass(t *testing.T) {
	objectAPI := singleObjectLayer(cmd.ObjectInfo{Bucket: "bucket", Name: "key", Size: 4, ETag: "etag"}, "data")
	var gotMetadata map[string]string
	objectAPI.copyObject = func(_ context.Context, _, _, dstBucket, dstObject string, srcInfo cmd.ObjectInfo, _, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
		gotMetadata = srcInfo.UserDefined
		return cmd.ObjectInfo{Bucket: dstBucket, Name: dstObject, ETag: "newetag"}, nil
	}

	for _, tt := range []struct {
		target    string
		header    http.Header
		expStatus int
		expClass  string
	}{
		{target: "/bucket/dst?X-Amz-Storage-Class=GLACIER", expStatus: http.StatusBadRequest},
		{target: "/bucket/dst?x-amz-storage-class=GLACIER", expStatus: http.StatusBadRequest},
		{target: "/bucket/dst", header: http.Header{"X-Amz-Storage-Class": {"GLACIER"}}, expStatus: http.StatusBadRequest},
		{target: "/bucket/dst?X-Amz-Storage-Class=STANDARD", expStatus: http.StatusOK, expClass: "STANDARD"},
	} {
		for _, directive := range []string{"COPY", "REPLACE"} {
			gotMetadata = nil
			header := http.Header{"X-Amz-Copy-Source": {"/bucket/key"}, "X-Amz-Metadata-Directive": {directive}}
			for k, v := range tt.header {
				header[k] = v
			}
			resp := serve(t, objectAPI, http.MethodPut, tt.target, header, nil)
			require.Equal(t, tt.expStatus, resp.StatusCode, "%s %s: %s", directive, tt.target, resp.Body)
			if tt.expStatus == http.StatusOK {
				require.Equal(t, tt.expClass, gotMetadata["X-Amz-Storage-Class"], "%s %s", directive, tt.target)
			}
		}
	}
}

type taggingObjectLayer struct {
	*fakeObjectLayer
	gotTags string
}

func (l *taggingObjectLayer) PutObjectTags(_ context.Context, bucket, object, tags string, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
	l.gotTags = tags
	return cmd.ObjectInfo{Bucket: bucket, Name: object}, nil
}

func TestPutObjectTaggingUnknownContentLength(t *testing.T) {
	objectAPI := &taggingObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
	router := mux.NewRouter()
	api.New(objectAPI, testCredentialsProvider{}, api.Config{}).RegisterHandlers(router)

	body := []byte(`<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag></TagSet></Tagging>`)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/bucket/key?tagging", bytes.NewReader(body))
	req.ContentLength = -1
	signV4(req, body, time.Now())

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "a=1", objectAPI.gotTags)
}

type putObjectLayer struct {
	*fakeObjectLayer
}

func (putObjectLayer) PutObject(_ context.Context, bucket, object string, _ *cmd.PutObjReader, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
	return cmd.ObjectInfo{Bucket: bucket, Name: object, ETag: "etag", VersionID: "version"}, nil
}

func TestPutObjectVersionID(t *testing.T) {
	resp := serve(t, putObjectLayer{&fakeObjectLayer{}}, http.MethodPut, "/bucket/key", http.Header{"Content-Length": {"3"}}, []byte("abc"))
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, `"etag"`, resp.Header.Get("ETag"))
	require.Equal(t, "version", resp.Header.Get("X-Amz-Version-Id"))
}
