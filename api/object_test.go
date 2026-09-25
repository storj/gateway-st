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
		require.Equal(t, http.StatusRequestedRangeNotSatisfiable, resp.StatusCode, method)
	}

	// GetObject rejects the part before the object layer starts a download.
	objectAPI.getObjectNInfo = func(context.Context, string, string, *cmd.HTTPRangeSpec, http.Header, cmd.LockType, cmd.ObjectOptions) (*cmd.GetObjectReader, error) {
		t.Fatal("the object layer was called")
		return nil, nil
	}
	resp := serve(t, objectAPI, http.MethodGet, "/bucket/key?partNumber=2", nil, nil)
	require.Equal(t, http.StatusRequestedRangeNotSatisfiable, resp.StatusCode)
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

type copyPartObjectLayer struct {
	*fakeObjectLayer
	gotSrcInfo cmd.ObjectInfo
}

func (l *copyPartObjectLayer) CopyObjectPart(_ context.Context, _, _, _, _, _ string, partID int, _, _ int64, srcInfo cmd.ObjectInfo, _, _ cmd.ObjectOptions) (cmd.PartInfo, error) {
	l.gotSrcInfo = srcInfo
	return cmd.PartInfo{PartNumber: partID, ETag: "partetag"}, nil
}

func TestUploadPartCopySource(t *testing.T) {
	modTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	small := cmd.ObjectInfo{Bucket: "bucket", Name: "small", ModTime: modTime, Size: 10, ETag: "etag"}
	large := cmd.ObjectInfo{Bucket: "bucket", Name: "large", ModTime: modTime, Size: 5<<30 + 1, ETag: "etag"}
	objectAPI := &copyPartObjectLayer{fakeObjectLayer: &fakeObjectLayer{
		getObjectInfo: func(_ context.Context, _, object string, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
			if object == large.Name {
				return large, nil
			}
			return small, nil
		},
	}}

	for _, tt := range []struct {
		name      string
		header    http.Header
		expStatus int
		expCode   string
	}{
		{name: "if-match match", header: http.Header{"X-Amz-Copy-Source": {"/bucket/small"}, "X-Amz-Copy-Source-If-Match": {`"etag"`}}, expStatus: http.StatusOK},
		{name: "if-match mismatch", header: http.Header{"X-Amz-Copy-Source": {"/bucket/small"}, "X-Amz-Copy-Source-If-Match": {`"other"`}}, expStatus: http.StatusPreconditionFailed, expCode: "PreconditionFailed"},
		{name: "range within source", header: http.Header{"X-Amz-Copy-Source": {"/bucket/small"}, "X-Amz-Copy-Source-Range": {"bytes=0-9"}}, expStatus: http.StatusOK},
		{name: "range past end of source", header: http.Header{"X-Amz-Copy-Source": {"/bucket/small"}, "X-Amz-Copy-Source-Range": {"bytes=5-10"}}, expStatus: http.StatusRequestedRangeNotSatisfiable, expCode: "InvalidRange"},
		{name: "whole source too large", header: http.Header{"X-Amz-Copy-Source": {"/bucket/large"}}, expStatus: http.StatusBadRequest, expCode: "EntityTooLarge"},
		{name: "range of large source", header: http.Header{"X-Amz-Copy-Source": {"/bucket/large"}, "X-Amz-Copy-Source-Range": {"bytes=0-9"}}, expStatus: http.StatusOK},
		{name: "range length overflows", header: http.Header{"X-Amz-Copy-Source": {"/bucket/large"}, "X-Amz-Copy-Source-Range": {"bytes=0-9223372036854775807"}}, expStatus: http.StatusRequestedRangeNotSatisfiable, expCode: "InvalidRange"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			objectAPI.gotSrcInfo = cmd.ObjectInfo{}
			resp := serve(t, objectAPI, http.MethodPut, "/bucket/dst?partNumber=1&uploadId=upload", tt.header, nil)
			require.Equal(t, tt.expStatus, resp.StatusCode, resp.Body)
			if tt.expCode != "" {
				require.Contains(t, resp.Body, "<Code>"+tt.expCode+"</Code>")
			} else {
				require.Equal(t, "etag", objectAPI.gotSrcInfo.ETag)
			}
		})
	}
}

func TestGetObjectAcl(t *testing.T) {
	objectAPI := singleObjectLayer(cmd.ObjectInfo{Bucket: "bucket", Name: "key", ModTime: time.Now()}, "")

	resp := serve(t, objectAPI, http.MethodGet, "/bucket/key?acl", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, "application/xml", resp.Header.Get("Content-Type"))
	require.True(t, strings.HasPrefix(resp.Body, `<?xml version="1.0" encoding="UTF-8"?>`), resp.Body)
	require.Contains(t, resp.Body, `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	require.Contains(t, resp.Body, "<Owner><ID>7b25a206cc747e61355f1af9395c2e1dc93664b7b64838ca859b245e20dead3c</ID><DisplayName>storj</DisplayName></Owner>")
}

// versionedCopyPartLayer models the storage layer selecting the latest version when
// CopyObjectPart is called without an explicit source version.
type versionedCopyPartLayer struct {
	*fakeObjectLayer
	versions      map[string]string
	latestVersion string
	copied        string
}

func (l *versionedCopyPartLayer) CopyObjectPart(_ context.Context, _, _, _, _, _ string, partID int, _, _ int64, _ cmd.ObjectInfo, srcOpts, _ cmd.ObjectOptions) (cmd.PartInfo, error) {
	version := srcOpts.VersionID
	if version == "" {
		version = l.latestVersion
	}
	l.copied = l.versions[version]
	return cmd.PartInfo{PartNumber: partID, ETag: "partetag"}, nil
}

func TestUploadPartCopySourceVersion(t *testing.T) {
	const oldVersion = "00000000-0000-0000-0000-000000000001"
	const newVersion = "00000000-0000-0000-0000-000000000002"
	for _, source := range []string{"/bucket/src", "/bucket/src?versionId=" + oldVersion} {
		t.Run(source, func(t *testing.T) {
			layer := &versionedCopyPartLayer{
				fakeObjectLayer: &fakeObjectLayer{},
				versions:        map[string]string{oldVersion: "old contents", newVersion: "new contents"},
				latestVersion:   oldVersion,
			}
			layer.getObjectInfo = func(context.Context, string, string, cmd.ObjectOptions) (cmd.ObjectInfo, error) {
				info := cmd.ObjectInfo{VersionID: oldVersion, ETag: "old-etag", Size: 12, ModTime: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)}
				// Another writer replaces the source after stat but before the part download.
				layer.latestVersion = newVersion
				return info, nil
			}
			resp := serve(t, layer, http.MethodPut, "/bucket/dst?partNumber=1&uploadId=id", http.Header{
				"X-Amz-Copy-Source":          {source},
				"X-Amz-Copy-Source-If-Match": {`"old-etag"`},
			}, nil)
			require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
			require.Equal(t, "old contents", layer.copied)
		})
	}
}
