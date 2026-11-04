// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"

	"storj.io/gateway/api"
	"storj.io/minio/cmd"
	objectlock "storj.io/minio/pkg/bucket/object/lock"
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
		// S3 compares ETags weakly for If-Match too.
		{name: "if-match weak etag", ifMatch: `W/"etag"`, expGetStatus: http.StatusOK, expCopyStatus: http.StatusOK},
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

// TestPreconditionsWithoutETag checks that an object without an ETag, such as one uploaded with
// uplink, matches no ETag, not even an empty one.
func TestPreconditionsWithoutETag(t *testing.T) {
	objectAPI := singleObjectLayer(cmd.ObjectInfo{Bucket: "bucket", Name: "key", Size: 4}, "data")
	objectAPI.copyObject = func(_ context.Context, _, _, dstBucket, dstObject string, _ cmd.ObjectInfo, _, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
		return cmd.ObjectInfo{Bucket: dstBucket, Name: dstObject}, nil
	}

	for _, tt := range []struct {
		header                   http.Header
		expStatus, expCopyStatus int
	}{
		{http.Header{"If-Match": {""}}, http.StatusPreconditionFailed, http.StatusPreconditionFailed},
		{http.Header{"If-Match": {`""`}}, http.StatusPreconditionFailed, http.StatusPreconditionFailed},
		{http.Header{"If-Match": {`"a",`}}, http.StatusPreconditionFailed, http.StatusPreconditionFailed},
		{http.Header{"If-Match": {"*"}}, http.StatusOK, http.StatusOK},
		{http.Header{"If-None-Match": {""}}, http.StatusOK, http.StatusOK},
		{http.Header{"If-None-Match": {`""`}}, http.StatusOK, http.StatusOK},
		{http.Header{"If-None-Match": {`"a",`}}, http.StatusOK, http.StatusOK},
		{http.Header{"If-None-Match": {"*"}}, http.StatusNotModified, http.StatusPreconditionFailed},
	} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			resp := serve(t, objectAPI, method, "/bucket/key", tt.header, nil)
			require.Equal(t, tt.expStatus, resp.StatusCode, "%s %v", method, tt.header)
		}

		copyHeader := http.Header{"X-Amz-Copy-Source": {"/bucket/key"}}
		for k, v := range tt.header {
			copyHeader["X-Amz-Copy-Source-"+k] = v
		}
		resp := serve(t, objectAPI, http.MethodPut, "/bucket/dst", copyHeader, nil)
		require.Equal(t, tt.expCopyStatus, resp.StatusCode, "CopyObject %v: %s", tt.header, resp.Body)
	}

	// An If-Range that doesn't match serves the whole object.
	resp := serve(t, objectAPI, http.MethodGet, "/bucket/key", http.Header{"Range": {"bytes=0-1"}, "If-Range": {`""`}}, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestIfRangeLikeS3 checks the responses that S3 returned for If-Range values on GetObject and
// HeadObject, for a satisfiable and an unsatisfiable range.
func TestIfRangeLikeS3(t *testing.T) {
	objectAPI := singleObjectLayer(cmd.ObjectInfo{Bucket: "bucket", Name: "key", Size: 10, ETag: "etag"}, "0123456789")

	for _, tt := range []struct {
		name                       string
		ifRange                    []string
		expStatus, expUnsatisfying int
	}{
		{"no If-Range", nil, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable},
		{"matching ETag", []string{`"etag"`}, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable},
		{"empty", []string{""}, http.StatusOK, http.StatusOK},
		{"quoted empty", []string{`""`}, http.StatusOK, http.StatusOK},
		// Only the first of repeated lines is used.
		{"empty then matching ETag", []string{"", `"etag"`}, http.StatusOK, http.StatusOK},
		{"matching ETag then empty", []string{`"etag"`, ""}, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, method := range []string{http.MethodGet, http.MethodHead} {
				for rangeValue, expStatus := range map[string]int{"bytes=2-4": tt.expStatus, "bytes=10-": tt.expUnsatisfying} {
					header := http.Header{"Range": {rangeValue}}
					if tt.ifRange != nil {
						header["If-Range"] = tt.ifRange
					}
					resp := serve(t, objectAPI, method, "/bucket/key", header, nil)
					require.Equal(t, expStatus, resp.StatusCode, "%s %s: %s", method, rangeValue, resp.Body)
				}
			}
		})
	}
}

// TestPreconditionsLikeS3 checks the responses that S3 returned for the same conditional headers on
// GetObject, HeadObject and the source of CopyObject.
func TestPreconditionsLikeS3(t *testing.T) {
	modTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	objInfo := cmd.ObjectInfo{Bucket: "bucket", Name: "src", ModTime: modTime, Size: 10, ETag: "etag"}
	objectAPI := singleObjectLayer(objInfo, "0123456789")
	objectAPI.copyObject = func(_ context.Context, _, _, dstBucket, dstObject string, _ cmd.ObjectInfo, _, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
		return cmd.ObjectInfo{Bucket: dstBucket, Name: dstObject, ETag: "newetag", ModTime: modTime}, nil
	}
	future := time.Now().Add(48 * time.Hour).UTC().Format(http.TimeFormat)

	for _, tt := range []struct {
		name                  string
		header                http.Header
		expGet, expCopyStatus int
	}{
		{"future If-Modified-Since is ignored", http.Header{"If-Modified-Since": {future}}, http.StatusOK, http.StatusOK},
		{"future If-Unmodified-Since", http.Header{"If-Unmodified-Since": {future}}, http.StatusOK, http.StatusOK},
		{"two If-Match lines, bad then good", http.Header{"If-Match": {`"other"`, `"etag"`}}, http.StatusOK, http.StatusOK},
		{"two If-None-Match lines, other then matching", http.Header{"If-None-Match": {`"other"`, `"etag"`}}, http.StatusNotModified, http.StatusPreconditionFailed},
		{"empty If-Match fails", http.Header{"If-Match": {""}}, http.StatusPreconditionFailed, http.StatusPreconditionFailed},
		{"empty If-None-Match is ignored", http.Header{"If-None-Match": {""}}, http.StatusOK, http.StatusOK},
		// Even an empty If-None-Match makes S3 ignore If-Modified-Since.
		{"empty If-None-Match ignores If-Modified-Since", http.Header{"If-None-Match": {""}, "If-Modified-Since": {modTime.Format(http.TimeFormat)}}, http.StatusOK, http.StatusOK},
		{"two empty If-None-Match lines ignore If-Modified-Since", http.Header{"If-None-Match": {"", ""}, "If-Modified-Since": {modTime.Format(http.TimeFormat)}}, http.StatusOK, http.StatusOK},
		{"If-Modified-Since alone", http.Header{"If-Modified-Since": {modTime.Format(http.TimeFormat)}}, http.StatusNotModified, http.StatusPreconditionFailed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, method := range []string{http.MethodGet, http.MethodHead} {
				resp := serve(t, objectAPI, method, "/bucket/src", tt.header, nil)
				require.Equal(t, tt.expGet, resp.StatusCode, "%s: %s", method, resp.Body)
			}

			copyHeader := http.Header{"X-Amz-Copy-Source": {"/bucket/src"}}
			for name, values := range tt.header {
				copyHeader["X-Amz-Copy-Source-"+name] = values
			}
			resp := serve(t, objectAPI, http.MethodPut, "/bucket/dst", copyHeader, nil)
			require.Equal(t, tt.expCopyStatus, resp.StatusCode, "CopyObject: %s", resp.Body)
		})
	}
}

func TestCompleteMultipartUploadDuplicatePart(t *testing.T) {
	resp := serve(t, &fakeObjectLayer{}, http.MethodPost, "/bucket/key?uploadId=upload-id", nil, []byte(`<CompleteMultipartUpload>`+
		`<Part><PartNumber>1</PartNumber><ETag>a</ETag></Part>`+
		`<Part><PartNumber>1</PartNumber><ETag>b</ETag></Part>`+
		`</CompleteMultipartUpload>`))
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>InvalidPartOrder</Code>")

	// Part numbers out of range are rejected before they're compared.
	resp = serve(t, &fakeObjectLayer{}, http.MethodPost, "/bucket/key?uploadId=upload-id", nil, []byte(`<CompleteMultipartUpload>`+
		`<Part><PartNumber>10000</PartNumber><ETag>a</ETag></Part>`+
		`<Part><PartNumber>-1</PartNumber><ETag>b</ETag></Part>`+
		`</CompleteMultipartUpload>`))
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>InvalidArgument</Code><Message>PartNumber must be &gt;= 1</Message>")
}

// TestCompleteMultipartUploadLikeS3 checks the responses that S3 returned for the same requests.
func TestCompleteMultipartUploadLikeS3(t *testing.T) {
	var got []cmd.CompletePart
	objectAPI := &fakeObjectLayer{completeMultipartUpload: func(_ context.Context, _, _, _ string, parts []cmd.CompletePart, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
		got = parts
		return cmd.ObjectInfo{ETag: "etag"}, nil
	}}
	complete := func(query, body string) response {
		return serve(t, objectAPI, http.MethodPost, "/bucket/key?"+query, nil, []byte(body))
	}
	part := func(inner string) string {
		return "<CompleteMultipartUpload><Part>" + inner + "</Part></CompleteMultipartUpload>"
	}

	for _, tt := range []struct{ name, query, body, code string }{
		{"empty body", "uploadId=id", "", "InvalidRequest"},
		{"empty upload ID", "uploadId=", part("<PartNumber>1</PartNumber><ETag>a</ETag>"), "NoSuchUpload"},
		{"no PartNumber", "uploadId=id", part("<ETag>a</ETag>"), "MalformedXML"},
		{"two PartNumbers", "uploadId=id", part("<PartNumber>1</PartNumber><PartNumber>1</PartNumber><ETag>a</ETag>"), "MalformedXML"},
		{"two ETags", "uploadId=id", part("<PartNumber>1</PartNumber><ETag>a</ETag><ETag>a</ETag>"), "MalformedXML"},
		{"PartNumber not an integer", "uploadId=id", part("<PartNumber>abc</PartNumber><ETag>a</ETag>"), "MalformedXML"},
		{"PartNumber 2^31", "uploadId=id", part("<PartNumber>2147483648</PartNumber><ETag>a</ETag>"), "MalformedXML"},
		{"PartNumber 2^31-1", "uploadId=id", part("<PartNumber>2147483647</PartNumber><ETag>a</ETag>"), "InvalidPart"},
		{"empty ETag", "uploadId=id", part("<PartNumber>1</PartNumber><ETag></ETag>"), "InvalidPart"},
		{"unknown element in Part", "uploadId=id", part("<PartNumber>1</PartNumber><ETag>a</ETag><Foo/>"), "MalformedXML"},
		{"unknown checksum", "uploadId=id", part("<PartNumber>1</PartNumber><ETag>a</ETag><ChecksumBogus>x</ChecksumBogus>"), "MalformedXML"},
		{"two checksums of one algorithm", "uploadId=id", part("<PartNumber>1</PartNumber><ETag>a</ETag><ChecksumCRC32>AAAAAA==</ChecksumCRC32><ChecksumCRC32>AAAAAA==</ChecksumCRC32>"), "MalformedXML"},
		{"unknown element in root", "uploadId=id", "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>a</ETag></Part><Foo/></CompleteMultipartUpload>", "MalformedXML"},
		{"text in root", "uploadId=id", "<CompleteMultipartUpload>hi<Part><PartNumber>1</PartNumber><ETag>a</ETag></Part></CompleteMultipartUpload>", "MalformedXML"},
		{"text after the parts", "uploadId=id", "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>a</ETag></Part>hi</CompleteMultipartUpload>", "MalformedXML"},
		{"entity in root", "uploadId=id", "<CompleteMultipartUpload>&amp;<Part><PartNumber>1</PartNumber><ETag>a</ETag></Part></CompleteMultipartUpload>", "MalformedXML"},
		{"non-breaking space in root", "uploadId=id", "<CompleteMultipartUpload>\u00a0<Part><PartNumber>1</PartNumber><ETag>a</ETag></Part></CompleteMultipartUpload>", "MalformedXML"},
		{"text in Part", "uploadId=id", part("x<PartNumber>1</PartNumber><ETag>a</ETag>"), "MalformedXML"},
		{"PartNumber sign only", "uploadId=id", part("<PartNumber>+</PartNumber><ETag>a</ETag>"), "MalformedXML"},
		// S3 reads an empty part number as 0, and checks every number for being too small first.
		{"empty PartNumber", "uploadId=id", part("<PartNumber></PartNumber><ETag>a</ETag>"), "InvalidArgument"},
		{"blank PartNumber", "uploadId=id", part("<PartNumber> </PartNumber><ETag>a</ETag>"), "InvalidArgument"},
		{"PartNumber +0", "uploadId=id", part("<PartNumber>+0</PartNumber><ETag>a</ETag>"), "InvalidArgument"},
		{"PartNumber too large then too small", "uploadId=id", part("<PartNumber>10001</PartNumber><ETag>a</ETag></Part><Part><PartNumber>-1</PartNumber><ETag>a</ETag>"), "InvalidArgument"},
		{"PartNumber too large", "uploadId=id", part("<PartNumber>10001</PartNumber><ETag>a</ETag>"), "InvalidPart"},
		// S3 validates the checksums before the parts, even with a wrong ETag.
		{"checksum that isn't base64", "uploadId=id", part("<PartNumber>1</PartNumber><ETag>a</ETag><ChecksumCRC32>!!!</ChecksumCRC32>"), "InvalidArgument"},
		{"checksum without padding", "uploadId=id", part("<PartNumber>1</PartNumber><ETag>a</ETag><ChecksumCRC32>AQEBAQ</ChecksumCRC32>"), "InvalidArgument"},
		{"checksum with a newline", "uploadId=id", part("<PartNumber>1</PartNumber><ETag>a</ETag><ChecksumCRC32>AQEBAQ==\n</ChecksumCRC32>"), "InvalidArgument"},
		{"checksum with spaces", "uploadId=id", part("<PartNumber>1</PartNumber><ETag>a</ETag><ChecksumCRC32> AQEBAQ== </ChecksumCRC32>"), "InvalidArgument"},
		{"checksum of the wrong size", "uploadId=id", part("<PartNumber>1</PartNumber><ETag>a</ETag><ChecksumSHA256>AAAA</ChecksumSHA256>"), "InvalidArgument"},
		{"CRC32 of 5 bytes", "uploadId=id", part("<PartNumber>1</PartNumber><ETag>a</ETag><ChecksumCRC32>AQEBAQE=</ChecksumCRC32>"), "InvalidArgument"},
		{"two checksums", "uploadId=id", part("<PartNumber>1</PartNumber><ETag>a</ETag><ChecksumCRC32>AQEBAQ==</ChecksumCRC32><ChecksumSHA1>AQEBAQEBAQEBAQEBAQEBAQEBAQE=</ChecksumSHA1>"), "InvalidArgument"},
		{"checksum before a too small part number", "uploadId=id", part("<PartNumber>0</PartNumber><ETag>a</ETag><ChecksumCRC32>AAAA</ChecksumCRC32>"), "InvalidArgument"},
	} {
		got = nil
		resp := complete(tt.query, tt.body)
		require.Contains(t, resp.Body, "<Code>"+tt.code+"</Code>", tt.name)
		require.Nil(t, got, tt.name)
	}

	resp := complete("uploadId=id", part("<PartNumber>1</PartNumber><ETag>a</ETag><ChecksumCRC32>!!!</ChecksumCRC32>"))
	require.Contains(t, resp.Body, "<Message>Invalid Base64 or multiple checksums present in request</Message>")
	require.Contains(t, resp.Body, "<ArgumentName>Checksum</ArgumentName><ArgumentValue>CRC32:!!!;</ArgumentValue>")
	resp = complete("uploadId=id", part("<PartNumber>1</PartNumber><ETag>a</ETag><ChecksumCRC32>AQEBAQ==</ChecksumCRC32><ChecksumSHA1>AAAA</ChecksumSHA1>"))
	require.Contains(t, resp.Body, "<ArgumentValue>CRC32:AQEBAQ==;SHA1:AAAA;</ArgumentValue>")
	resp = complete("uploadId=id", part("<PartNumber></PartNumber><ETag>a</ETag>"))
	require.Contains(t, resp.Body, "<Message>PartNumber must be &gt;= 1</Message>")
	require.Contains(t, resp.Body, "<ArgumentValue>0</ArgumentValue>")

	resp = complete("uploadId=id", part("<PartNumber>2</PartNumber><ETag>a</ETag></Part><Part><PartNumber>1</PartNumber><ETag>a</ETag>"))
	require.Contains(t, resp.Body, "<Code>InvalidPartOrder</Code><Message>The list of parts was not in ascending order. Parts must be ordered by part number.</Message>")
	resp = complete("uploadId=id", part("<PartNumber>1</PartNumber><ETag>a</ETag></Part><Part><PartNumber>1</PartNumber><ETag>a</ETag>"))
	require.Contains(t, resp.Body, "<Code>InvalidPartOrder</Code><Message>The list of parts was not in ascending order. Parts must be ordered by part number.</Message>")
	resp = complete("uploadId=", part("<PartNumber>1</PartNumber><ETag>a</ETag>"))
	require.Contains(t, resp.Body, "<Code>NoSuchUpload</Code><Message>The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.</Message>")

	// Whitespace between the elements, an empty checksum and a checksum of each size are accepted.
	got = nil
	resp = complete("uploadId=id", "<CompleteMultipartUpload>\n <Part> <PartNumber>1</PartNumber> <ETag>a</ETag> <ChecksumCRC32/> </Part>\n</CompleteMultipartUpload>")
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, []cmd.CompletePart{{PartNumber: 1, ETag: "a"}}, got)

	// Any root element, part number whitespace and leading zeros, and the checksum elements are
	// accepted.
	resp = complete("uploadId=id", "<Complete><Part><PartNumber> 01 </PartNumber><ETag>a</ETag>"+
		"<ChecksumCRC32>AAAAAA==</ChecksumCRC32></Part></Complete>")
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, []cmd.CompletePart{{PartNumber: 1, ETag: "a"}}, got)
}

// TestUploadPartHeadersLikeS3 checks the responses that S3 returned for the same requests.
func TestUploadPartHeadersLikeS3(t *testing.T) {
	const (
		notValid = "<Code>InvalidArgument</Code><Message>The specified header is not valid in this context</Message>"
		metadata = "<Code>InvalidArgument</Code><Message>Metadata cannot be specified in this context.</Message>"
	)
	for _, copySource := range []string{"", "/bucket/src"} {
		for _, tt := range []struct {
			header http.Header
			want   string
		}{
			{http.Header{"X-Amz-Storage-Class": {""}}, notValid},
			{http.Header{"X-Amz-Tagging": {"a=1"}}, notValid},
			{http.Header{"X-Amz-Acl": {"private"}}, notValid},
			{http.Header{"X-Amz-Meta-": {"x"}}, metadata},
			{http.Header{"X-Amz-Meta-Foo": {"x"}, "X-Amz-Storage-Class": {"STANDARD"}}, notValid},
			{http.Header{"X-Amz-Website-Redirect-Location": {"/x"}}, "x-amz-website-redirect-location header is not supported for this operation."},
			{http.Header{"X-Amz-Server-Side-Encryption": {"AES256"}}, "x-amz-server-side-encryption header is not supported for this operation."},
			// With several, S3 reports encryption first, then the website redirect location, then
			// the headers that aren't valid, then metadata.
			{http.Header{"X-Amz-Meta-Foo": {"x"}, "X-Amz-Website-Redirect-Location": {"/x"}}, "x-amz-website-redirect-location header is not supported for this operation."},
			{http.Header{"X-Amz-Website-Redirect-Location": {"/x"}, "X-Amz-Server-Side-Encryption": {"AES256"}}, "x-amz-server-side-encryption header is not supported for this operation."},
			{http.Header{"X-Amz-Storage-Class": {"STANDARD"}, "X-Amz-Website-Redirect-Location": {"/x"}}, "x-amz-website-redirect-location header is not supported for this operation."},
			{http.Header{"X-Amz-Acl": {"private"}, "X-Amz-Server-Side-Encryption": {"AES256"}}, "x-amz-server-side-encryption header is not supported for this operation."},
			{http.Header{"X-Amz-Meta-Foo": {"x"}, "X-Amz-Website-Redirect-Location": {"/x"}, "X-Amz-Server-Side-Encryption": {"AES256"}}, "x-amz-server-side-encryption header is not supported for this operation."},
			{http.Header{"X-Amz-Tagging": {"a=1"}, "X-Amz-Meta-Foo": {"x"}}, notValid},
		} {
			header := tt.header.Clone()
			if copySource != "" {
				header.Set("X-Amz-Copy-Source", copySource)
			} else {
				header.Set("Content-Length", "1")
			}
			objectAPI := &readingObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
			resp := serve(t, objectAPI, http.MethodPut, "/bucket/key?partNumber=1&uploadId=id", header, []byte("x")[:len(header.Get("Content-Length"))])
			require.Equal(t, http.StatusBadRequest, resp.StatusCode, "%v: %s", header, resp.Body)
			require.Contains(t, resp.Body, tt.want, header)
			require.Empty(t, objectAPI.Calls(), header)
		}

		// The directives are only refused on UploadPartCopy.
		header := http.Header{"X-Amz-Metadata-Directive": {"COPY"}}
		if copySource != "" {
			header.Set("X-Amz-Copy-Source", copySource)
			resp := serve(t, &fakeObjectLayer{}, http.MethodPut, "/bucket/key?partNumber=1&uploadId=id", header, nil)
			require.Contains(t, resp.Body, notValid)
		} else {
			header.Set("Content-Length", "1")
			objectAPI := &readingObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
			resp := serve(t, objectAPI, http.MethodPut, "/bucket/key?partNumber=1&uploadId=id", header, []byte("x"))
			require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		}

		// A part number without an upload ID.
		for _, target := range []string{"/bucket/key?partNumber=1&uploadId=", "/bucket/key?partNumber=&uploadId=", "/bucket/key?partNumber=1"} {
			header := http.Header{"Content-Length": {"0"}}
			if copySource != "" {
				header = http.Header{"X-Amz-Copy-Source": {copySource}}
			}
			resp := serve(t, &fakeObjectLayer{}, http.MethodPut, target, header, nil)
			require.Equal(t, http.StatusBadRequest, resp.StatusCode, target)
			require.Contains(t, resp.Body, "<Message>This operation does not accept partNumber without uploadId</Message>", target)
		}
	}

	// Headers that S3 accepts on parts.
	header := http.Header{
		"Content-Length":               {"1"},
		"Content-Type":                 {"text/plain"},
		"Cache-Control":                {"no-cache"},
		"X-Amz-Object-Lock-Mode":       {"GOVERNANCE"},
		"X-Amz-Object-Lock-Legal-Hold": {"ON"},
		"X-Amz-Grant-Read":             {"id=abc"},
		"X-Amz-Tagging-Directive":      {"bogus"},
	}
	objectAPI := &readingObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
	resp := serve(t, objectAPI, http.MethodPut, "/bucket/key?partNumber=1&uploadId=id", header, []byte("x"))
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
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

type retentionObjectLayer struct {
	*fakeObjectLayer
	retention *objectlock.ObjectRetention
}

func (l *retentionObjectLayer) PutObject(_ context.Context, bucket, object string, _ *cmd.PutObjReader, opts cmd.ObjectOptions) (cmd.ObjectInfo, error) {
	l.retention = opts.Retention
	return cmd.ObjectInfo{Bucket: bucket, Name: object}, nil
}

func (l *retentionObjectLayer) SetObjectRetention(_ context.Context, _, _, _ string, opts cmd.ObjectOptions) error {
	l.retention = opts.Retention
	return nil
}

func TestRetainUntilDateTruncatedToMilliseconds(t *testing.T) {
	want := time.Date(2100, 1, 2, 3, 4, 5, 123000000, time.UTC)

	objectAPI := &retentionObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
	resp := serve(t, objectAPI, http.MethodPut, "/bucket/key", http.Header{
		"Content-Length":                      {"3"},
		"X-Amz-Object-Lock-Mode":              {"GOVERNANCE"},
		"X-Amz-Object-Lock-Retain-Until-Date": {"2100-01-02T03:04:05.123456Z"},
	}, []byte("abc"))
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, want, objectAPI.retention.RetainUntilDate.Time)

	objectAPI = &retentionObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
	body := []byte(`<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2100-01-02T03:04:05.123456Z</RetainUntilDate></Retention>`)
	resp = serve(t, objectAPI, http.MethodPut, "/bucket/key?retention", http.Header{"Content-Md5": {base64MD5(body)}}, body)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, want, objectAPI.retention.RetainUntilDate.Time)
}

func TestPutObjectWithoutContentLength(t *testing.T) {
	// A request without Content-Length or Transfer-Encoding has an empty body to net/http.
	resp := serve(t, putObjectLayer{&fakeObjectLayer{}}, http.MethodPut, "/bucket/key", nil, nil)
	require.Equal(t, http.StatusLengthRequired, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>MissingContentLength</Code>")

	resp = serve(t, putObjectLayer{&fakeObjectLayer{}}, http.MethodPut, "/bucket/key", http.Header{"Content-Length": {"0"}}, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
}

func TestPutObjectStreamingWithoutContentLength(t *testing.T) {
	data := []byte("hello")
	body := fmt.Appendf(nil, "%x\r\n%s\r\n0\r\nx-amz-checksum-crc32:%s\r\n\r\n", len(data), data, crc32Base64(data))
	objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
	// Transfer-Encoding: chunked requests have no Content-Length header.
	resp := serve(t, objectAPI, http.MethodPut, "/bucket/key", http.Header{
		"X-Amz-Content-Sha256":         {"STREAMING-UNSIGNED-PAYLOAD-TRAILER"},
		"Content-Encoding":             {"aws-chunked"},
		"X-Amz-Decoded-Content-Length": {strconv.Itoa(len(data))},
		"X-Amz-Trailer":                {"x-amz-checksum-crc32"},
	}, body)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, data, objectAPI.data)
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
		{name: "whole source too large", header: http.Header{"X-Amz-Copy-Source": {"/bucket/large"}}, expStatus: http.StatusBadRequest, expCode: "InvalidRequest"},
		{name: "range of large source", header: http.Header{"X-Amz-Copy-Source": {"/bucket/large"}, "X-Amz-Copy-Source-Range": {"bytes=0-9"}}, expStatus: http.StatusOK},
		{name: "range length overflows", header: http.Header{"X-Amz-Copy-Source": {"/bucket/large"}, "X-Amz-Copy-Source-Range": {"bytes=0-9223372036854775807"}}, expStatus: http.StatusRequestedRangeNotSatisfiable, expCode: "InvalidRange"},
		{name: "owner and payer headers", header: http.Header{
			"X-Amz-Copy-Source":                  {"/bucket/small"},
			"X-Amz-Expected-Bucket-Owner":        {"123456789012"},
			"X-Amz-Source-Expected-Bucket-Owner": {"123456789012"},
			"X-Amz-Request-Payer":                {"requester"},
		}, expStatus: http.StatusOK},
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

func TestPutAcl(t *testing.T) {
	ownerGrant := `<Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>7b25a206cc747e61355f1af9395c2e1dc93664b7b64838ca859b245e20dead3c</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant>`
	publicGrant := `<Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ</Permission></Grant>`
	policy := func(grants ...string) string {
		return `<AccessControlPolicy><AccessControlList>` + strings.Join(grants, "") + `</AccessControlList></AccessControlPolicy>`
	}

	for _, tt := range []struct {
		name     string
		header   http.Header
		body     string
		wantCode string
	}{
		{name: "owner", body: policy(ownerGrant)},
		{name: "owner twice", body: policy(ownerGrant, ownerGrant)},
		{name: "owner and public", body: policy(ownerGrant, publicGrant), wantCode: "AccessControlListNotSupported"},
		{name: "no grants", body: policy(), wantCode: "AccessControlListNotSupported"},
		{name: "private", header: http.Header{"X-Amz-Acl": {"private"}}},
		{name: "bucket-owner-full-control", header: http.Header{"X-Amz-Acl": {"bucket-owner-full-control"}}},
		{name: "public-read", header: http.Header{"X-Amz-Acl": {"public-read"}}, wantCode: "AccessControlListNotSupported"},
		{name: "bogus canned", header: http.Header{"X-Amz-Acl": {"bogus"}}, wantCode: "AccessControlListNotSupported"},
		{name: "canned and body", header: http.Header{"X-Amz-Acl": {"private"}}, body: policy(ownerGrant), wantCode: "InvalidRequest"},
		{name: "canned and grant", header: http.Header{"X-Amz-Acl": {"private"}, "X-Amz-Grant-Read": {"id=x"}}, wantCode: "InvalidRequest"},
		{name: "grant", header: http.Header{"X-Amz-Grant-Read": {"id=x"}}, wantCode: "AccessControlListNotSupported"},
	} {
		for _, target := range []string{"/bucket/key?acl", "/bucket?acl"} {
			t.Run(tt.name+" "+target, func(t *testing.T) {
				resp := serve(t, newBodyObjectLayer(), http.MethodPut, target, tt.header, []byte(tt.body))
				if tt.wantCode != "" {
					require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
					require.Contains(t, resp.Body, "<Code>"+tt.wantCode+"</Code>")
					return
				}
				require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
				require.Equal(t, "0", resp.Header.Get("Content-Length"))
				require.NotEmpty(t, resp.Header.Get("Server"))
			})
		}
	}
}

func TestObjectAclVersionID(t *testing.T) {
	const versionID = "00000000000000000000000000000001"
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		var got string
		objectAPI := &fakeObjectLayer{getObjectInfo: func(_ context.Context, _, _ string, opts cmd.ObjectOptions) (cmd.ObjectInfo, error) {
			got = opts.VersionID
			return cmd.ObjectInfo{}, nil
		}}
		resp := serve(t, objectAPI, method, "/bucket/key?acl&versionId="+versionID, http.Header{"X-Amz-Acl": {"private"}}, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Equal(t, versionID, got, method)
	}
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
	const oldVersion = "00000000000000000000000000000001"
	const newVersion = "00000000000000000000000000000002"
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
			require.Equal(t, oldVersion, resp.Header.Get("X-Amz-Copy-Source-Version-Id"))
		})
	}
}

// TestPutObjectRetentionLikeS3 checks the responses that S3 returned for the same requests.
func TestPutObjectRetentionLikeS3(t *testing.T) {
	objectAPI := &retentionObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
	put := func(inner string) response {
		body := []byte("<Retention>" + inner + "</Retention>")
		return serve(t, objectAPI, http.MethodPut, "/bucket/key?retention", http.Header{"Content-Md5": {base64MD5(body)}}, body)
	}
	date := time.Now().UTC().Add(time.Hour).Truncate(time.Second)

	for _, inner := range []string{
		"<Mode>GOVERNANCE</Mode><RetainUntilDate>" + date.Format("2006-01-02T15:04") + "Z</RetainUntilDate>",
		"<Mode>GOVERNANCE</Mode><RetainUntilDate>" + date.Format("2006-01-02") + "</RetainUntilDate>",
		"<Mode>GOVERNANCE</Mode><RetainUntilDate>" + date.Format(http.TimeFormat) + "</RetainUntilDate>",
		"<Mode>GOVERNANCE</Mode><RetainUntilDate></RetainUntilDate>",
		"<Mode>GOVERNANCE</Mode><RetainUntilDate>soon</RetainUntilDate>",
		"<Mode>GOVERNANCE</Mode><RetainUntilDate>" + date.Format(time.RFC3339) + "</RetainUntilDate><Foo/>",
		"<Mode>GOVERNANCE</Mode><Mode>GOVERNANCE</Mode><RetainUntilDate>" + date.Format(time.RFC3339) + "</RetainUntilDate>",
		"<Mode>GOVERNANCE</Mode><RetainUntilDate>" + date.Format(time.RFC3339) + "</RetainUntilDate><RetainUntilDate>" + date.Format(time.RFC3339) + "</RetainUntilDate>",
		"<Mode>governance</Mode><RetainUntilDate>" + date.Format(time.RFC3339) + "</RetainUntilDate>",
		"<Mode>GOVERNANCE</Mode>",
		// Text in the root, but not whitespace.
		"hi<Mode>GOVERNANCE</Mode><RetainUntilDate>" + date.Format(time.RFC3339) + "</RetainUntilDate>",
		"<Mode>GOVERNANCE</Mode><RetainUntilDate>" + date.Format(time.RFC3339) + "</RetainUntilDate>hi",
		"<![CDATA[hi]]><Mode>GOVERNANCE</Mode><RetainUntilDate>" + date.Format(time.RFC3339) + "</RetainUntilDate>",
		"&amp;<Mode>GOVERNANCE</Mode><RetainUntilDate>" + date.Format(time.RFC3339) + "</RetainUntilDate>",
		"\u00a0<Mode>GOVERNANCE</Mode><RetainUntilDate>" + date.Format(time.RFC3339) + "</RetainUntilDate>",
	} {
		objectAPI.retention = nil
		resp := put(inner)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, inner)
		require.Contains(t, resp.Body, "<Code>MalformedXML</Code>", inner)
		require.Nil(t, objectAPI.retention, inner)
	}

	// Whitespace and comments between the elements are accepted.
	objectAPI.retention = nil
	resp := put("\n\t <!-- c --><Mode>GOVERNANCE</Mode>\r\n<![CDATA[ ]]><RetainUntilDate>" + date.Format(time.RFC3339) + "</RetainUntilDate> ")
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.NotNil(t, objectAPI.retention)

	// A date without a time zone is in UTC, and whitespace around it is ignored.
	for _, value := range []string{
		date.Format("2006-01-02T15:04:05"),
		" " + date.Format(time.RFC3339) + " ",
		date.In(time.FixedZone("", 2*60*60)).Format(time.RFC3339),
		date.Add(123 * time.Millisecond).Format(time.RFC3339Nano),
	} {
		objectAPI.retention = nil
		resp := put("<Mode>GOVERNANCE</Mode><RetainUntilDate>" + value + "</RetainUntilDate>")
		require.Equal(t, http.StatusOK, resp.StatusCode, value+": "+resp.Body)
		require.NotNil(t, objectAPI.retention, value)
		require.True(t, objectAPI.retention.RetainUntilDate.Truncate(time.Second).Equal(date), value)
	}

	// Any root element is accepted.
	body := []byte("<Ret><Mode>GOVERNANCE</Mode><RetainUntilDate>" + date.Format(time.RFC3339) + "</RetainUntilDate></Ret>")
	resp = serve(t, objectAPI, http.MethodPut, "/bucket/key?retention", http.Header{"Content-Md5": {base64MD5(body)}}, body)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
}

// legalHoldObjectLayer records the legal hold set on an object.
type legalHoldObjectLayer struct {
	*fakeObjectLayer
	got *objectlock.ObjectLegalHold
}

func (l *legalHoldObjectLayer) SetObjectLegalHold(_ context.Context, _, _, _ string, hold *objectlock.ObjectLegalHold) error {
	l.got = hold
	return nil
}

// TestPutObjectLegalHoldLikeS3 checks the responses that S3 returned for the same requests.
func TestPutObjectLegalHoldLikeS3(t *testing.T) {
	objectAPI := &legalHoldObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
	put := func(body string) response {
		return serve(t, objectAPI, http.MethodPut, "/bucket/key?legal-hold", http.Header{"Content-Md5": {base64MD5([]byte(body))}}, []byte(body))
	}

	for _, body := range []string{
		"<LegalHold><Status>OFF</Status><Status>OFF</Status></LegalHold>",
		"<LegalHold><Status>OFF</Status><Foo/></LegalHold>",
		"<LegalHold><Status>on</Status></LegalHold>",
		"<LegalHold></LegalHold>",
		// Text in the root, but not whitespace.
		"<LegalHold>hi<Status>ON</Status></LegalHold>",
		"<LegalHold><Status>ON</Status>hi</LegalHold>",
		"<LegalHold><![CDATA[hi]]><Status>ON</Status></LegalHold>",
		"<LegalHold>&amp;<Status>ON</Status></LegalHold>",
		"<LegalHold>\u00a0<Status>ON</Status></LegalHold>",
	} {
		objectAPI.got = nil
		resp := put(body)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, body)
		require.Contains(t, resp.Body, "<Code>MalformedXML</Code>", body)
		require.Nil(t, objectAPI.got, body)
	}

	// Any root element and declared encoding is accepted.
	for _, body := range []string{
		"<Hold><Status>ON</Status></Hold>",
		"<LegalHold>\n\t <!-- c --><Status>ON</Status>\r\n<![CDATA[ ]]></LegalHold>",
		`<?xml version="1.0" encoding="bogus"?><LegalHold><Status>ON</Status></LegalHold>`,
	} {
		objectAPI.got = nil
		resp := put(body)
		require.Equal(t, http.StatusOK, resp.StatusCode, body+": "+resp.Body)
		require.NotNil(t, objectAPI.got, body)
		require.Equal(t, objectlock.LegalHoldOn, objectAPI.got.Status, body)
	}
}

func TestUnparsableXMLIsMalformed(t *testing.T) {
	for _, target := range []string{"/bucket?tagging", "/bucket/key?tagging"} {
		resp := serve(t, &fakeObjectLayer{}, http.MethodPut, target, http.Header{"Content-Md5": {base64MD5(nil)}}, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, target+": "+resp.Body)
		require.Contains(t, resp.Body, "<Code>MissingRequestBodyError</Code>", target)
	}
	for _, target := range []string{"/bucket/key?legal-hold", "/bucket/key?retention"} {
		for _, body := range []string{"", `<?xml version="1.0" encoding="ISO-8859-1"?><Tagging></Tagging>`} {
			if target == "/bucket/key?retention" && body != "" {
				continue // an empty retention, accepted like S3 by TestPutObjectRetentionLikeS3
			}
			resp := serve(t, &fakeObjectLayer{}, http.MethodPut, target, http.Header{"Content-Md5": {base64MD5([]byte(body))}}, []byte(body))
			require.Equal(t, http.StatusBadRequest, resp.StatusCode, target+": "+resp.Body)
			require.Contains(t, resp.Body, "<Code>MalformedXML</Code>", target)
		}
	}

	objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
	resp := postObject(t, objectAPI, "/bucket",
		[]any{map[string]string{"bucket": "bucket"}, map[string]string{"key": "photo.jpg"}, map[string]string{"tagging": ""}},
		[][2]string{{"key", "photo.jpg"}, {"tagging", ""}}, "data")
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>MalformedXML</Code>")
}

func TestGetObjectTaggingVersionID(t *testing.T) {
	const versionID = "00000000000000000000000000000001"
	objectAPI := &fakeObjectLayer{
		getObjectInfo: func(context.Context, string, string, cmd.ObjectOptions) (cmd.ObjectInfo, error) {
			return cmd.ObjectInfo{VersionID: versionID, UserDefined: map[string]string{"s3:tags": "a=1"}}, nil
		},
	}

	// The version that was read is named even when the request names none.
	resp := serve(t, objectAPI, http.MethodGet, "/bucket/key?tagging", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, versionID, resp.Header.Get("X-Amz-Version-Id"))
	require.Contains(t, resp.Body, "<Tag><Key>a</Key><Value>1</Value></Tag>")
}

func TestGetObjectTaggingOrder(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		getObjectInfo: func(context.Context, string, string, cmd.ObjectOptions) (cmd.ObjectInfo, error) {
			return cmd.ObjectInfo{UserDefined: map[string]string{"s3:tags": "a=1&b=2&c=3&d=4&e=5&f=6&g=7&h=8&i=9&j=10"}}, nil
		},
	}

	// Tags are sorted by key, rather than in the random order of a map.
	resp := serve(t, objectAPI, http.MethodGet, "/bucket/key?tagging", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	var expected strings.Builder
	for i, key := range strings.Split("abcdefghij", "") {
		fmt.Fprintf(&expected, "<Tag><Key>%s</Key><Value>%d</Value></Tag>", key, i+1)
	}
	require.Contains(t, resp.Body, "<TagSet>"+expected.String()+"</TagSet>")
}

func TestReadDeleteMarkerVersion(t *testing.T) {
	const versionID = "00000000000000000000000000000001"
	objectAPI := &fakeObjectLayer{
		getObjectInfo: func(_ context.Context, bucket, object string, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
			return cmd.ObjectInfo{}, cmd.MethodNotAllowed{Bucket: bucket, Object: object}
		},
		getObjectNInfo: func(_ context.Context, bucket, object string, _ *cmd.HTTPRangeSpec, _ http.Header, _ cmd.LockType, _ cmd.ObjectOptions) (*cmd.GetObjectReader, error) {
			return nil, cmd.MethodNotAllowed{Bucket: bucket, Object: object}
		},
	}

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		resp := serve(t, objectAPI, method, "/bucket/key?versionId="+versionID, nil, nil)
		require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode, method)
		require.Equal(t, "true", resp.Header.Get("X-Amz-Delete-Marker"), method)
		require.Equal(t, versionID, resp.Header.Get("X-Amz-Version-Id"), method)
		require.Equal(t, "DELETE", resp.Header.Get("Allow"), method)
	}
}

func TestUserMetadataTooLarge(t *testing.T) {
	// The limit counts the keys without their prefix.
	atLimit := http.Header{"X-Amz-Meta-A": {strings.Repeat("v", 2<<10-1)}, "Content-Length": {"0"}}
	resp := serve(t, putObjectLayer{&fakeObjectLayer{}}, http.MethodPut, "/bucket/key", atLimit, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)

	for _, header := range []http.Header{
		{"X-Amz-Meta-A": {strings.Repeat("v", 2<<10)}},
		{"X-Amz-Meta-A": {strings.Repeat("v", 2<<9)}, "X-Minio-Meta-B": {strings.Repeat("v", 2<<9)}},
	} {
		header.Set("Content-Length", "0")
		resp := serve(t, putObjectLayer{&fakeObjectLayer{}}, http.MethodPut, "/bucket/key", header, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, "<Code>MetadataTooLarge</Code>")
	}

	resp = postObject(t, &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}, "/bucket",
		[]any{map[string]string{"bucket": "bucket"}, map[string]string{"key": "photo.jpg"}, []string{"starts-with", "$x-amz-meta-a", ""}},
		[][2]string{{"key", "photo.jpg"}, {"x-amz-meta-a", strings.Repeat("v", 2<<10)}}, "data")
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>MetadataTooLarge</Code>")
}

func TestPresignedV2QueryHeaders(t *testing.T) {
	data := []byte("data")
	sum := md5.Sum(data)
	otherSum := md5.Sum([]byte("other"))

	// put sends a SigV2 presigned PUT to resource with headers in the query, signed like botocore's
	// HmacV1QueryAuth signs them.
	put := func(objectAPI cmd.ObjectLayer, resource string, headers map[string]string, body []byte) *httptest.ResponseRecorder {
		expires := strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)
		query := url.Values{"AWSAccessKeyId": {testAccessKeyID}, "Expires": {expires}}
		var amzHeaders []string
		for k, v := range headers {
			query.Set(k, v)
			if strings.HasPrefix(k, "x-amz-") {
				amzHeaders = append(amzHeaders, k+":"+v+"\n")
			}
		}
		slices.Sort(amzHeaders)
		mac := hmac.New(sha1.New, []byte(testSecretAccessKey))
		mac.Write([]byte("PUT\n" + headers["content-md5"] + "\n" + headers["content-type"] + "\n" + expires + "\n" + strings.Join(amzHeaders, "") + resource))
		query.Set("Signature", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
		separator := "?"
		if strings.Contains(resource, "?") {
			separator = "&"
		}
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPut, resource+separator+query.Encode(), bytes.NewReader(body))
		req.Header.Set("Content-Length", strconv.Itoa(len(body)))
		rec := httptest.NewRecorder()
		newTestRouter(objectAPI).ServeHTTP(rec, req)
		return rec
	}

	objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
	rec := put(objectAPI, "/bucket/key", map[string]string{
		"content-md5":    base64.StdEncoding.EncodeToString(sum[:]),
		"content-type":   "text/plain",
		"x-amz-meta-foo": "bar",
	}, data)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "bar", objectAPI.metadata["X-Amz-Meta-Foo"])
	require.Equal(t, "text/plain", objectAPI.metadata["Content-Type"])

	rec = put(&postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}, "/bucket/key", map[string]string{
		"content-md5": base64.StdEncoding.EncodeToString(otherSum[:]),
	}, data)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "<Code>BadDigest</Code>")

	for _, resource := range []string{"/bucket/key?acl", "/bucket?acl"} {
		rec = put(newBodyObjectLayer(), resource, map[string]string{"x-amz-acl": "public-read"}, nil)
		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "<Code>AccessControlListNotSupported</Code>")
	}
}
