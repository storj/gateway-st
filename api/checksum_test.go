// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/gateway/api/internal/fsobjects"
	"storj.io/gateway/api/objectlayer"
	"storj.io/minio/cmd"
)

func crc32Base64(b []byte) string {
	return base64.StdEncoding.EncodeToString(binary.BigEndian.AppendUint32(nil, crc32.ChecksumIEEE(b)))
}

func TestPutObjectChecksums(t *testing.T) {
	data := []byte("hello")

	t.Run("header", func(t *testing.T) {
		objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
		header := http.Header{"X-Amz-Checksum-Crc32": {crc32Base64(data)}, "Content-Length": {"5"}}

		resp := serve(t, objectAPI, http.MethodPut, "/bucket/key", header, data)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Equal(t, crc32Base64(data), resp.Header.Get("X-Amz-Checksum-Crc32"))
		// The SHA-256 of the signed payload wasn't asked for.
		require.Empty(t, resp.Header.Get("X-Amz-Checksum-Sha256"))
		require.Equal(t, data, objectAPI.data)
	})

	t.Run("no checksum", func(t *testing.T) {
		objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
		resp := serve(t, objectAPI, http.MethodPut, "/bucket/key", http.Header{"Content-Length": {"5"}}, data)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		for k := range resp.Header {
			require.NotContains(t, k, "X-Amz-Checksum-")
		}
	})

	t.Run("trailer without streaming payload", func(t *testing.T) {
		objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
		header := http.Header{"X-Amz-Trailer": {"x-amz-checksum-crc32"}, "Content-Length": {"5"}}
		resp := serve(t, objectAPI, http.MethodPut, "/bucket/key", header, data)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, "<Code>InvalidRequest</Code>")
	})

	t.Run("streaming trailer without X-Amz-Trailer", func(t *testing.T) {
		objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
		body := fmt.Appendf(nil, "%x\r\n%s\r\n0\r\n\r\n", len(data), data)
		header := http.Header{
			"X-Amz-Content-Sha256":         {"STREAMING-UNSIGNED-PAYLOAD-TRAILER"},
			"Content-Encoding":             {"aws-chunked"},
			"X-Amz-Decoded-Content-Length": {strconv.Itoa(len(data))},
			"Content-Length":               {strconv.Itoa(len(body))},
		}
		resp := serve(t, objectAPI, http.MethodPut, "/bucket/key", header, body)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, "<Code>MalformedTrailerError</Code>")
	})

	t.Run("mismatch", func(t *testing.T) {
		objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
		header := http.Header{"X-Amz-Checksum-Crc32": {crc32Base64([]byte("other"))}, "Content-Length": {"5"}}

		resp := serve(t, objectAPI, http.MethodPut, "/bucket/key", header, data)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, "<Code>BadDigest</Code>")
	})

	t.Run("multiple algorithms", func(t *testing.T) {
		objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
		header := http.Header{
			"X-Amz-Checksum-Crc32":  {crc32Base64(data)},
			"X-Amz-Checksum-Crc32c": {"AAAAAA=="},
			"Content-Length":        {"5"},
		}

		resp := serve(t, objectAPI, http.MethodPut, "/bucket/key", header, data)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, "<Code>InvalidRequest</Code>")
	})

	t.Run("trailer", func(t *testing.T) {
		objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
		body := fmt.Appendf(nil, "%x\r\n%s\r\n0\r\nx-amz-checksum-crc32:%s\r\n\r\n", len(data), data, crc32Base64(data))
		header := http.Header{
			"X-Amz-Content-Sha256":         {"STREAMING-UNSIGNED-PAYLOAD-TRAILER"},
			"Content-Encoding":             {"aws-chunked"},
			"X-Amz-Decoded-Content-Length": {strconv.Itoa(len(data))},
			"X-Amz-Trailer":                {"x-amz-checksum-crc32"},
			"Content-Length":               {strconv.Itoa(len(body))},
		}

		resp := serve(t, objectAPI, http.MethodPut, "/bucket/key", header, body)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Equal(t, crc32Base64(data), resp.Header.Get("X-Amz-Checksum-Crc32"))
		require.Equal(t, data, objectAPI.data)
	})

	// Like S3, a malformed trailing checksum is an invalid request, and a wrong one a bad digest.
	t.Run("invalid trailer", func(t *testing.T) {
		for _, tt := range []struct{ name, value, code, message string }{
			{"wrong", crc32Base64([]byte("other")), "BadDigest", "The CRC32 you specified did not match the calculated checksum."},
			{"not base64", "!!!!!!!!", "InvalidRequest", "Value for x-amz-checksum-crc32 trailing header is invalid."},
			{"empty", "", "InvalidRequest", "Value for x-amz-checksum-crc32 trailing header is invalid."},
			{"another decoded size", "AAAAAAA=", "InvalidRequest", "Value for x-amz-checksum-crc32 trailing header is invalid."},
			// NhCmhh== is crc32Base64(data), NhCmhg==, with non-zero padding bits.
			{"non-zero padding bits", "NhCmhh==", "InvalidRequest", "Value for x-amz-checksum-crc32 trailing header is invalid."},
		} {
			objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
			body := fmt.Appendf(nil, "%x\r\n%s\r\n0\r\nx-amz-checksum-crc32:%s\r\n\r\n", len(data), data, tt.value)
			header := http.Header{
				"X-Amz-Content-Sha256":         {"STREAMING-UNSIGNED-PAYLOAD-TRAILER"},
				"Content-Encoding":             {"aws-chunked"},
				"X-Amz-Decoded-Content-Length": {strconv.Itoa(len(data))},
				"X-Amz-Trailer":                {"x-amz-checksum-crc32"},
				"Content-Length":               {strconv.Itoa(len(body))},
			}
			resp := serve(t, objectAPI, http.MethodPut, "/bucket/key", header, body)
			require.Equal(t, http.StatusBadRequest, resp.StatusCode, tt.name)
			require.Contains(t, resp.Body, "<Code>"+tt.code+"</Code><Message>"+tt.message+"</Message>", tt.name)
			require.Nil(t, objectAPI.data, tt.name)
		}
	})
}

func TestDeleteObjectsWithChecksum(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		deleteObjects: func(_ context.Context, _ string, objects []cmd.ObjectToDelete, _ objectlayer.ObjectOptions) ([]cmd.DeletedObject, []cmd.DeleteObjectsError, error) {
			return []cmd.DeletedObject{{ObjectName: objects[0].ObjectName}}, nil, nil
		},
	}

	body := []byte(`<Delete><Object><Key>key</Key></Object></Delete>`)
	header := http.Header{"X-Amz-Checksum-Crc32": {crc32Base64(body)}}

	resp := serve(t, objectAPI, http.MethodPost, "/bucket?delete", header, body)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Deleted><Key>key</Key>")

	resp = serve(t, objectAPI, http.MethodPost, "/bucket?delete", nil, body)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
}

func TestChecksumHeaderValidation(t *testing.T) {
	data := []byte("hello")
	trailerBody := fmt.Appendf(nil, "%x\r\n%s\r\n0\r\nx-amz-checksum-crc32:%s\r\n\r\n", len(data), data, crc32Base64(data))
	trailer := http.Header{
		"X-Amz-Content-Sha256":         {"STREAMING-UNSIGNED-PAYLOAD-TRAILER"},
		"Content-Encoding":             {"aws-chunked"},
		"X-Amz-Decoded-Content-Length": {strconv.Itoa(len(data))},
		"X-Amz-Trailer":                {"x-amz-checksum-crc32"},
	}
	with := func(h http.Header, kv ...string) http.Header {
		h = h.Clone()
		if h == nil {
			h = http.Header{}
		}
		for i := 0; i < len(kv); i += 2 {
			h.Set(kv[i], kv[i+1])
		}
		return h
	}

	const (
		invalidCRC32   = "Value for x-amz-checksum-crc32 header is invalid."
		invalidSdk     = "Value for x-amz-sdk-checksum-algorithm header is invalid."
		multiple       = "Expecting a single x-amz-checksum- header. Multiple checksum Types are not allowed."
		notImplemented = "Checksum options are not supported at this time."
	)
	for _, tt := range []struct {
		name    string
		header  http.Header
		trailer bool
		status  int
		message string
	}{
		{name: "empty value", header: with(nil, "X-Amz-Checksum-Crc32", ""), status: 400, message: invalidCRC32},
		{name: "wrong decoded length", header: with(nil, "X-Amz-Checksum-Crc32", "AAAAAAA="), status: 400, message: invalidCRC32},
		{name: "wrong encoded length", header: with(nil, "X-Amz-Checksum-Crc64nvme", crc32Base64(data)), status: 400, message: "Value for x-amz-checksum-crc64nvme header is invalid."},
		{name: "unknown algorithm", header: with(nil, "X-Amz-Checksum-Foo", "AAAAAA=="), status: 400, message: "The algorithm type you specified in x-amz-checksum- header is invalid."},
		{name: "unimplemented algorithm", header: with(nil, "X-Amz-Checksum-Sha512", "AAAA"), status: 501, message: notImplemented},
		{name: "unimplemented trailer", header: with(trailer, "X-Amz-Trailer", "x-amz-checksum-xxhash64"), trailer: true, status: 501, message: notImplemented},
		{name: "multiple algorithms", header: with(nil, "X-Amz-Checksum-Crc32", crc32Base64(data), "X-Amz-Checksum-Crc32c", "AAAAAA=="), status: 400, message: multiple},
		{name: "header and trailer", header: with(trailer, "X-Amz-Checksum-Crc32c", "AAAAAA=="), trailer: true, status: 400, message: multiple},
		{name: "sdk algorithm without checksum", header: with(nil, "X-Amz-Sdk-Checksum-Algorithm", "CRC32"), status: 400, message: "x-amz-sdk-checksum-algorithm specified, but no corresponding x-amz-checksum-* or x-amz-trailer headers were found."},
		{name: "sdk algorithm mismatches header", header: with(nil, "X-Amz-Sdk-Checksum-Algorithm", "SHA1", "X-Amz-Checksum-Crc32", crc32Base64(data)), status: 400, message: invalidSdk},
		{name: "sdk algorithm mismatches trailer", header: with(trailer, "X-Amz-Sdk-Checksum-Algorithm", "CRC32C"), trailer: true, status: 400, message: invalidSdk},
		{name: "unknown sdk algorithm", header: with(nil, "X-Amz-Sdk-Checksum-Algorithm", "FOO", "X-Amz-Checksum-Crc32", crc32Base64(data)), status: 400, message: invalidSdk},
		{name: "unimplemented sdk algorithm", header: with(nil, "X-Amz-Sdk-Checksum-Algorithm", "SHA512", "X-Amz-Checksum-Crc32", crc32Base64(data)), status: 501, message: notImplemented},
		{name: "sdk algorithm matches header", header: with(nil, "X-Amz-Sdk-Checksum-Algorithm", "CRC32", "X-Amz-Checksum-Crc32", crc32Base64(data)), status: 200},
		{name: "sdk algorithm matches trailer", header: with(trailer, "X-Amz-Sdk-Checksum-Algorithm", "crc32"), trailer: true, status: 200},
		{name: "checksum type alone", header: with(nil, "X-Amz-Checksum-Type", "FULL_OBJECT"), status: 200},
	} {
		t.Run(tt.name, func(t *testing.T) {
			objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
			body := data
			if tt.trailer {
				body = trailerBody
			}
			resp := serveHTTP(t, objectAPI, http.MethodPut, "/bucket/key", tt.header, body)
			require.Equal(t, tt.status, resp.StatusCode, resp.Body)
			if tt.status != http.StatusOK {
				require.Contains(t, resp.Body, "<Message>"+tt.message+"</Message>")
				require.Nil(t, objectAPI.data)
				return
			}
			require.Equal(t, data, objectAPI.data)
			if tt.header.Get("X-Amz-Checksum-Crc32") != "" || tt.trailer {
				require.Equal(t, crc32Base64(data), resp.Header.Get("X-Amz-Checksum-Crc32"))
				require.Equal(t, "FULL_OBJECT", resp.Header.Get("X-Amz-Checksum-Type"))
			} else {
				require.Empty(t, resp.Header.Get("X-Amz-Checksum-Type"))
			}
		})
	}
}

func TestCompleteMultipartUploadChecksumHeaders(t *testing.T) {
	body := []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>etag</ETag></Part></CompleteMultipartUpload>`)
	for _, tt := range []struct {
		name   string
		header http.Header
		status int
		code   string
	}{
		// The completed-object checksum isn't verified yet, so even a composite value passes.
		{name: "object checksum deferred", header: http.Header{"X-Amz-Checksum-Crc32": {"AAAAAA==-2"}}, status: 200},
		{name: "unknown algorithm", header: http.Header{"X-Amz-Checksum-Foo": {"AAAAAA=="}}, status: 400, code: "InvalidRequest"},
		{name: "unimplemented algorithm", header: http.Header{"X-Amz-Checksum-Sha512": {"AAAA"}}, status: 501, code: "NotImplemented"},
		{name: "XML still verified", header: http.Header{"X-Amz-Checksum-Crc32": {"AAAAAA=="}, "Content-Md5": {"AAAAAAAAAAAAAAAAAAAAAA=="}}, status: 400, code: "BadDigest"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			layer := &fakeObjectLayer{completeMultipartUpload: func(context.Context, string, string, string, []objectlayer.CompletePart, objectlayer.ObjectOptions) (objectlayer.ObjectInfo, error) {
				called = true
				return objectlayer.ObjectInfo{ETag: "etag"}, nil
			}}
			resp := serveHTTP(t, layer, http.MethodPost, "/bucket/key?uploadId=id", tt.header, body)
			require.Equal(t, tt.status, resp.StatusCode, resp.Body)
			require.Equal(t, tt.code == "", called)
			if tt.code != "" {
				require.Contains(t, resp.Body, "<Code>"+tt.code+"</Code>")
			}
		})
	}
}

func TestDeleteObjectsChecksumMismatch(t *testing.T) {
	for _, padding := range []string{"", strings.Repeat(" ", 8192)} {
		t.Run(fmt.Sprintf("padding=%d", len(padding)), func(t *testing.T) {
			body := []byte(`<Delete><Object><Key>key</Key></Object></Delete>` + padding)
			for _, valid := range []bool{false, true} {
				called := false
				layer := &fakeObjectLayer{deleteObjects: func(context.Context, string, []cmd.ObjectToDelete, objectlayer.ObjectOptions) ([]cmd.DeletedObject, []cmd.DeleteObjectsError, error) {
					called = true
					return nil, nil, nil
				}}
				checksum := crc32Base64([]byte("other"))
				if valid {
					checksum = crc32Base64(body)
				}
				resp := serve(t, layer, http.MethodPost, "/bucket?delete", http.Header{"X-Amz-Checksum-Crc32": {checksum}}, body)
				if valid {
					require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
					require.True(t, called)
				} else {
					require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
					require.Contains(t, resp.Body, "<Code>BadDigest</Code>")
					require.False(t, called)
				}
			}
		})
	}
}

func TestPutObjectChecksumStrict(t *testing.T) {
	data := []byte("hello")
	objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}

	// The canonical encoding of the CRC32 is NhCmhg==; other padding bits are rejected.
	resp := serve(t, objectAPI, http.MethodPut, "/bucket/key", http.Header{"X-Amz-Checksum-Crc32": {"NhCmhv=="}, "Content-Length": {"5"}}, data)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>InvalidRequest</Code>")

	resp = serve(t, objectAPI, http.MethodPut, "/bucket/key", http.Header{
		"X-Amz-Checksum-Crc32": {crc32Base64(data)},
		"X-Amz-Checksum-Type":  {"COMPOSITE"},
		"Content-Length":       {"5"},
	}, data)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>InvalidRequest</Code>")

	resp = serve(t, objectAPI, http.MethodPut, "/bucket/key", http.Header{
		"X-Amz-Checksum-Crc32": {crc32Base64(data)},
		"X-Amz-Checksum-Type":  {"full_object"},
		"Content-Length":       {"5"},
	}, data)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, "FULL_OBJECT", resp.Header.Get("X-Amz-Checksum-Type"))
}

// readingObjectLayer reads the data of PutObject and PutObjectPart, which is when its integrity is
// verified.
type readingObjectLayer struct {
	*fakeObjectLayer
	stored bool
}

func (l *readingObjectLayer) PutObject(_ context.Context, bucket, object string, data *cmd.PutObjReader, _ objectlayer.ObjectOptions) (objectlayer.ObjectInfo, error) {
	if _, err := io.Copy(io.Discard, data); err != nil {
		return objectlayer.ObjectInfo{}, err
	}
	l.stored = true
	return objectlayer.ObjectInfo{Bucket: bucket, Name: object, ETag: "etag"}, nil
}

func (l *readingObjectLayer) PutObjectPart(_ context.Context, _, _, _ string, partID int, data *cmd.PutObjReader, _ objectlayer.ObjectOptions) (objectlayer.PartInfo, error) {
	if _, err := io.Copy(io.Discard, data); err != nil {
		return objectlayer.PartInfo{}, err
	}
	l.stored = true
	return objectlayer.PartInfo{PartNumber: partID, ETag: "etag"}, nil
}

func TestPutObjectIntegrityErrorPriority(t *testing.T) {
	data, other := []byte("hello"), []byte("other")
	const (
		md5Error    = "<Code>BadDigest</Code><Message>The Content-MD5 you specified did not match what we received.</Message>"
		sha256Error = "<Code>XAmzContentSHA256Mismatch</Code>"
		crc32Error  = "<Code>BadDigest</Code><Message>The CRC32 you specified did not match the calculated checksum.</Message>"
	)
	// The errors S3 returned for each combination of mismatching Content-MD5, X-Amz-Content-Sha256
	// and X-Amz-Checksum-Crc32.
	for _, tt := range []struct {
		name                   string
		badMD5, badSHA, badCRC bool
		want                   string
	}{
		{"MD5", true, false, false, md5Error},
		{"SHA256", false, true, false, sha256Error},
		{"CRC32", false, false, true, crc32Error},
		{"MD5 and SHA256", true, true, false, sha256Error},
		{"MD5 and CRC32", true, false, true, md5Error},
		{"SHA256 and CRC32", false, true, true, sha256Error},
		{"all", true, true, true, sha256Error},
	} {
		pick := func(bad bool) []byte {
			if bad {
				return other
			}
			return data
		}
		header := http.Header{
			"Content-Length":       {strconv.Itoa(len(data))},
			"Content-Md5":          {base64MD5(pick(tt.badMD5))},
			"X-Amz-Content-Sha256": {hexSHA256(pick(tt.badSHA))},
			"X-Amz-Checksum-Crc32": {crc32Base64(pick(tt.badCRC))},
		}
		for _, target := range []string{"/bucket/key", "/bucket/key?partNumber=1&uploadId=id"} {
			t.Run(tt.name+" "+target, func(t *testing.T) {
				objectAPI := &readingObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
				resp := serve(t, objectAPI, http.MethodPut, target, header, data)
				require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
				require.Contains(t, resp.Body, tt.want)
				require.False(t, objectAPI.stored)
			})
		}
	}

	// All three match.
	header := http.Header{
		"Content-Length":       {strconv.Itoa(len(data))},
		"Content-Md5":          {base64MD5(data)},
		"X-Amz-Checksum-Crc32": {crc32Base64(data)},
	}
	for _, target := range []string{"/bucket/key", "/bucket/key?partNumber=1&uploadId=id"} {
		objectAPI := &readingObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
		resp := serve(t, objectAPI, http.MethodPut, target, header, data)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.True(t, objectAPI.stored)
	}
}

// TestXMLBodyChecksumPriorityLikeS3 checks that S3 reports Content-MD5 before X-Amz-Content-Sha256
// for operations with an XML body, but not for PutObject.
func TestXMLBodyChecksumPriorityLikeS3(t *testing.T) {
	sha256Hex := func(b []byte) string {
		sum := sha256.Sum256(b)
		return hex.EncodeToString(sum[:])
	}
	other := []byte("other")

	for _, tt := range []struct{ name, method, target, body string }{
		{"PutBucketTagging", http.MethodPut, "/bucket?tagging", `<Tagging><TagSet></TagSet></Tagging>`},
		{"PutObjectTagging", http.MethodPut, "/bucket/key?tagging", `<Tagging><TagSet></TagSet></Tagging>`},
		{"PutBucketVersioning", http.MethodPut, "/bucket?versioning", `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`},
		{"DeleteObjects", http.MethodPost, "/bucket?delete", `<Delete><Object><Key>key</Key></Object></Delete>`},
		{"CompleteMultipartUpload", http.MethodPost, "/bucket/key?uploadId=id", `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>a</ETag></Part></CompleteMultipartUpload>`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte(tt.body)
			md5Of := func(b []byte) string {
				sum := md5.Sum(b)
				return base64.StdEncoding.EncodeToString(sum[:])
			}
			for _, c := range []struct {
				name           string
				badMD5, badSHA bool
				code           string
			}{
				{"bad MD5 and bad SHA-256", true, true, "BadDigest"},
				{"bad MD5", true, false, "BadDigest"},
				{"bad SHA-256", false, true, "XAmzContentSHA256Mismatch"},
			} {
				md5Body, shaBody := body, body
				if c.badMD5 {
					md5Body = other
				}
				if c.badSHA {
					shaBody = other
				}
				resp := serveHTTP(t, &fakeObjectLayer{}, tt.method, tt.target, http.Header{
					"Content-Md5":          {md5Of(md5Body)},
					"X-Amz-Content-Sha256": {sha256Hex(shaBody)},
				}, body)
				require.Equal(t, http.StatusBadRequest, resp.StatusCode, c.name+": "+resp.Body)
				require.Contains(t, resp.Body, "<Code>"+c.code+"</Code>", c.name)
			}
		})
	}

	// PutObject reports X-Amz-Content-Sha256 first.
	sum := md5.Sum(other)
	resp := serveHTTP(t, &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}, http.MethodPut, "/bucket/key", http.Header{
		"Content-Md5":          {base64.StdEncoding.EncodeToString(sum[:])},
		"X-Amz-Content-Sha256": {sha256Hex(other)},
		"Content-Length":       {"5"},
	}, []byte("hello"))
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>XAmzContentSHA256Mismatch</Code>")
}

// TestObjectChecksums checks the answers of S3 to puts and reads of objects with checksums.
func TestObjectChecksums(t *testing.T) {
	objectAPI := fsobjects.New(t.TempDir())
	require.Equal(t, http.StatusOK, serve(t, objectAPI, http.MethodPut, "/bucket", nil, nil).StatusCode)

	data := []byte("hello world")
	const crc32Value = "DUoRhQ=="
	put := serve(t, objectAPI, http.MethodPut, "/bucket/crc32", http.Header{"X-Amz-Checksum-Crc32": {crc32Value}}, data)
	require.Equal(t, http.StatusOK, put.StatusCode, put.Body)
	require.Equal(t, crc32Value, put.Header.Get("X-Amz-Checksum-Crc32"))
	require.Equal(t, "FULL_OBJECT", put.Header.Get("X-Amz-Checksum-Type"))
	// Like S3, fsobjects stores a CRC64NVME checksum with an object put without one.
	const crc64nvmeValue = "jSnVw/bqjr4="
	put = serve(t, objectAPI, http.MethodPut, "/bucket/none", nil, data)
	require.Equal(t, http.StatusOK, put.StatusCode, put.Body)
	require.Equal(t, crc64nvmeValue, put.Header.Get("X-Amz-Checksum-Crc64nvme"))
	require.Equal(t, "FULL_OBJECT", put.Header.Get("X-Amz-Checksum-Type"))
	head := serve(t, objectAPI, http.MethodHead, "/bucket/none", http.Header{"X-Amz-Checksum-Mode": {"ENABLED"}}, nil)
	require.Equal(t, http.StatusOK, head.StatusCode)
	require.Equal(t, crc64nvmeValue, head.Header.Get("X-Amz-Checksum-Crc64nvme"))

	enabled := http.Header{"X-Amz-Checksum-Mode": {"ENABLED"}}
	with := func(h http.Header, kv ...string) http.Header {
		h = h.Clone()
		for i := 0; i < len(kv); i += 2 {
			h.Set(kv[i], kv[i+1])
		}
		return h
	}
	for _, tc := range []struct {
		name     string
		method   string
		target   string
		header   http.Header
		status   int
		checksum bool
	}{
		{"HEAD without mode", http.MethodHead, "/bucket/crc32", nil, http.StatusOK, false},
		{"HEAD mode ENABLED", http.MethodHead, "/bucket/crc32", enabled, http.StatusOK, true},
		{"HEAD mode lowercase", http.MethodHead, "/bucket/crc32", http.Header{"X-Amz-Checksum-Mode": {"enabled"}}, http.StatusOK, true},
		{"HEAD mode DISABLED", http.MethodHead, "/bucket/crc32", http.Header{"X-Amz-Checksum-Mode": {"DISABLED"}}, http.StatusBadRequest, false},
		{"HEAD mode empty", http.MethodHead, "/bucket/crc32", http.Header{"X-Amz-Checksum-Mode": {""}}, http.StatusBadRequest, false},
		{"HEAD mode DISABLED missing key", http.MethodHead, "/bucket/missing", http.Header{"X-Amz-Checksum-Mode": {"DISABLED"}}, http.StatusNotFound, false},
		{"HEAD mode DISABLED If-None-Match match", http.MethodHead, "/bucket/crc32", http.Header{"X-Amz-Checksum-Mode": {"DISABLED"}, "If-None-Match": {`"5eb63bbbe01eeed093cb22bb8f5acdc3"`}}, http.StatusBadRequest, false},
		{"HEAD range from 1", http.MethodHead, "/bucket/crc32", with(enabled, "Range", "bytes=1-"), http.StatusPartialContent, false},
		{"HEAD range exact length", http.MethodHead, "/bucket/crc32", with(enabled, "Range", "bytes=0-10"), http.StatusPartialContent, true},
		{"HEAD partNumber=1", http.MethodHead, "/bucket/crc32?partNumber=1", enabled, http.StatusPartialContent, true},
		{"GET without mode", http.MethodGet, "/bucket/crc32", nil, http.StatusOK, false},
		{"GET mode ENABLED", http.MethodGet, "/bucket/crc32", enabled, http.StatusOK, true},
		{"GET mode with spaces", http.MethodGet, "/bucket/crc32", http.Header{"X-Amz-Checksum-Mode": {" ENABLED "}}, http.StatusOK, true},
		{"GET mode two values", http.MethodGet, "/bucket/crc32", http.Header{"X-Amz-Checksum-Mode": {"ENABLED,ENABLED"}}, http.StatusBadRequest, false},
		{"GET mode DISABLED missing key", http.MethodGet, "/bucket/missing", http.Header{"X-Amz-Checksum-Mode": {"DISABLED"}}, http.StatusBadRequest, false},
		{"GET mode DISABLED versionId=bad", http.MethodGet, "/bucket/crc32?versionId=bad", http.Header{"X-Amz-Checksum-Mode": {"DISABLED"}}, http.StatusBadRequest, false},
		{"GET range from 1", http.MethodGet, "/bucket/crc32", with(enabled, "Range", "bytes=1-"), http.StatusPartialContent, false},
		{"GET range 0-1", http.MethodGet, "/bucket/crc32", with(enabled, "Range", "bytes=0-1"), http.StatusPartialContent, false},
		{"GET range whole", http.MethodGet, "/bucket/crc32", with(enabled, "Range", "bytes=0-"), http.StatusPartialContent, true},
		{"GET range past the end", http.MethodGet, "/bucket/crc32", with(enabled, "Range", "bytes=0-100"), http.StatusPartialContent, true},
		{"GET range suffix longer", http.MethodGet, "/bucket/crc32", with(enabled, "Range", "bytes=-100"), http.StatusPartialContent, true},
		{"GET range invalid syntax", http.MethodGet, "/bucket/crc32", with(enabled, "Range", "bytes=x-"), http.StatusOK, true},
		{"GET partNumber=1", http.MethodGet, "/bucket/crc32?partNumber=1", enabled, http.StatusPartialContent, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := serve(t, objectAPI, tc.method, tc.target, tc.header, nil)
			require.Equal(t, tc.status, resp.StatusCode, resp.Body)
			if tc.status == http.StatusBadRequest && tc.method == http.MethodGet {
				require.Contains(t, resp.Body, "<Message>Value for x-amz-checksum-mode header is invalid.</Message>")
			}
			if tc.checksum {
				require.Equal(t, crc32Value, resp.Header.Get("X-Amz-Checksum-Crc32"))
				require.Equal(t, "FULL_OBJECT", resp.Header.Get("X-Amz-Checksum-Type"))
			} else {
				require.Empty(t, resp.Header.Get("X-Amz-Checksum-Crc32"))
				require.Empty(t, resp.Header.Get("X-Amz-Checksum-Type"))
			}
		})
	}

	t.Run("GetObjectAttributes", func(t *testing.T) {
		resp := serve(t, objectAPI, http.MethodGet, "/bucket/crc32?attributes", http.Header{"X-Amz-Object-Attributes": {"StorageClass,ObjectSize,Checksum,ETag"}}, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, `<ETag>5eb63bbbe01eeed093cb22bb8f5acdc3</ETag><Checksum><ChecksumCRC32>DUoRhQ==</ChecksumCRC32><ChecksumType>FULL_OBJECT</ChecksumType></Checksum><StorageClass>STANDARD</StorageClass><ObjectSize>11</ObjectSize>`)

		// The mode isn't checked.
		resp = serve(t, objectAPI, http.MethodGet, "/bucket/crc32?attributes", http.Header{"X-Amz-Object-Attributes": {"Checksum"}, "X-Amz-Checksum-Mode": {"DISABLED"}}, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, `<Checksum><ChecksumCRC32>DUoRhQ==</ChecksumCRC32>`)

		resp = serve(t, objectAPI, http.MethodGet, "/bucket/none?attributes", http.Header{"X-Amz-Object-Attributes": {"Checksum"}}, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, `<Checksum><ChecksumCRC64NVME>jSnVw/bqjr4=</ChecksumCRC64NVME><ChecksumType>FULL_OBJECT</ChecksumType></Checksum>`)
	})
}

// TestListChecksums checks that, like S3, listings report the algorithm and type of each
// object's checksum, but not of delete markers.
func TestListChecksums(t *testing.T) {
	objectAPI := fsobjects.New(t.TempDir())
	require.Equal(t, http.StatusOK, serve(t, objectAPI, http.MethodPut, "/bucket", nil, nil).StatusCode)
	versioning := []byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`)
	require.Equal(t, http.StatusOK, serve(t, objectAPI, http.MethodPut, "/bucket?versioning", http.Header{"Content-Md5": {base64MD5(versioning)}}, versioning).StatusCode)

	data := []byte("hello world")
	require.Equal(t, http.StatusOK, serve(t, objectAPI, http.MethodPut, "/bucket/crc32", http.Header{"X-Amz-Checksum-Crc32": {"DUoRhQ=="}}, data).StatusCode)
	require.Equal(t, http.StatusOK, serve(t, objectAPI, http.MethodPut, "/bucket/deleted", nil, data).StatusCode)
	require.Equal(t, http.StatusNoContent, serve(t, objectAPI, http.MethodDelete, "/bucket/deleted", nil, nil).StatusCode)

	const crc32Elements = `<ETag>&#34;5eb63bbbe01eeed093cb22bb8f5acdc3&#34;</ETag><ChecksumAlgorithm>CRC32</ChecksumAlgorithm><ChecksumType>FULL_OBJECT</ChecksumType><Size>11</Size>`
	for _, target := range []string{"/bucket", "/bucket?list-type=2", "/bucket?versions"} {
		resp := serve(t, objectAPI, http.MethodGet, target, nil, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, crc32Elements, target)
	}

	// Of the two versions and the delete marker, only the versions have checksums.
	resp := serve(t, objectAPI, http.MethodGet, "/bucket?versions", nil, nil)
	require.Equal(t, 2, strings.Count(resp.Body, "<ChecksumAlgorithm>"))
}

// TestCopyObjectChecksums checks that, like S3, a copy keeps its source's checksum and
// returns it, and how the requested checksum algorithm is checked. Unlike S3, which computes a
// checksum with another algorithm, the gateway refuses one, and it lists only the algorithms it
// supports.
func TestCopyObjectChecksums(t *testing.T) {
	objectAPI := fsobjects.New(t.TempDir())
	require.Equal(t, http.StatusOK, serve(t, objectAPI, http.MethodPut, "/bucket", nil, nil).StatusCode)
	data := []byte("hello world")
	require.Equal(t, http.StatusOK, serve(t, objectAPI, http.MethodPut, "/bucket/crc32", http.Header{"X-Amz-Checksum-Crc32": {"DUoRhQ=="}}, data).StatusCode)
	require.Equal(t, http.StatusOK, serve(t, objectAPI, http.MethodPut, "/bucket/none", nil, data).StatusCode)
	resp := serve(t, objectAPI, http.MethodPost, "/bucket/composite?uploads", http.Header{"X-Amz-Checksum-Algorithm": {"CRC32"}}, nil)
	uploadID, _, _ := strings.Cut(resp.Body[strings.Index(resp.Body, "<UploadId>")+len("<UploadId>"):], "<")
	resp = serve(t, objectAPI, http.MethodPut, "/bucket/composite?partNumber=1&uploadId="+uploadID, http.Header{"X-Amz-Checksum-Crc32": {"DUoRhQ=="}}, data)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	complete := fmt.Sprintf("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag><ChecksumCRC32>DUoRhQ==</ChecksumCRC32></Part></CompleteMultipartUpload>", resp.Header.Get("ETag"))
	resp = serve(t, objectAPI, http.MethodPost, "/bucket/composite?uploadId="+uploadID, nil, []byte(complete))
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)

	const (
		crc32Result     = `<ETag>&#34;5eb63bbbe01eeed093cb22bb8f5acdc3&#34;</ETag><ChecksumCRC32>DUoRhQ==</ChecksumCRC32><ChecksumType>FULL_OBJECT</ChecksumType></CopyObjectResult>`
		crc64nvmeResult = `<ChecksumCRC64NVME>jSnVw/bqjr4=</ChecksumCRC64NVME><ChecksumType>FULL_OBJECT</ChecksumType></CopyObjectResult>`
	)
	for _, tc := range []struct {
		name, dst, src string
		header         http.Header
		status         int
		body           string
	}{
		{"crc32", "copy", "crc32", nil, http.StatusOK, crc32Result},
		{"default", "copy-none", "none", nil, http.StatusOK, crc64nvmeResult},
		{"same algorithm", "copy-same", "crc32", http.Header{"X-Amz-Checksum-Algorithm": {"CRC32"}}, http.StatusOK, crc32Result},
		{"algorithm with spaces", "copy-spaces", "crc32", http.Header{"X-Amz-Checksum-Algorithm": {" crc32 "}}, http.StatusOK, crc32Result},
		{"empty algorithm", "copy-empty", "crc32", http.Header{"X-Amz-Checksum-Algorithm": {""}}, http.StatusOK, crc32Result},
		{"checksum value header", "copy-value", "crc32", http.Header{"X-Amz-Checksum-Crc32": {"DUoRhQ=="}}, http.StatusOK, crc32Result},
		{"onto itself", "crc32", "crc32", http.Header{"X-Amz-Metadata-Directive": {"REPLACE"}}, http.StatusOK, crc32Result},
		{"invalid algorithm", "copy-invalid", "crc32", http.Header{"X-Amz-Checksum-Algorithm": {"MD4"}}, http.StatusBadRequest, "Checksum algorithm provided is unsupported. Please try again with any of the valid types: [CRC32, CRC32C, CRC64NVME, SHA1, SHA256]"},
		{"invalid algorithm missing source", "copy-invalid", "missing", http.Header{"X-Amz-Checksum-Algorithm": {"MD4"}}, http.StatusBadRequest, "Checksum algorithm provided is unsupported."},
		{"invalid algorithm invalid metadata directive", "copy-invalid", "crc32", http.Header{"X-Amz-Checksum-Algorithm": {"MD4"}, "X-Amz-Metadata-Directive": {"BAD"}}, http.StatusBadRequest, "Checksum algorithm provided is unsupported."},
		{"unsupported algorithm", "copy-sha512", "crc32", http.Header{"X-Amz-Checksum-Algorithm": {"SHA512"}}, http.StatusNotImplemented, "<Code>NotImplemented</Code>"},
		{"different algorithm", "copy-sha256", "crc32", http.Header{"X-Amz-Checksum-Algorithm": {"SHA256"}}, http.StatusNotImplemented, "Specifying a checksum algorithm for the destination object that differs from that of the source object is not supported."},
		{"different algorithm onto itself", "crc32", "crc32", http.Header{"X-Amz-Checksum-Algorithm": {"SHA1"}, "X-Amz-Metadata-Directive": {"REPLACE"}}, http.StatusNotImplemented, "Specifying a checksum algorithm for the destination object that differs from that of the source object is not supported."},
		// Unlike S3, which gives the copy a full object checksum.
		{"composite", "copy-composite", "composite", nil, http.StatusOK, "<ChecksumCRC32>YChbaA==-1</ChecksumCRC32><ChecksumType>COMPOSITE</ChecksumType></CopyObjectResult>"},
		{"composite same algorithm", "copy-composite-crc32", "composite", http.Header{"X-Amz-Checksum-Algorithm": {"CRC32"}}, http.StatusNotImplemented, "of a source object with a composite checksum is not supported."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			header := http.Header{"X-Amz-Copy-Source": {"/bucket/" + tc.src}}
			maps.Copy(header, tc.header)
			resp := serve(t, objectAPI, http.MethodPut, "/bucket/"+tc.dst, header, nil)
			require.Equal(t, tc.status, resp.StatusCode, resp.Body)
			require.Contains(t, resp.Body, tc.body)
		})
	}

	head := serve(t, objectAPI, http.MethodHead, "/bucket/copy", http.Header{"X-Amz-Checksum-Mode": {"ENABLED"}}, nil)
	require.Equal(t, http.StatusOK, head.StatusCode)
	require.Equal(t, "DUoRhQ==", head.Header.Get("X-Amz-Checksum-Crc32"))
}

// TestMultipartUploadChecksums checks the answers of S3 to the requests of multipart uploads with
// checksums. Unlike S3, an upload requested without an algorithm reports its default full object
// CRC64NVME checksums before it's completed.
func TestMultipartUploadChecksums(t *testing.T) {
	objectAPI := fsobjects.New(t.TempDir())
	require.Equal(t, http.StatusOK, serve(t, objectAPI, http.MethodPut, "/bucket", nil, nil).StatusCode)
	data := []byte("hello world")
	const crc32Value = "DUoRhQ=="

	create := func(t *testing.T, key string, header http.Header) (response, string) {
		resp := serve(t, objectAPI, http.MethodPost, "/bucket/"+key+"?uploads", header, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		uploadID, _, _ := strings.Cut(resp.Body[strings.Index(resp.Body, "<UploadId>")+len("<UploadId>"):], "<")
		return resp, uploadID
	}
	putPart := func(key, uploadID string, header http.Header) response {
		return serve(t, objectAPI, http.MethodPut, fmt.Sprintf("/bucket/%s?partNumber=1&uploadId=%s", key, uploadID), header, data)
	}
	complete := func(key, uploadID, partChecksum string, header http.Header) response {
		body := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"5eb63bbbe01eeed093cb22bb8f5acdc3"</ETag>` + partChecksum + `</Part></CompleteMultipartUpload>`
		return serve(t, objectAPI, http.MethodPost, fmt.Sprintf("/bucket/%s?uploadId=%s", key, uploadID), header, []byte(body))
	}

	t.Run("composite", func(t *testing.T) {
		resp, uploadID := create(t, "composite", http.Header{"X-Amz-Checksum-Algorithm": {"crc32"}})
		require.Equal(t, "CRC32", resp.Header.Get("X-Amz-Checksum-Algorithm"))
		require.Equal(t, "COMPOSITE", resp.Header.Get("X-Amz-Checksum-Type"))

		resp = putPart("composite", uploadID, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		require.Contains(t, resp.Body, "<Message>Checksum Type mismatch occurred, expected checksum Type: crc32, actual checksum Type: null</Message>")
		resp = putPart("composite", uploadID, http.Header{"X-Amz-Checksum-Crc32": {"yZRlqg=="}})
		require.Contains(t, resp.Body, "<Code>BadDigest</Code><Message>The CRC32 you specified did not match the calculated checksum.</Message>")
		// Like S3, a checksum with non-zero padding bits is malformed, not wrong.
		resp = putPart("composite", uploadID, http.Header{"X-Amz-Checksum-Crc32": {"DUoRhR=="}})
		require.Contains(t, resp.Body, "<Code>InvalidRequest</Code><Message>Value for x-amz-checksum-crc32 header is invalid.</Message>")
		resp = putPart("composite", uploadID, http.Header{"X-Amz-Checksum-Crc32": {crc32Value}})
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Equal(t, crc32Value, resp.Header.Get("X-Amz-Checksum-Crc32"))
		require.Empty(t, resp.Header.Get("X-Amz-Checksum-Type"))

		resp = serve(t, objectAPI, http.MethodGet, "/bucket/composite?uploadId="+uploadID, nil, nil)
		require.Contains(t, resp.Body, "<StorageClass>STANDARD</StorageClass><ChecksumAlgorithm>CRC32</ChecksumAlgorithm><ChecksumType>COMPOSITE</ChecksumType><PartNumberMarker>")
		require.Contains(t, resp.Body, "<Size>11</Size><ChecksumCRC32>"+crc32Value+"</ChecksumCRC32></Part>")
		resp = serve(t, objectAPI, http.MethodGet, "/bucket?uploads&prefix=composite", nil, nil)
		require.Regexp(t, `</Initiated><ChecksumAlgorithm>CRC32</ChecksumAlgorithm><ChecksumType>COMPOSITE</ChecksumType></Upload>`, resp.Body)

		resp = complete("composite", uploadID, "", nil)
		require.Contains(t, resp.Body, "<Message>The upload was created using a crc32 checksum. The complete request must include the checksum for each part. It was missing for part 1 in the request.</Message>")
		resp = complete("composite", uploadID, "<ChecksumSHA512>"+base64.StdEncoding.EncodeToString(make([]byte, 64))+"</ChecksumSHA512>", nil)
		require.Contains(t, resp.Body, "<Code>BadDigest</Code><Message>The sha512 you specified for part 1 did not match what we received.</Message>")
		resp = complete("composite", uploadID, "<ChecksumCRC32>"+crc32Value+"</ChecksumCRC32>", http.Header{"X-Amz-Checksum-Type": {"BAD"}})
		require.Contains(t, resp.Body, "<Message>Value for x-amz-checksum-type header is invalid.</Message>")
		resp = complete("composite", uploadID, "<ChecksumCRC32>"+crc32Value+"</ChecksumCRC32>", http.Header{"X-Amz-Checksum-Type": {"full_object"}})
		require.Contains(t, resp.Body, "<Message>The upload was created using the COMPOSITE checksum mode. The complete request must use the same checksum mode.</Message>")

		for _, value := range []string{"YChbaA==-0", "YChbaA==-"} {
			resp = complete("composite", uploadID, "<ChecksumCRC32>"+crc32Value+"</ChecksumCRC32>", http.Header{"X-Amz-Checksum-Crc32": {value}})
			require.Contains(t, resp.Body, "<Message>Value for x-amz-checksum-crc32 header is invalid.</Message>", value)
		}
		resp = complete("composite", uploadID, "<ChecksumCRC32>"+crc32Value+"</ChecksumCRC32>", http.Header{"X-Amz-Checksum-Crc32": {"YChbaA==-2"}})
		require.Contains(t, resp.Body, "<Code>BadDigest</Code>")

		// Like S3, the composite checksum may leave out the number of parts, as minio-go does.
		resp = complete("composite", uploadID, "<ChecksumCRC32>"+crc32Value+"</ChecksumCRC32>", http.Header{"X-Amz-Checksum-Type": {"composite"}, "X-Amz-Checksum-Crc32": {"YChbaA=="}})
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, "</ETag><ChecksumCRC32>YChbaA==-1</ChecksumCRC32><ChecksumType>COMPOSITE</ChecksumType></CompleteMultipartUploadResult>")

		head := serve(t, objectAPI, http.MethodHead, "/bucket/composite", http.Header{"X-Amz-Checksum-Mode": {"ENABLED"}}, nil)
		require.Equal(t, "YChbaA==-1", head.Header.Get("X-Amz-Checksum-Crc32"))
		require.Equal(t, "COMPOSITE", head.Header.Get("X-Amz-Checksum-Type"))
		resp = serve(t, objectAPI, http.MethodGet, "/bucket/composite?attributes", http.Header{"X-Amz-Object-Attributes": {"Checksum"}}, nil)
		require.Contains(t, resp.Body, "<Checksum><ChecksumCRC32>YChbaA==</ChecksumCRC32><ChecksumType>COMPOSITE</ChecksumType></Checksum>")
	})

	t.Run("object layer without checksums", func(t *testing.T) {
		withoutChecksums := &fakeObjectLayer{
			newMultipartUpload: func(context.Context, string, string, objectlayer.ObjectOptions) (objectlayer.NewMultipartUploadResult, error) {
				return objectlayer.NewMultipartUploadResult{UploadID: "id"}, nil
			},
		}
		resp := serve(t, withoutChecksums, http.MethodPost, "/bucket/key?uploads", http.Header{"X-Amz-Checksum-Algorithm": {"CRC32"}}, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Empty(t, resp.Header.Get("X-Amz-Checksum-Algorithm"))
		require.Empty(t, resp.Header.Get("X-Amz-Checksum-Type"))
	})

	t.Run("full object", func(t *testing.T) {
		resp, uploadID := create(t, "full", http.Header{"X-Amz-Checksum-Algorithm": {"CRC32"}, "X-Amz-Checksum-Type": {"FULL_OBJECT"}})
		require.Equal(t, "FULL_OBJECT", resp.Header.Get("X-Amz-Checksum-Type"))

		// The part's checksum is computed and returned without being requested.
		resp = putPart("full", uploadID, http.Header{"X-Amz-Checksum-Crc32c": {"yZRlqg=="}})
		require.Contains(t, resp.Body, "<Message>Checksum Type mismatch occurred, expected checksum Type: crc32, actual checksum Type: crc32c</Message>")
		resp = putPart("full", uploadID, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Equal(t, crc32Value, resp.Header.Get("X-Amz-Checksum-Crc32"))

		// Like S3, which refuses even the right whole object checksum without the type.
		resp = complete("full", uploadID, "", http.Header{"X-Amz-Checksum-Crc32": {crc32Value}})
		require.Contains(t, resp.Body, "<Code>BadDigest</Code>")
		resp = complete("full", uploadID, "", http.Header{"X-Amz-Checksum-Type": {"FULL_OBJECT"}, "X-Amz-Checksum-Crc32": {"yZRlqg=="}})
		require.Contains(t, resp.Body, "<Code>BadDigest</Code><Message>The crc32 you specified did not match the calculated checksum.</Message>")
		resp = complete("full", uploadID, "", http.Header{"X-Amz-Checksum-Type": {"FULL_OBJECT"}, "X-Amz-Checksum-Crc32": {crc32Value}})
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, "<ChecksumCRC32>"+crc32Value+"</ChecksumCRC32><ChecksumType>FULL_OBJECT</ChecksumType>")
	})

	t.Run("copy part", func(t *testing.T) {
		require.Equal(t, http.StatusOK, serve(t, objectAPI, http.MethodPut, "/bucket/source", nil, data).StatusCode)
		_, uploadID := create(t, "copy", http.Header{"X-Amz-Checksum-Algorithm": {"CRC32"}})

		// The part gets a checksum with the upload's algorithm, which isn't its source's.
		resp := serve(t, objectAPI, http.MethodPut, "/bucket/copy?partNumber=1&uploadId="+uploadID, http.Header{"X-Amz-Copy-Source": {"/bucket/source"}}, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, "</ETag><ChecksumCRC32>"+crc32Value+"</ChecksumCRC32></CopyPartResult>")
	})

	// Like S3, an upload requested without an algorithm is reported as without checksums until it's
	// completed, though it has default CRC64NVME checksums.
	t.Run("default", func(t *testing.T) {
		resp, uploadID := create(t, "default", nil)
		require.Empty(t, resp.Header.Get("X-Amz-Checksum-Algorithm"))
		require.Empty(t, resp.Header.Get("X-Amz-Checksum-Type"))

		resp = putPart("default", uploadID, http.Header{"X-Amz-Checksum-Crc32": {crc32Value}})
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Equal(t, crc32Value, resp.Header.Get("X-Amz-Checksum-Crc32"))
		resp = putPart("default", uploadID, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Empty(t, resp.Header.Get("X-Amz-Checksum-Crc64nvme"))

		resp = serve(t, objectAPI, http.MethodGet, "/bucket/default?uploadId="+uploadID, nil, nil)
		require.NotContains(t, resp.Body, "Checksum")
		resp = serve(t, objectAPI, http.MethodGet, "/bucket?uploads&prefix=default", nil, nil)
		require.NotContains(t, resp.Body, "Checksum")

		require.Equal(t, http.StatusOK, serve(t, objectAPI, http.MethodPut, "/bucket/default-source", nil, data).StatusCode)
		_, copyUploadID := create(t, "default-copy", nil)
		resp = serve(t, objectAPI, http.MethodPut, "/bucket/default-copy?partNumber=1&uploadId="+copyUploadID, http.Header{"X-Amz-Copy-Source": {"/bucket/default-source"}}, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.NotContains(t, resp.Body, "Checksum")

		resp = complete("default", uploadID, "<ChecksumCRC64NVME>jSnVw/bqjr4=</ChecksumCRC64NVME>", nil)
		require.Contains(t, resp.Body, "<Code>InvalidPart</Code>")
		resp = complete("default", uploadID, "", http.Header{"X-Amz-Checksum-Type": {"FULL_OBJECT"}})
		require.Contains(t, resp.Body, "<Message>The upload was created using the null checksum mode. The complete request must use the same checksum mode.</Message>")

		// A whole object checksum without a type is ignored.
		resp = complete("default", uploadID, "", http.Header{"X-Amz-Checksum-Crc32": {crc32Value}})
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, "<ChecksumCRC64NVME>jSnVw/bqjr4=</ChecksumCRC64NVME><ChecksumType>FULL_OBJECT</ChecksumType>")
	})
}
