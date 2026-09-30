// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"context"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

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
		body := []byte(fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(data), data))
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
		body := []byte(fmt.Sprintf("%x\r\n%s\r\n0\r\nx-amz-checksum-crc32:%s\r\n\r\n", len(data), data, crc32Base64(data)))
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
}

func TestDeleteObjectsWithChecksum(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		deleteObjects: func(_ context.Context, _ string, objects []cmd.ObjectToDelete, _ cmd.ObjectOptions) ([]cmd.DeletedObject, []cmd.DeleteObjectsError, error) {
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
	trailerBody := []byte(fmt.Sprintf("%x\r\n%s\r\n0\r\nx-amz-checksum-crc32:%s\r\n\r\n", len(data), data, crc32Base64(data)))
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
		{name: "unimplemented algorithm", header: with(nil, "X-Amz-Checksum-Md5", "AAAA"), status: 501, message: notImplemented},
		{name: "unimplemented trailer", header: with(trailer, "X-Amz-Trailer", "x-amz-checksum-md5"), trailer: true, status: 501, message: notImplemented},
		{name: "multiple algorithms", header: with(nil, "X-Amz-Checksum-Crc32", crc32Base64(data), "X-Amz-Checksum-Crc32c", "AAAAAA=="), status: 400, message: multiple},
		{name: "header and trailer", header: with(trailer, "X-Amz-Checksum-Crc32c", "AAAAAA=="), trailer: true, status: 400, message: multiple},
		{name: "sdk algorithm without checksum", header: with(nil, "X-Amz-Sdk-Checksum-Algorithm", "CRC32"), status: 400, message: "x-amz-sdk-checksum-algorithm specified, but no corresponding x-amz-checksum-* or x-amz-trailer headers were found."},
		{name: "sdk algorithm mismatches header", header: with(nil, "X-Amz-Sdk-Checksum-Algorithm", "SHA1", "X-Amz-Checksum-Crc32", crc32Base64(data)), status: 400, message: invalidSdk},
		{name: "sdk algorithm mismatches trailer", header: with(trailer, "X-Amz-Sdk-Checksum-Algorithm", "CRC32C"), trailer: true, status: 400, message: invalidSdk},
		{name: "unknown sdk algorithm", header: with(nil, "X-Amz-Sdk-Checksum-Algorithm", "FOO", "X-Amz-Checksum-Crc32", crc32Base64(data)), status: 400, message: invalidSdk},
		{name: "unimplemented sdk algorithm", header: with(nil, "X-Amz-Sdk-Checksum-Algorithm", "MD5", "X-Amz-Checksum-Crc32", crc32Base64(data)), status: 501, message: notImplemented},
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
		{name: "unimplemented algorithm", header: http.Header{"X-Amz-Checksum-Md5": {"AAAA"}}, status: 501, code: "NotImplemented"},
		{name: "XML still verified", header: http.Header{"X-Amz-Checksum-Crc32": {"AAAAAA=="}, "Content-Md5": {"AAAAAAAAAAAAAAAAAAAAAA=="}}, status: 400, code: "BadDigest"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			layer := &fakeObjectLayer{completeMultipartUpload: func(context.Context, string, string, string, []cmd.CompletePart, cmd.ObjectOptions) (cmd.ObjectInfo, error) {
				called = true
				return cmd.ObjectInfo{ETag: "etag"}, nil
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
				layer := &fakeObjectLayer{deleteObjects: func(context.Context, string, []cmd.ObjectToDelete, cmd.ObjectOptions) ([]cmd.DeletedObject, []cmd.DeleteObjectsError, error) {
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

func TestPutObjectSHA512AndXXHASHChecksums(t *testing.T) {
	data := []byte("hello")
	sum := sha512.Sum512(data)
	sha512Base64 := base64.StdEncoding.EncodeToString(sum[:])

	objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
	header := http.Header{"X-Amz-Checksum-Sha512": {sha512Base64}, "Content-Length": {"5"}}
	resp := serveHTTP(t, objectAPI, http.MethodPut, "/bucket/key", header, data)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, sha512Base64, resp.Header.Get("X-Amz-Checksum-Sha512"))
	require.Equal(t, data, objectAPI.data)

	// A wrong XXHASH64 value is verified and rejected.
	objectAPI = &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
	header = http.Header{"X-Amz-Checksum-Xxhash64": {"AAAAAAAAAAA="}, "Content-Length": {"5"}}
	resp = serveHTTP(t, objectAPI, http.MethodPut, "/bucket/key", header, data)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>BadDigest</Code>")
	require.Nil(t, objectAPI.data)
}
