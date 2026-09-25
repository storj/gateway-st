// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"context"
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
		require.Contains(t, resp.Body, "<Code>InvalidRequest</Code>")
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
