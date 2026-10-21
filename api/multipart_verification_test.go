// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/minio/cmd"
)

func TestCompleteMultipartUploadBodyVerification(t *testing.T) {
	for _, padding := range []string{"", strings.Repeat(" ", 8192)} {
		body := []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>etag</ETag></Part></CompleteMultipartUpload>` + padding)
		for _, tt := range []struct {
			name   string
			header http.Header
			code   string
		}{
			{name: "valid"},
			{name: "invalid payload hash", header: http.Header{"X-Amz-Content-Sha256": {strings.Repeat("0", 64)}}, code: "XAmzContentSHA256Mismatch"},
			{name: "invalid MD5", header: http.Header{"Content-Md5": {"AAAAAAAAAAAAAAAAAAAAAA=="}}, code: "BadDigest"},
		} {
			t.Run(fmt.Sprintf("%s/padding=%d", tt.name, len(padding)), func(t *testing.T) {
				called := false
				layer := &fakeObjectLayer{completeMultipartUpload: func(context.Context, string, string, string, []cmd.CompletePart, cmd.ObjectOptions) (cmd.ObjectInfo, error) {
					called = true
					return cmd.ObjectInfo{ETag: "etag"}, nil
				}}
				resp := serve(t, layer, http.MethodPost, "/bucket/key?uploadId=id", tt.header, body)
				if tt.code == "" {
					require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
					require.True(t, called)
				} else {
					require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
					require.Contains(t, resp.Body, "<Code>"+tt.code+"</Code>")
					require.False(t, called)
				}
			})
		}
	}
}

func TestCreateMultipartUploadChecksumHeaders(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		newMultipartUpload: func(context.Context, string, string, cmd.ObjectOptions) (string, error) {
			return "upload-id", nil
		},
	}

	for _, tt := range []struct {
		algorithm, checksumType string
		status                  int
	}{
		{"", "", http.StatusOK},
		{"CRC32", "", http.StatusOK},
		{"crc32c", "FULL_OBJECT", http.StatusOK},
		{"CRC64NVME", "FULL_OBJECT", http.StatusOK},
		{"SHA256", "COMPOSITE", http.StatusOK},
		{"FOO", "", http.StatusBadRequest},
		{"", "COMPOSITE", http.StatusBadRequest},
		{"SHA256", "FULL_OBJECT", http.StatusBadRequest},
		{"CRC64NVME", "COMPOSITE", http.StatusBadRequest},
		{"CRC32", "BOGUS", http.StatusBadRequest},
		{"MD5", "", http.StatusNotImplemented},
	} {
		header := http.Header{}
		if tt.algorithm != "" {
			header.Set("X-Amz-Checksum-Algorithm", tt.algorithm)
		}
		if tt.checksumType != "" {
			header.Set("X-Amz-Checksum-Type", tt.checksumType)
		}
		resp := serve(t, objectAPI, http.MethodPost, "/bucket/key?uploads", header, nil)
		require.Equal(t, tt.status, resp.StatusCode, "%s %s: %s", tt.algorithm, tt.checksumType, resp.Body)
	}
}
