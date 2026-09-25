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
