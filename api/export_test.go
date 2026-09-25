// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api

import (
	"testing"
	"time"
)

// SetCompleteMultipartUploadKeepAliveInterval overrides the keep-alive interval of
// CompleteMultipartUpload responses for the duration of a test.
func SetCompleteMultipartUploadKeepAliveInterval(t testing.TB, interval time.Duration) {
	old := completeMultipartUploadKeepAliveInterval
	completeMultipartUploadKeepAliveInterval = interval
	t.Cleanup(func() { completeMultipartUploadKeepAliveInterval = old })
}
