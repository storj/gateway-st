// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api

import (
	"errors"
	"net/http"
	"testing"

	"github.com/amwolff/awsig"
	"github.com/stretchr/testify/require"

	"storj.io/minio/cmd"
)

func TestGetChecksumMismatchFromErrorPriority(t *testing.T) {
	err := awsig.ChecksumMismatchError{Mismatches: []awsig.ChecksumMismatch{
		{Algorithm: awsig.AlgorithmCRC32},
		{Algorithm: awsig.AlgorithmSHA256, IsContentSHA256: true},
		{Algorithm: awsig.AlgorithmMD5},
	}}

	mismatch, ok := getChecksumMismatchFromError(err)
	require.True(t, ok)
	require.Equal(t, awsig.AlgorithmMD5, mismatch.Algorithm)
}

func TestObjectLayerErrorMessages(t *testing.T) {
	resp, ok := errToResponse(cmd.NotImplemented{Message: "UploadPartCopy: not enabled"})
	require.True(t, ok)
	require.Equal(t, http.StatusNotImplemented, resp.HTTPStatusCode)
	require.Equal(t, "UploadPartCopy: not enabled", resp.Description)

	resp, ok = errToResponse(cmd.InvalidArgument{Bucket: "bucket", Object: "key", Err: errors.New("partID is out of range")})
	require.True(t, ok)
	require.Equal(t, http.StatusBadRequest, resp.HTTPStatusCode)
	require.Equal(t, "InvalidArgument", resp.Code)
	require.Equal(t, "partID is out of range", resp.Description)
}
