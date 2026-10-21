// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api

import (
	"errors"
	"fmt"
	"io"
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

func TestAwsigErrToCode(t *testing.T) {
	for _, tt := range []struct {
		err  error
		code string
	}{
		// awsig nests these errors in more general ones.
		{fmt.Errorf("%w: %w", awsig.ErrInvalidChunkSize, awsig.ErrEntityTooSmall), "InvalidChunkSizeError"},
		{fmt.Errorf("%w: %w", awsig.ErrMalformedPOSTRequest, awsig.ErrMessageTooLarge), "MaxPostPreDataLengthExceededError"},
		{fmt.Errorf("%w: %w", awsig.ErrMalformedTrailer, io.ErrUnexpectedEOF), "MalformedTrailerError"},
		{awsig.ErrAuthorizationQueryParametersError, "AuthorizationQueryParametersError"},
		{awsig.ErrInvalidChecksumRequest, "InvalidRequest"},
		{awsig.ErrUnsignedHeader, "AccessDenied"},
		{awsig.ErrInvalidArgument, "InvalidArgument"},
	} {
		resp, ok := errToResponse(tt.err)
		require.True(t, ok, tt.err)
		require.Equal(t, tt.code, resp.Code, tt.err)
	}
}
