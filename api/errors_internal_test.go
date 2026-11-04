// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/amwolff/awsig"
	"github.com/stretchr/testify/require"

	"storj.io/gateway/api/apierr"
	"storj.io/gateway/api/objectlayer"
	"storj.io/minio/cmd"
	objectlock "storj.io/minio/pkg/bucket/object/lock"
)

func TestGetChecksumMismatchFromErrorPriority(t *testing.T) {
	sha256 := awsig.ChecksumMismatch{Algorithm: awsig.AlgorithmSHA256, IsContentSHA256: true}
	md5 := awsig.ChecksumMismatch{Algorithm: awsig.AlgorithmMD5}
	crc32 := awsig.ChecksumMismatch{Algorithm: awsig.AlgorithmCRC32}

	// The mismatch that S3 reported for each combination.
	for _, tt := range []struct {
		mismatches []awsig.ChecksumMismatch
		want       awsig.ChecksumMismatch
	}{
		{[]awsig.ChecksumMismatch{crc32, md5, sha256}, sha256},
		{[]awsig.ChecksumMismatch{md5, sha256}, sha256},
		{[]awsig.ChecksumMismatch{crc32, sha256}, sha256},
		{[]awsig.ChecksumMismatch{crc32, md5}, md5},
	} {
		mismatch, ok := getChecksumMismatchFromError(awsig.ChecksumMismatchError{Mismatches: tt.mismatches})
		require.True(t, ok)
		require.Equal(t, tt.want, mismatch)
	}

	// Unlike PutObject and UploadPart, the operations with an XML body, which readVerifiedBody
	// reads, report Content-MD5 before X-Amz-Content-Sha256.
	for _, tt := range []struct {
		mismatches []awsig.ChecksumMismatch
		want       awsig.ChecksumMismatch
	}{
		{[]awsig.ChecksumMismatch{crc32, md5, sha256}, md5},
		{[]awsig.ChecksumMismatch{sha256, md5}, md5},
		{[]awsig.ChecksumMismatch{md5, sha256}, md5},
		{[]awsig.ChecksumMismatch{crc32, sha256}, sha256},
		{[]awsig.ChecksumMismatch{crc32, md5}, md5},
	} {
		err := xmlBodyChecksumError{awsig.ChecksumMismatchError{Mismatches: tt.mismatches}}
		mismatch, ok := getChecksumMismatchFromError(err)
		require.True(t, ok)
		require.Equal(t, tt.want, mismatch)
	}
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

// TestValidateTaggingHeaderLikeS3 checks the answers that S3 gave for the same x-amz-tagging
// headers, including the tags it stored.
func TestValidateTaggingHeaderLikeS3(t *testing.T) {
	for _, tt := range []struct {
		header string
		err    error
	}{
		{"a=1", nil},
		// S3 drops trailing equals signs.
		{"a==", nil},
		{"a===", nil},
		{"a=b=", nil},
		{"a==&c=d", nil},
		{"&a==&", nil},
		{"a=%3D", nil},
		{"a=%3D=", nil},
		{"a=1&b=2&c=3=", nil},
		{"a==b", apierr.CodeInvalidTaggingHeader},
		{"a=b=c", apierr.CodeInvalidTaggingHeader},
		{"a=1&b=2=3", apierr.CodeInvalidTaggingHeader},
		{"=", apierr.CodeInvalidTaggingHeader},
		{"==", apierr.CodeInvalidTaggingHeader},
		{"===", apierr.CodeInvalidTaggingHeader},
		{"=a=", apierr.CodeInvalidTaggingHeader},
		{"a=1&=", apierr.CodeInvalidTaggingHeader},
		// A semicolon is part of a tag, not a separator.
		{";", errInvalidTagKey},
		{"a;b=1", errInvalidTagKey},
		{"a=1;", errInvalidTagValue},
		{"a=1;b", errInvalidTagValue},
		{"a=1;b=2", apierr.CodeInvalidTaggingHeader},
	} {
		_, err := validateTaggingHeader(tt.header)
		require.Equal(t, tt.err, err, tt.header)
	}
}

// TestValidateTaggingHeaderNormalizes checks that the stored header holds the tags that S3
// stored, which dropped the trailing equals signs.
func TestValidateTaggingHeaderNormalizes(t *testing.T) {
	for header, want := range map[string]string{
		"a==&c=d":   "a&c=d",
		"&a=b=&":    "a=b",
		"a=%3D&b==": "a=%3D&b",
		"a=1&&b=2":  "a=1&b=2",
		"a+b=c%20d": "a+b=c%20d",
	} {
		got, err := validateTaggingHeader(header)
		require.NoError(t, err, header)
		require.Equal(t, want, got, header)
	}
}

func TestErrorResponsesLikeS3(t *testing.T) {
	// The key's length and the limit are reported for a key that is too long.
	resp, ok := errToResponse(objectlayer.ObjectNameTooLong{Bucket: "bucket", Object: strings.Repeat("k", 1025)})
	require.True(t, ok)
	require.Equal(t, "KeyTooLongError", resp.Code)
	require.Equal(t, "1025", resp.Size)
	require.Equal(t, "1024", resp.MaxSizeAllowed)

	for _, err := range []error{apierr.CodeMalformedXML, objectlock.ErrMalformedXML} {
		resp, ok = errToResponse(err)
		require.True(t, ok)
		require.Equal(t, "The XML you provided was not well-formed or did not validate against our published schema", resp.Description)
	}

	resp, ok = errToResponse(apierr.CodeMissingContentMD5)
	require.True(t, ok)
	require.Equal(t, "Missing required header for this request: Content-MD5 OR x-amz-checksum-*", resp.Description)
}
