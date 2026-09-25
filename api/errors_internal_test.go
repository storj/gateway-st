// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api

import (
	"testing"

	"github.com/amwolff/awsig"
	"github.com/stretchr/testify/require"
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
