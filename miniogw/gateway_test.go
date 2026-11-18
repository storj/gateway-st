// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"testing"

	"github.com/stretchr/testify/require"

	minio "storj.io/minio/cmd"
)

func TestVerifyIfNoneMatch(t *testing.T) {
	unimplementedErr := minio.NotImplemented{Message: "If-None-Match only supports a single value of '*'"}

	var testCases = []struct {
		name  string
		input []string
		err   error
	}{
		{"empty", []string{}, nil},
		{"match all", []string{"*"}, nil},
		{"match all with invalid value", []string{"*", "something"}, unimplementedErr},
		{"match all with invalid values", []string{"*", "something", "else"}, unimplementedErr},
		{"invalid value", []string{"something"}, unimplementedErr},
		{"invalue values", []string{"something", "else"}, unimplementedErr},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorIs(t, verifyIfNoneMatch(tc.input), tc.err)
		})
	}
}

func TestDefaultChecksumAlgorithm(t *testing.T) {
	for _, tt := range []struct {
		algorithm string
		valid     bool
	}{
		{"", true},
		{"CRC32", true},
		{"CRC32C", true},
		{"CRC64NVME", true},
		{"SHA1", false},
		{"SHA256", false},
		{"FOO", false},
	} {
		_, err := NewStorjGateway(S3CompatibilityConfig{ObjectChecksums: true, DefaultChecksumAlgorithm: tt.algorithm}).NewObjectLayer()
		if tt.valid {
			require.NoError(t, err, tt.algorithm)
		} else {
			require.Error(t, err, tt.algorithm)
		}
	}
}
