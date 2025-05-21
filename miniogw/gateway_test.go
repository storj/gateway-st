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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorIs(t, verifyIfNoneMatch(tc.input), tc.err)
		})
	}
}
