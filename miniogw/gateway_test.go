// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	minio "storj.io/minio/cmd"
)

func TestLimitResultsWithAlignment(t *testing.T) {
	for i, tt := range [...]struct {
		maxKeys  int
		expected int
	}{
		{-10000, 999},
		{-4500, 999},
		{-1000, 999},
		{-999, 999},
		{-998, 999},
		{-500, 999},
		{-1, 999},
		{0, 0},
		{1, 1},
		{500, 500},
		{998, 998},
		{999, 999},
		{1000, 999},
		{4500, 999},
		{10000, 999},
	} {
		assert.Equal(t, tt.expected, limitResultsWithAlignment(tt.maxKeys, 1000), i)
	}
}

func TestLimitResults(t *testing.T) {
	for i, tt := range [...]struct {
		maxKeys  int
		expected int
	}{
		{-10000, 1000},
		{-4500, 1000},
		{-1000, 1000},
		{-999, 1000},
		{-998, 1000},
		{-500, 1000},
		{-1, 1000},
		{0, 0},
		{1, 1},
		{500, 500},
		{998, 998},
		{999, 999},
		{1000, 1000},
		{4500, 1000},
		{10000, 1000},
	} {
		assert.Equal(t, tt.expected, limitResults(tt.maxKeys, 1000), i)
	}
}

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
