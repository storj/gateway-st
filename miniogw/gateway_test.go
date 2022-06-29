// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLimitResults(t *testing.T) {
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
		assert.Equal(t, tt.expected, limitResults(tt.maxKeys, 1000), i)
	}
}
