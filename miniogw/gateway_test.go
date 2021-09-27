// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLimitMaxKeys(t *testing.T) {
	g := gatewayLayer{
		compatibilityConfig: S3CompatibilityConfig{
			MaxKeysLimit: 1000,
		},
	}

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
		{0, 999},
		{1, 1},
		{500, 500},
		{998, 998},
		{999, 999},
		{1000, 999},
		{4500, 999},
		{10000, 999},
	} {
		assert.Equal(t, tt.expected, g.limitMaxKeys(tt.maxKeys), i)
	}
}
