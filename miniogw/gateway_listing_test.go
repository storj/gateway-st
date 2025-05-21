// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
