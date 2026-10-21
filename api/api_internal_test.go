// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHostname(t *testing.T) {
	for host, exp := range map[string]string{
		"example.com":              "example.com",
		"Example.COM:7777":         "example.com",
		"bucket.example.com.":      "bucket.example.com",
		"bucket.example.com.:7777": "bucket.example.com",
		"[::1]":                    "::1",
		"[::1]:7777":               "::1",
		"127.0.0.1:7777":           "127.0.0.1",
	} {
		require.Equal(t, exp, hostname(&http.Request{Host: host}), host)
	}
}
