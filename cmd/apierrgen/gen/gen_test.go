// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package gen_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/gateway/cmd/apierrgen/gen"
)

func TestGenerateStrict(t *testing.T) {
	_, err := gen.Generate([]byte("- name: A\n  code: A\n  description: a\n  http_status: 400\n"), "p")
	require.NoError(t, err)

	_, err = gen.Generate([]byte("- name: A\n  code: A\n  message: a\n  http_status: 400\n"), "p")
	require.ErrorContains(t, err, "message")

	_, err = gen.Generate([]byte("- name: A\n  code: A\n  description: \"\"\n  http_status: 400\n"), "p")
	require.ErrorContains(t, err, "no description")
}
