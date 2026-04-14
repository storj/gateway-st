// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package apierr_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/gateway/cmd/apierrgen/gen"
)

func TestGeneratedErrors(t *testing.T) {
	actualGenerated, err := os.ReadFile("errors_gen.go")
	require.NoError(t, err)

	errorDefs, err := os.ReadFile("errors.yaml")
	require.NoError(t, err)

	expectedGenerated, err := gen.Generate(errorDefs, "apierr")
	require.NoError(t, err)

	require.Equal(t, string(expectedGenerated), string(actualGenerated),
		"The file containing generated error definitions has unexpected contents and may have been manually modified."+
			" Error definitions must be generated from \"./api/apierr/errors.yaml\"."+
			" Run \"go generate ./api/apierr\" to resolve this issue.")
}
