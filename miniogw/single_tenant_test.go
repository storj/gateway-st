// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	minio "storj.io/minio/cmd"
)

func TestMinioError(t *testing.T) {
	require.True(t, minioError(minio.ObjectNotFound{}))
	// InvalidUploadID isn't a GenericError, but it's a client error that mustn't be logged.
	require.True(t, minioError(minio.InvalidUploadID{UploadID: "id"}))
	require.False(t, minioError(errors.New("unexpected")))
}
