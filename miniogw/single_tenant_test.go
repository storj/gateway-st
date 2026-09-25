// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	minio "storj.io/minio/cmd"
)

func TestMinioError(t *testing.T) {
	require.True(t, minioError(minio.ObjectNotFound{}))
	// InvalidUploadID isn't a GenericError, but it's a client error that mustn't be logged.
	require.True(t, minioError(minio.InvalidUploadID{UploadID: "id"}))
	require.True(t, minioError(DeleteMarkerError{VersionID: "v1"}))
	require.False(t, minioError(errors.New("unexpected")))
}

func TestDeleteMarkerErrorAPIError(t *testing.T) {
	apiErr := minio.ToAPIError(t.Context(), DeleteMarkerError{VersionID: "v1"})
	require.Equal(t, http.StatusMethodNotAllowed, apiErr.HTTPStatusCode)
	require.Equal(t, "MethodNotAllowed", apiErr.Code)
}
