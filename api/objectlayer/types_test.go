// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package objectlayer_test

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/gateway/api/objectlayer"
	"storj.io/minio/cmd"
)

func TestGetObjectReader(t *testing.T) {
	info := objectlayer.ObjectInfo{ETag: "etag"}

	t.Run("precondition failed", func(t *testing.T) {
		var cleanedUp bool
		opts := objectlayer.ObjectOptions{CheckPrecondFn: func(info objectlayer.ObjectInfo) bool { return info.ETag == "etag" }}

		_, err := objectlayer.NewGetObjectReader(strings.NewReader("data"), info, opts, func() { cleanedUp = true })
		require.ErrorIs(t, err, cmd.PreConditionFailed{})
		assert.True(t, cleanedUp)
	})

	t.Run("cleanup order", func(t *testing.T) {
		var order []string
		reader, err := objectlayer.NewGetObjectReader(strings.NewReader("data"), info, objectlayer.ObjectOptions{}, func() { order = append(order, "download") })
		require.NoError(t, err)
		reader.AppendCleanupFunc(func() { order = append(order, "project") })

		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, "data", string(data))

		require.NoError(t, reader.Close())
		require.NoError(t, reader.Close())
		assert.Equal(t, []string{"download", "project"}, order)
	})
}
