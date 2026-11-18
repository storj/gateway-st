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

func TestSelectParts(t *testing.T) {
	parts := []objectlayer.ObjectPart{
		{PartNumber: 1, Offset: 0, Size: 5},
		{PartNumber: 3, Offset: 5, Size: 5},
		{PartNumber: 4, Offset: 10, Size: 1},
	}
	selected := func(parts []objectlayer.ObjectPart, opts objectlayer.ObjectOptions) objectlayer.ObjectInfo {
		var info objectlayer.ObjectInfo
		objectlayer.SelectParts(&info, parts, opts)
		return info
	}

	assert.Equal(t, objectlayer.ObjectInfo{PartsCount: 3, Parts: parts[1:2]}, selected(parts, objectlayer.ObjectOptions{PartNumber: 3}))
	assert.Equal(t, objectlayer.ObjectInfo{PartsCount: 3}, selected(parts, objectlayer.ObjectOptions{PartNumber: 2}))
	assert.Equal(t, objectlayer.ObjectInfo{}, selected(nil, objectlayer.ObjectOptions{PartNumber: 1}))

	page := func(marker, maxParts int) objectlayer.ObjectOptions {
		return objectlayer.ObjectOptions{ObjectParts: &objectlayer.ObjectPartsOptions{PartNumberMarker: marker, MaxParts: maxParts}}
	}
	assert.Equal(t, objectlayer.ObjectInfo{PartsCount: 3, Parts: parts}, selected(parts, page(0, 1000)))
	assert.Equal(t, objectlayer.ObjectInfo{PartsCount: 3, Parts: parts[:1], PartsTruncated: true}, selected(parts, page(0, 1)))
	assert.Equal(t, objectlayer.ObjectInfo{PartsCount: 3, Parts: parts[1:2], PartsTruncated: true}, selected(parts, page(1, 1)))
	assert.Equal(t, objectlayer.ObjectInfo{PartsCount: 3}, selected(parts, page(4, 1000)))
	assert.Equal(t, objectlayer.ObjectInfo{PartsCount: 3}, selected(parts, page(0, 0)))
}
