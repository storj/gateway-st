// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testrand"
	minio "storj.io/minio/cmd"
	"storj.io/uplink"
)

func TestCopyObjectPart(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		bucket, err := project.CreateBucket(ctx, testrand.BucketName())
		require.NoError(t, err)

		srcObject, dstObject := "original", "upc-copy"
		data := testrand.Bytes(100 * memory.KB)

		_, err = createFile(ctx, project, bucket.Name, srcObject, data, nil)
		require.NoError(t, err)

		uploadID, err := layer.NewMultipartUpload(ctx, bucket.Name, dstObject, minio.ObjectOptions{})
		require.NoError(t, err)

		defer func() {
			if err = layer.AbortMultipartUpload(ctx, bucket.Name, dstObject, uploadID, minio.ObjectOptions{}); err != nil {
				assert.ErrorIs(t, err, minio.InvalidUploadID{Bucket: bucket.Name, Object: dstObject, UploadID: uploadID})
			}
		}()

		var parts []minio.CompletePart
		for i, offsetLength := range [...]struct {
			offset int64
			length int64
		}{
			{
				offset: 0 * memory.KB.Int64(),
				length: 10 * memory.KB.Int64(),
			},
			{
				offset: 10 * memory.KB.Int64(),
				length: 80 * memory.KB.Int64(),
			},
			{
				offset: 90 * memory.KB.Int64(),
				length: 10 * memory.KB.Int64(),
			},
		} {
			offset, length := offsetLength.offset, offsetLength.length

			info, err := layer.CopyObjectPart(ctx, bucket.Name, srcObject, bucket.Name, dstObject, uploadID, i+1, offset, length, minio.ObjectInfo{}, minio.ObjectOptions{}, minio.ObjectOptions{})
			require.NoError(t, err)

			require.Equal(t, i+1, info.PartNumber)
			// require.NotEmpty(t, info.LastModified) -- TODO(artur): not set?
			require.Equal(t, md5Hex(data[offset:offset+length]), info.ETag)
			require.Equal(t, length, info.Size)
			require.Equal(t, length, info.ActualSize)

			parts = append(parts, minio.CompletePart{
				PartNumber: info.PartNumber,
				ETag:       info.ETag,
			})
		}

		_, err = layer.CompleteMultipartUpload(ctx, bucket.Name, dstObject, uploadID, parts, minio.ObjectOptions{})
		require.NoError(t, err)

		actual, err := project.DownloadObject(ctx, bucket.Name, dstObject, nil)
		require.NoError(t, err)

		defer func() { require.NoError(t, actual.Close()) }()

		actualData, err := io.ReadAll(actual)
		require.NoError(t, err)

		require.Equal(t, data, actualData)
	})
}
