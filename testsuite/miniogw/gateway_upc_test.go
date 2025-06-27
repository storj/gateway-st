// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/gateway/miniogw"
	minio "storj.io/minio/cmd"
	"storj.io/minio/pkg/auth"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite"
)

func TestCopyObjectPart(t *testing.T) {
	t.Parallel()

	const uploadPartCopyEnabledLocation = "UploadPartCopyEnabled"

	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				require.NoError(t, config.Placement.Set("test_placement_rules.yaml"))
				config.Console.Placement.SelfServeEnabled = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]
		upl := planet.Uplinks[0]

		project, err := upl.OpenProject(ctx, sat)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		// Establish new context with *uplink.Project for the gateway to pick up.
		ctx2 := miniogw.WithCredentials(ctx, project, miniogw.CredentialsInfo{
			Access:          upl.Access[sat.ID()],
			PublicProjectID: upl.Projects[0].PublicID.String(),
		})

		defaultS3CompatibilityConfig.UploadPartCopy.EnabledCombinations = []string{
			testrand.UUID().String() + ":" + uploadPartCopyEnabledLocation,
			upl.Projects[0].PublicID.String() + ":" + uploadPartCopyEnabledLocation,
			"*:1234567890",
		}
		layer, err := miniogw.NewStorjGateway(defaultS3CompatibilityConfig).NewGatewayLayer(auth.Credentials{})
		require.NoError(t, err)

		defer func() { require.NoError(t, layer.Shutdown(ctx2)) }()

		bucketName := testrand.BucketName()
		require.NoError(t, layer.MakeBucketWithLocation(ctx2, bucketName, minio.BucketOptions{
			Location: uploadPartCopyEnabledLocation,
		}))

		srcObject, dstObject := "original", "upc-copy"
		data := testrand.Bytes(100 * memory.KB)

		_, err = createFile(ctx2, project, bucketName, srcObject, data, map[string]string{
			"apples":  "20",
			"bananas": "12",
		})
		require.NoError(t, err)

		uploadID, err := layer.NewMultipartUpload(ctx2, bucketName, dstObject, minio.ObjectOptions{})
		require.NoError(t, err)

		defer func() {
			if err = layer.AbortMultipartUpload(ctx2, bucketName, dstObject, uploadID, minio.ObjectOptions{}); err != nil {
				assert.ErrorIs(t, err, minio.InvalidUploadID{Bucket: bucketName, Object: dstObject, UploadID: uploadID})
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

			info, err := layer.CopyObjectPart(ctx2, bucketName, srcObject, bucketName, dstObject, uploadID, i+1, offset, length, minio.ObjectInfo{}, minio.ObjectOptions{}, minio.ObjectOptions{})
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

		_, err = layer.CompleteMultipartUpload(ctx2, bucketName, dstObject, uploadID, parts, minio.ObjectOptions{})
		require.NoError(t, err)

		actual, err := project.DownloadObject(ctx2, bucketName, dstObject, nil)
		require.NoError(t, err)

		defer func() { require.NoError(t, actual.Close()) }()

		actualData, err := io.ReadAll(actual)
		require.NoError(t, err)

		require.Equal(t, data, actualData)
	})
}
