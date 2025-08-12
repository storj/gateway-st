// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"context"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"storj.io/common/memory"
	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/gateway/miniogw"
	minio "storj.io/minio/cmd"
	"storj.io/minio/pkg/auth"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite"
	"storj.io/storj/satellite/console"
	"storj.io/storj/satellite/nodeselection"
	"storj.io/uplink"
)

func TestCopyObjectPart(t *testing.T) {
	t.Parallel()

	for _, tc := range [...]struct {
		name                      string
		enabledCombinations       []string
		projectID, bucketLocation string
		expectedFailure           bool
	}{
		{
			name:           "project enabled for one location",
			projectID:      "a04ade2c-6b0e-4e0d-92f8-df8410699bb2",
			bucketLocation: "New Zealand",
			enabledCombinations: []string{
				"*:Yugoslavia",
				"a04ade2c-6b0e-4e0d-92f8-df8410699bb2:New Zealand",
				"e0e4a478-4e94-4fe3-b592-661f67ef652c:*",
			},
			expectedFailure: false,
		},
		{
			name:           "project enabled for all locations",
			projectID:      "a04ade2c-6b0e-4e0d-92f8-df8410699bb2",
			bucketLocation: "Poland",
			enabledCombinations: []string{
				"*:United States of America",
				"a04ade2c-6b0e-4e0d-92f8-df8410699bb2:*",
				"a04ade2c-6b0e-4e0d-92f8-df8410699bb2:New Zealand",
				"e0e4a478-4e94-4fe3-b592-661f67ef652c:New Zealand",
			},
			expectedFailure: false,
		},
		{
			name:           "location enabled for all projects",
			projectID:      "a04ade2c-6b0e-4e0d-92f8-df8410699bb2",
			bucketLocation: "United States of America",
			enabledCombinations: []string{
				"*:United States of America",
				"e0e4a478-4e94-4fe3-b592-661f67ef652c:*",
				"e0e4a478-4e94-4fe3-b592-661f67ef652c:New Zealand",
			},
			expectedFailure: false,
		},
		{
			name:           "location enabled for all projects 2",
			projectID:      "e0e4a478-4e94-4fe3-b592-661f67ef652c",
			bucketLocation: "New Zealand",
			enabledCombinations: []string{
				"*:New Zealand",
				"e0e4a478-4e94-4fe3-b592-661f67ef652c:*",
				"e0e4a478-4e94-4fe3-b592-661f67ef652c:United States of America",
			},
			expectedFailure: false,
		},
		{
			name:           "all projects enabled for all locations",
			projectID:      "a04ade2c-6b0e-4e0d-92f8-df8410699bb2",
			bucketLocation: "New Zealand",
			enabledCombinations: []string{
				"*:*",
				"*:United States of America",
				"e0e4a478-4e94-4fe3-b592-661f67ef652c:*",
				"e0e4a478-4e94-4fe3-b592-661f67ef652c:Poland",
			},
			expectedFailure: false,
		},
		{
			name:                "no project ID",
			projectID:           "",
			bucketLocation:      "New Zealand",
			enabledCombinations: []string{"*:*"},
			expectedFailure:     false,
		},
		{
			name:           "project is not enabled for the location",
			projectID:      "a04ade2c-6b0e-4e0d-92f8-df8410699bb2",
			bucketLocation: "New Zealand",
			enabledCombinations: []string{
				"*:United States of America",
				"a04ade2c-6b0e-4e0d-92f8-df8410699bb2:Poland",
				"e0e4a478-4e94-4fe3-b592-661f67ef652c:*",
			},
			expectedFailure: true,
		},
		{
			name:           "project is not enabled for all locations",
			projectID:      "a04ade2c-6b0e-4e0d-92f8-df8410699bb2",
			bucketLocation: "New Zealand",
			enabledCombinations: []string{
				"*:United States of America",
				"e0e4a478-4e94-4fe3-b592-661f67ef652c:Poland",
			},
			expectedFailure: true,
		},
		{
			name:                "no enabled combinations",
			projectID:           "a04ade2c-6b0e-4e0d-92f8-df8410699bb2",
			bucketLocation:      "New Zealand",
			enabledCombinations: []string{},
			expectedFailure:     true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rand.Shuffle(len(tc.enabledCombinations), func(i, j int) {
				tc.enabledCombinations[i], tc.enabledCombinations[j] = tc.enabledCombinations[j], tc.enabledCombinations[i]
			})

			testCopyObjectPart(t, tc.enabledCombinations, tc.projectID, tc.bucketLocation, tc.expectedFailure)
		})
	}
}

func TestCopyObjectPartRange(t *testing.T) {
	t.Parallel()

	for _, tc := range [...]struct {
		name            string
		contentLength   memory.Size
		offset          int64
		length          int64
		expectedFailure bool
	}{
		{
			name:            "zero offset, negative length",
			contentLength:   5 * memory.KB,
			offset:          0,
			length:          -1,
			expectedFailure: false,
		},
		{
			name:            "zero offset, equal length",
			contentLength:   500,
			offset:          0,
			length:          500,
			expectedFailure: false,
		},
		{
			name:            "zero offset, less than content length length",
			contentLength:   500,
			offset:          0,
			length:          456,
			expectedFailure: false,
		},
		{
			name:            "non-zero offset, negative length",
			contentLength:   5 * memory.KB,
			offset:          123,
			length:          -1,
			expectedFailure: false,
		},
		{
			name:            "non-zero offset, less than content length length 1",
			contentLength:   500,
			offset:          123,
			length:          377,
			expectedFailure: false,
		},
		{
			name:            "non-zero offset, less than content length length 2",
			contentLength:   500,
			offset:          123,
			length:          246,
			expectedFailure: false,
		},
		{
			name:            "out of range 1",
			contentLength:   500,
			offset:          500,
			length:          1,
			expectedFailure: true,
		},
		{
			name:            "out of range 2",
			contentLength:   500,
			offset:          501,
			length:          1,
			expectedFailure: true,
		},
		{
			name:            "out of range 3",
			contentLength:   500,
			offset:          0,
			length:          501,
			expectedFailure: true,
		},
		{
			name:            "out of range 4",
			contentLength:   500,
			offset:          0,
			length:          999,
			expectedFailure: true,
		},
		{
			name:            "out of range 5",
			contentLength:   500,
			offset:          123,
			length:          999,
			expectedFailure: true,
		},
		{
			name:            "out of range 6",
			contentLength:   500,
			offset:          123,
			length:          500,
			expectedFailure: true,
		},
		{
			name:            "out of range 7",
			contentLength:   500,
			offset:          123,
			length:          378,
			expectedFailure: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			testCopyObjectPartRange(t, tc.contentLength, tc.offset, tc.length, tc.expectedFailure)
		})
	}
}

func testCopyObjectPart(t *testing.T, enabledCombinations []string, projectID, bucketLocation string, expectFailure bool) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Placement = nodeselection.ConfigurablePlacementRule{
					PlacementRules: `39:annotation("location","New Zealand");40:annotation("location","Poland");41:annotation("location","United States of America")`,
				}
				config.Console.Placement.SelfServeEnabled = true
				config.Console.Placement.SelfServeDetails.SetMap(map[storj.PlacementConstraint]console.PlacementDetail{
					39: {
						ID:     39,
						IdName: "New Zealand",
					},
					40: {
						ID:     40,
						IdName: "Poland",
					},
					41: {
						ID:     41,
						IdName: "United States of America",
					},
				})
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
			PublicProjectID: projectID,
		})

		compatibilityConfig := defaultS3CompatibilityConfig
		compatibilityConfig.UploadPartCopy.EnabledCombinations = enabledCombinations

		layer, err := miniogw.NewStorjGateway(compatibilityConfig).NewGatewayLayer(auth.Credentials{})
		require.NoError(t, err)

		defer func() { require.NoError(t, layer.Shutdown(ctx2)) }()

		bucketName := testrand.BucketName()
		require.NoError(t, layer.MakeBucketWithLocation(ctx2, bucketName, minio.BucketOptions{
			Location: bucketLocation,
		}))

		srcObject, dstObject := "original", "upc-copy"
		data := testrand.Bytes(10 * memory.KB)

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
				length: 1 * memory.KB.Int64(),
			},
			{
				offset: 1 * memory.KB.Int64(),
				length: 8 * memory.KB.Int64(),
			},
			{
				offset: 9 * memory.KB.Int64(),
				length: 1 * memory.KB.Int64(),
			},
		} {
			offset, length := offsetLength.offset, offsetLength.length

			info, err := layer.CopyObjectPart(ctx2, bucketName, srcObject, bucketName, dstObject, uploadID, i+1, offset, length, minio.ObjectInfo{}, minio.ObjectOptions{}, minio.ObjectOptions{})
			if expectFailure {
				require.ErrorIs(t, err, minio.NotImplemented{Message: "UploadPartCopy: not enabled for this project or location"})
				return
			}
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

func testCopyObjectPartRange(t *testing.T, contentLength memory.Size, offset, length int64, expectFailure bool) {
	runTest(t, func(t *testing.T, ctx context.Context, ol minio.ObjectLayer, p *uplink.Project) {
		srcBucket := testrand.BucketName()
		dstBucket := testrand.BucketName()

		_, err := p.EnsureBucket(ctx, srcBucket)
		require.NoError(t, err)
		_, err = p.EnsureBucket(ctx, dstBucket)
		require.NoError(t, err)

		src := "Moka.jpg"
		dst := "Moka copy.jpg"

		data := testrand.Bytes(contentLength)

		_, err = createFile(ctx, p, srcBucket, "Moka.jpg", data, nil)
		require.NoError(t, err)

		uploadID, err := ol.NewMultipartUpload(ctx, dstBucket, dst, minio.ObjectOptions{})
		require.NoError(t, err)

		defer func() {
			require.NoError(t, ol.AbortMultipartUpload(ctx, dstBucket, dst, uploadID, minio.ObjectOptions{}))
		}()

		info, err := ol.CopyObjectPart(ctx, srcBucket, src, dstBucket, dst, uploadID, 2, offset, length, minio.ObjectInfo{}, minio.ObjectOptions{}, minio.ObjectOptions{})
		if expectFailure {
			require.Error(t, err)
			return
		}
		require.NoError(t, err)

		require.Equal(t, 2, info.PartNumber)
		if length >= 0 {
			require.Equal(t, md5Hex(data[offset:offset+length]), info.ETag)
			require.Equal(t, length, info.Size)
			require.Equal(t, length, info.ActualSize)
		} else {
			require.Equal(t, md5Hex(data[offset:]), info.ETag)
			require.Equal(t, contentLength.Int64()-offset, info.Size)
			require.Equal(t, contentLength.Int64()-offset, info.ActualSize)
		}
	})
}
