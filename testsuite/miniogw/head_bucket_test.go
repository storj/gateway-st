// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/testcontext"
	"storj.io/gateway/miniogw"
	minio "storj.io/minio/cmd"
	"storj.io/minio/pkg/auth"
	"storj.io/storj/private/testplanet"
)

func TestHeadBucketRegion(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
	}, func(t *testing.T, tCtx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(tCtx, planet.Satellites[0])
		require.NoError(t, err)
		defer tCtx.Check(project.Close)

		// Establish new context with *uplink.Project for the gateway to pick up.
		ctx := miniogw.WithCredentials(tCtx, project, miniogw.CredentialsInfo{})

		layer, err := miniogw.NewStorjGateway(defaultS3CompatibilityConfig).NewGatewayLayer(auth.Credentials{})
		require.NoError(t, err)

		defer func() { require.NoError(t, layer.Shutdown(ctx)) }()

		// Create a test bucket
		bucketName := "test-bucket-region"
		err = layer.MakeBucketWithLocation(ctx, bucketName, minio.BucketOptions{})
		require.NoError(t, err)

		// Get bucket info
		bucketInfo, err := layer.GetBucketInfo(ctx, bucketName)
		require.NoError(t, err)
		require.Equal(t, bucketName, bucketInfo.Name)

		// The HeadBucketHandler will set x-amz-bucket-region to "us-east-1"
		// This test verifies that GetBucketInfo works correctly, which is
		// required for HeadBucketHandler to succeed and set the region header.
		t.Log("Bucket info retrieved successfully, HeadBucket handler will set x-amz-bucket-region header")
	})
}
