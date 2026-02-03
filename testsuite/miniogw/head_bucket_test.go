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

// TestHeadBucketRegion tests that GetBucketInfo works correctly, which is the
// core dependency for the HeadBucketHandler. The HeadBucketHandler in the API
// package sets the x-amz-bucket-region header to "us-east-1" after successfully
// calling GetBucketInfo.
//
// Note: Full HTTP-level testing of the HeadBucket handler including header
// verification is done via integration tests (e.g., using rclone or AWS CLI).
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

		// Verify GetBucketInfo works - this is what HeadBucketHandler relies on
		bucketInfo, err := layer.GetBucketInfo(ctx, bucketName)
		require.NoError(t, err)
		require.Equal(t, bucketName, bucketInfo.Name)

		// If GetBucketInfo succeeds, HeadBucketHandler will:
		// 1. Return 200 OK
		// 2. Set the x-amz-bucket-region header to "us-east-1"
	})
}
