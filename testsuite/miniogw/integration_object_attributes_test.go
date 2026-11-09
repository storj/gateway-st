// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"net/http"
	"os"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"

	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
)

func TestObjectAttributes(t *testing.T) {
	runIntegrationTest(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 1,
		UplinkCount:      1,
	}, func(ctx *testcontext.Context, t *testing.T, planet *testplanet.Planet, gw *testGateway) {
		client := gw.client(t)

		bucket := testrand.BucketName()
		require.NoError(t, createBucket(ctx, client, bucket, true, false))

		unversionedBucket := testrand.BucketName()
		require.NoError(t, createBucket(ctx, client, unversionedBucket, false, false))

		testFilePath := ctx.File("random1.dat")
		require.NoError(t, os.WriteFile(testFilePath, testrand.Bytes(123), 0600))

		testFile, err := os.Open(testFilePath)
		require.NoError(t, err)

		putResp, err := putObject(ctx, client, bucket, "object1", testFile)
		require.NoError(t, err)

		_, err = putObject(ctx, client, unversionedBucket, "object1", testFile)
		require.NoError(t, err)

		_, err = getObjectAttributes(ctx, client, bucket, "objectnonexistent", "", []types.ObjectAttributes{
			"ETag",
		})
		requireS3Error(t, err, http.StatusNotFound, "NoSuchKey")

		_, err = getObjectAttributes(ctx, client, bucket, "object1", "abc123", []types.ObjectAttributes{
			"ETag",
		})
		requireS3Error(t, err, http.StatusBadRequest, "InvalidArgument")

		_, err = getObjectAttributes(ctx, client, bucket, "object1", "", []types.ObjectAttributes{
			"Invalid",
		})
		requireS3Error(t, err, http.StatusBadRequest, "InvalidArgument")

		attrResp, err := getObjectAttributes(ctx, client, unversionedBucket, "object1", "", []types.ObjectAttributes{
			"ObjectSize",
		})
		require.NoError(t, err)
		require.Empty(t, attrResp.VersionId)

		attrResp, err = getObjectAttributes(ctx, client, bucket, "object1", "", []types.ObjectAttributes{
			"ETag",
			"ObjectSize",
			"StorageClass",
		})
		require.NoError(t, err)

		require.Equal(t, putResp.VersionId, attrResp.VersionId)

		// ETag should really be double quoted everywhere by the spec, but GetObjectAttributes
		// is inconsistent with other S3 APIs that do return it quoted.
		require.Equal(t, regexp.MustCompile(`^"(.*)"$`).ReplaceAllString(*putResp.ETag, `$1`), *attrResp.ETag)
		require.Equal(t, new(int64(123)), attrResp.ObjectSize)
		require.Equal(t, types.StorageClassStandard, attrResp.StorageClass)
		require.Empty(t, attrResp.DeleteMarker)

		attrResp, err = getObjectAttributes(ctx, client, bucket, "object1", *putResp.VersionId, []types.ObjectAttributes{
			"ObjectSize",
		})
		require.NoError(t, err)

		require.Equal(t, putResp.VersionId, attrResp.VersionId)
		require.Equal(t, new(int64(123)), attrResp.ObjectSize)
		require.Empty(t, attrResp.ETag)
		require.Empty(t, attrResp.StorageClass)
		require.Empty(t, attrResp.DeleteMarker)
	})
}
