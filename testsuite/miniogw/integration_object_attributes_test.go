// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"bytes"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
)

func TestObjectAttributes(t *testing.T) {
	runIntegrationTest(t, checksumsPlanet, func(ctx *testcontext.Context, t *testing.T, planet *testplanet.Planet, gw *testGateway) {
		client := gw.client(t)

		bucket := testrand.BucketName()
		require.NoError(t, createBucket(ctx, client, bucket, true, false))

		unversionedBucket := testrand.BucketName()
		require.NoError(t, createBucket(ctx, client, unversionedBucket, false, false))

		testFilePath := ctx.File("random1.dat")
		data := testrand.Bytes(123)
		require.NoError(t, os.WriteFile(testFilePath, data, 0600))

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

		// Over plain HTTP the SDK sends no checksum, so the gateway's default applies.
		attrResp, err = getObjectAttributes(ctx, client, bucket, "object1", "", []types.ObjectAttributes{
			"Checksum",
			"ObjectParts",
		})
		require.NoError(t, err)
		require.Nil(t, attrResp.ObjectParts)
		if gw.kind == binaryGateway {
			// minio's handlers don't report checksums.
			require.Nil(t, attrResp.Checksum)
		} else {
			require.Equal(t, &types.Checksum{
				ChecksumCRC64NVME: new(checksumOf(types.ChecksumAlgorithmCrc64nvme, data)),
				ChecksumType:      types.ChecksumTypeFullObject,
			}, attrResp.Checksum)
		}

		parts := [][]byte{testrand.BytesInt(5 * memory.MiB.Int()), testrand.BytesInt(123)}
		completeResp, err := putObjectMultipart(ctx, client, &s3.CreateMultipartUploadInput{
			Bucket:            new(bucket),
			Key:               new("multipart"),
			ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
		}, parts...)
		require.NoError(t, err)

		attrResp, err = getObjectAttributes(ctx, client, bucket, "multipart", "", []types.ObjectAttributes{
			"Checksum",
			"ObjectParts",
			"ObjectSize",
		})
		require.NoError(t, err)
		require.Equal(t, new(int64(5*memory.MiB+123)), attrResp.ObjectSize)
		if gw.kind == binaryGateway {
			// minio's handlers don't report checksums or parts.
			require.Nil(t, attrResp.Checksum)
			require.Nil(t, attrResp.ObjectParts)
			return
		}

		// Unlike everywhere else, the composite checksum leaves out the part count.
		composite, _, _ := strings.Cut(aws.ToString(completeResp.ChecksumCRC32), "-")
		require.Equal(t, &types.Checksum{
			ChecksumCRC32: new(composite),
			ChecksumType:  types.ChecksumTypeComposite,
		}, attrResp.Checksum)
		require.NotNil(t, attrResp.ObjectParts)
		require.Equal(t, new(int32(2)), attrResp.ObjectParts.TotalPartsCount)
		require.False(t, aws.ToBool(attrResp.ObjectParts.IsTruncated))
		require.Len(t, attrResp.ObjectParts.Parts, 2)
		for i, part := range attrResp.ObjectParts.Parts {
			require.Equal(t, new(int32(i+1)), part.PartNumber)
			require.Equal(t, new(int64(len(parts[i]))), part.Size)
			require.Equal(t, new(checksumOf(types.ChecksumAlgorithmCrc32, parts[i])), part.ChecksumCRC32)
		}

		attrResp, err = client.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
			Bucket:           new(bucket),
			Key:              new("multipart"),
			ObjectAttributes: []types.ObjectAttributes{"ObjectParts"},
			MaxParts:         new(int32(1)),
		})
		require.NoError(t, err)
		require.NotNil(t, attrResp.ObjectParts)
		require.True(t, aws.ToBool(attrResp.ObjectParts.IsTruncated))
		require.Equal(t, new("1"), attrResp.ObjectParts.NextPartNumberMarker)
		require.Len(t, attrResp.ObjectParts.Parts, 1)

		// Like S3, a multipart upload with a full object checksum doesn't list its parts.
		_, err = putObjectMultipart(ctx, client, &s3.CreateMultipartUploadInput{
			Bucket:            new(bucket),
			Key:               new("multipart-full-object"),
			ChecksumAlgorithm: types.ChecksumAlgorithmCrc64nvme,
		}, parts...)
		require.NoError(t, err)

		attrResp, err = getObjectAttributes(ctx, client, bucket, "multipart-full-object", "", []types.ObjectAttributes{
			"Checksum",
			"ObjectParts",
		})
		require.NoError(t, err)
		require.Equal(t, &types.Checksum{
			ChecksumCRC64NVME: new(checksumOf(types.ChecksumAlgorithmCrc64nvme, bytes.Join(parts, nil))),
			ChecksumType:      types.ChecksumTypeFullObject,
		}, attrResp.Checksum)
		require.Nil(t, attrResp.ObjectParts)
	})
}
