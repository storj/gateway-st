// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"

	"storj.io/common/errs2"
	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/shared/dbutil"
)

func TestConditionalWriteHeaders(t *testing.T) {
	runIntegrationTest(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 1,
		UplinkCount:      1,
	}, func(ctx *testcontext.Context, t *testing.T, planet *testplanet.Planet, gw *testGateway) {
		client := gw.client(t)

		unversionedBucket, versionedBucket := testrand.BucketName(), testrand.BucketName()
		require.NoError(t, createBucket(ctx, client, unversionedBucket, false, false))
		require.NoError(t, createBucket(ctx, client, versionedBucket, true, false))

		runSubTest := func(name string, fn func(t *testing.T, bucket, key string)) {
			for _, tc := range []struct {
				name, bucket string
			}{
				{name: "unversioned bucket", bucket: unversionedBucket},
				{name: "versioned bucket", bucket: versionedBucket},
			} {
				t.Run(fmt.Sprintf("%s %s", name, tc.name), func(t *testing.T) {
					// todo: testrand.Path() throws a signature validation error on gateway (see https://github.com/storj/edge/issues/572)
					fn(t, tc.bucket, string(testrand.RandAlphaNumeric(16)))
				})
			}
		}

		runSubTestWithBody := func(name string, fn func(t *testing.T, bucket, key string, body io.ReadSeeker)) {
			runSubTest(name, func(t *testing.T, bucket, key string) {
				for _, tc := range []struct {
					name string
					body io.ReadSeeker
				}{
					{name: "inline", body: bytes.NewReader(testrand.Bytes(100 * memory.B))},
					{name: "non-inline", body: bytes.NewReader(testrand.Bytes(5 * memory.MiB))},
				} {
					t.Run(tc.name, func(t *testing.T) {
						// todo: testrand.Path() throws a signature validation error on gateway (see https://github.com/storj/edge/issues/572)
						fn(t, bucket, string(testrand.RandAlphaNumeric(16)), tc.body)
					})
				}
			})
		}

		doPutRequest := func(ctx context.Context, bucket, key string, body io.ReadSeeker, ifNoneMatch string) error {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      new(bucket),
				Key:         new(key),
				Body:        body,
				IfNoneMatch: new(ifNoneMatch),
			})
			return err
		}

		doCopyRequest := func(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string, ifNoneMatch string) error {
			_, err := client.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:      new(dstBucket),
				Key:         new(dstKey),
				CopySource:  new(srcBucket + "/" + srcKey),
				IfNoneMatch: new(ifNoneMatch),
			})
			return err
		}

		newUpload := func(bucket, key string, body io.ReadSeeker) (*string, []types.CompletedPart) {
			uploadResp, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
				Bucket: new(bucket),
				Key:    new(key),
			})
			require.NoError(t, err)

			partResp, err := client.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:     new(bucket),
				Key:        new(key),
				PartNumber: new(int32(1)),
				UploadId:   uploadResp.UploadId,
				Body:       body,
			})
			require.NoError(t, err)

			return uploadResp.UploadId, []types.CompletedPart{
				{
					PartNumber: new(int32(1)),
					ETag:       partResp.ETag,
				},
			}
		}

		completeUpload := func(bucket, key string, uploadID *string, completedParts []types.CompletedPart, ifNoneMatch string) error {
			_, err := client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
				Bucket:   new(bucket),
				Key:      new(key),
				UploadId: uploadID,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: completedParts,
				},
				IfNoneMatch: new(ifNoneMatch),
			})
			return err
		}

		runSubTest("Unimplemented PutObject", func(t *testing.T, bucket, key string) {
			requireS3Error(t, doPutRequest(ctx, bucket, key, nil, "something"), http.StatusNotImplemented, "NotImplemented")
		})

		runSubTest("Unimplemented CopyObject", func(t *testing.T, bucket, key string) {
			require.NoError(t, doPutRequest(ctx, bucket, key, nil, "*"))
			requireS3Error(t, doCopyRequest(ctx, bucket, key, bucket, key+"-copy", "something"), http.StatusNotImplemented, "NotImplemented")
		})

		runSubTest("Unimplemented CompleteMultipartUpload", func(t *testing.T, bucket, key string) {
			uploadID, completedParts := newUpload(bucket, key, nil)
			requireS3Error(t, completeUpload(bucket, key, uploadID, completedParts, "something"), http.StatusNotImplemented, "NotImplemented")
		})

		runSubTestWithBody("PutObject", func(t *testing.T, bucket, key string, body io.ReadSeeker) {
			require.NoError(t, doPutRequest(ctx, bucket, key, body, "*"))
			requireS3Error(t, doPutRequest(ctx, bucket, key, body, "*"), http.StatusPreconditionFailed, "PreconditionFailed")
			require.NoError(t, deleteObject(ctx, client, bucket, key, ""))
			require.NoError(t, doPutRequest(ctx, bucket, key, body, "*"))
		})

		runSubTest("PutObject concurrent", func(t *testing.T, bucket, key string) {
			for _, impl := range planet.Satellites[0].DB.Testing().Implementation() {
				if impl == dbutil.Postgres {
					t.Skip("todo: flaky test")
				}
			}

			var group errs2.Group
			group.Go(func() error {
				return doPutRequest(ctx, bucket, key, bytes.NewReader(testrand.Bytes(100*memory.B)), "*")
			})
			group.Go(func() error {
				return doPutRequest(ctx, bucket, key, bytes.NewReader(testrand.Bytes(100*memory.B)), "*")
			})
			errs := group.Wait()
			require.Len(t, errs, 1)
			requireS3Error(t, errs[0], http.StatusPreconditionFailed, "PreconditionFailed")
		})

		runSubTestWithBody("CopyObject", func(t *testing.T, bucket, key string, body io.ReadSeeker) {
			srcKey, dstKey := key, key+"-copy"

			require.NoError(t, doPutRequest(ctx, bucket, srcKey, body, "*"))
			require.NoError(t, doCopyRequest(ctx, bucket, srcKey, bucket, dstKey, "*"))
			requireS3Error(t, doCopyRequest(ctx, bucket, srcKey, bucket, dstKey, "*"), http.StatusPreconditionFailed, "PreconditionFailed")
			require.NoError(t, deleteObject(ctx, client, bucket, dstKey, ""))
			require.NoError(t, doCopyRequest(ctx, bucket, srcKey, bucket, dstKey, "*"))
		})

		runSubTest("CompleteMultipartUpload", func(t *testing.T, bucket, key string) {
			uploadID, completedParts := newUpload(bucket, key, nil)
			require.NoError(t, completeUpload(bucket, key, uploadID, completedParts, "*"))

			uploadID, completedParts = newUpload(bucket, key, nil)
			requireS3Error(t, completeUpload(bucket, key, uploadID, completedParts, "*"), http.StatusPreconditionFailed, "PreconditionFailed")

			require.NoError(t, deleteObject(ctx, client, bucket, key, ""))

			uploadID, completedParts = newUpload(bucket, key, nil)
			require.NoError(t, completeUpload(bucket, key, uploadID, completedParts, "*"))
		})

		runSubTest("CompleteMultipartUpload concurrent", func(t *testing.T, bucket, key string) {
			for _, impl := range planet.Satellites[0].DB.Testing().Implementation() {
				if impl == dbutil.Postgres {
					t.Skip("todo: flaky test")
				}
			}

			uploadID1, completedParts1 := newUpload(bucket, key, nil)
			uploadID2, completedParts2 := newUpload(bucket, key, nil)

			var group errs2.Group
			group.Go(func() error { return completeUpload(bucket, key, uploadID1, completedParts1, "*") })
			group.Go(func() error { return completeUpload(bucket, key, uploadID2, completedParts2, "*") })
			errs := group.Wait()
			require.Len(t, errs, 1)
			requireS3Error(t, errs[0], http.StatusPreconditionFailed, "PreconditionFailed")
		})
	})
}
