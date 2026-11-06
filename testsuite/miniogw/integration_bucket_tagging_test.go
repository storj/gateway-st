// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"net/http"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite"
	"storj.io/storj/satellite/buckets"
)

func TestBucketTagging(t *testing.T) {
	runIntegrationTest(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.BucketTaggingEnabled = true
			},
		},
		NonParallel: true,
	}, func(ctx *testcontext.Context, t *testing.T, planet *testplanet.Planet, gw *testGateway) {
		sat := planet.Satellites[0]
		upl := planet.Uplinks[0]
		bucketsDB := sat.DB.Buckets()
		projectID := upl.Projects[0].ID

		project, err := upl.OpenProject(ctx, sat)
		require.NoError(t, err)

		defer ctx.Check(project.Close)

		client := gw.client(t)

		bucketName := testrand.BucketName()

		_, err = bucketsDB.CreateBucket(ctx, buckets.Bucket{
			ProjectID: projectID,
			Name:      bucketName,
		})
		require.NoError(t, err)

		requireTags := func(t *testing.T, bucketName string, expectedTags []*s3.Tag) {
			tagResp, err := client.GetBucketTaggingWithContext(ctx, &s3.GetBucketTaggingInput{
				Bucket: aws.String(bucketName),
			})
			require.NoError(t, err)

			// sort tags before comparing, as minio uses a map for tags which is not stable in sort.
			sort.Slice(expectedTags, func(i, j int) bool {
				return *expectedTags[i].Key < *expectedTags[j].Key
			})
			sort.Slice(tagResp.TagSet, func(i, j int) bool {
				return *tagResp.TagSet[i].Key < *tagResp.TagSet[j].Key
			})

			require.Equal(t, expectedTags, tagResp.TagSet)
		}

		t.Run("Non-existent bucket", func(t *testing.T) {
			_, err := client.PutBucketTaggingWithContext(ctx, &s3.PutBucketTaggingInput{
				Bucket: aws.String("non-existent-bucket"),
				Tagging: &s3.Tagging{
					TagSet: []*s3.Tag{},
				},
			})
			requireS3Error(t, err, http.StatusNotFound, "NoSuchBucket")
		})

		t.Run("No tags", func(t *testing.T) {
			_, err := client.PutBucketTaggingWithContext(ctx, &s3.PutBucketTaggingInput{
				Bucket: aws.String(bucketName),
				Tagging: &s3.Tagging{
					TagSet: []*s3.Tag{
						{
							Key:   aws.String("key1"),
							Value: aws.String("value1"),
						},
					},
				},
			})
			require.NoError(t, err)

			_, err = client.PutBucketTaggingWithContext(ctx, &s3.PutBucketTaggingInput{
				Bucket: aws.String(bucketName),
				Tagging: &s3.Tagging{
					TagSet: []*s3.Tag{},
				},
			})
			require.NoError(t, err)

			_, err = client.GetBucketTaggingWithContext(ctx, &s3.GetBucketTaggingInput{
				Bucket: aws.String(bucketName),
			})
			requireS3Error(t, err, http.StatusNotFound, "NoSuchTagSet")
		})

		t.Run("Delete tags", func(t *testing.T) {
			_, err := client.PutBucketTaggingWithContext(ctx, &s3.PutBucketTaggingInput{
				Bucket: aws.String(bucketName),
				Tagging: &s3.Tagging{
					TagSet: []*s3.Tag{
						{
							Key:   aws.String("key1"),
							Value: aws.String("value1"),
						},
					},
				},
			})
			require.NoError(t, err)

			_, err = client.DeleteBucketTaggingWithContext(ctx, &s3.DeleteBucketTaggingInput{
				Bucket: aws.String(bucketName),
			})
			require.NoError(t, err)

			_, err = client.GetBucketTaggingWithContext(ctx, &s3.GetBucketTaggingInput{
				Bucket: aws.String(bucketName),
			})
			requireS3Error(t, err, http.StatusNotFound, "NoSuchTagSet")

			_, err = client.DeleteBucketTaggingWithContext(ctx, &s3.DeleteBucketTaggingInput{
				Bucket: aws.String(bucketName),
			})
			require.NoError(t, err) // delete returns success even if there are no tags
		})

		t.Run("Basic", func(t *testing.T) {
			expectedTags := []*s3.Tag{
				{
					Key:   aws.String("abcdeABCDE01234+-./:=@_"),
					Value: aws.String("_@=:/.-+fghijFGHIJ56789"),
				},
				{
					Key:   aws.String(string([]rune{'Ա', 'א', 'ء', 'ऄ', 'ঀ', '٠', '०', '০'})),
					Value: aws.String(string([]rune{'ֆ', 'ת', 'ي', 'ह', 'হ', '٩', '९', '৯'})),
				},
				{
					Key:   aws.String("key"),
					Value: aws.String("value"),
				},
			}
			_, err := client.PutBucketTaggingWithContext(ctx, &s3.PutBucketTaggingInput{
				Bucket: aws.String(bucketName),
				Tagging: &s3.Tagging{
					TagSet: expectedTags,
				},
			})
			require.NoError(t, err)
			requireTags(t, bucketName, expectedTags)
		})

		t.Run("Tag key too long", func(t *testing.T) {
			_, err := client.PutBucketTaggingWithContext(ctx, &s3.PutBucketTaggingInput{
				Bucket: aws.String(bucketName),
				Tagging: &s3.Tagging{
					TagSet: []*s3.Tag{
						{
							Key:   aws.String(string(testrand.RandAlphaNumeric(129))),
							Value: aws.String("value"),
						},
					},
				},
			})
			requireS3Error(t, err, http.StatusBadRequest, "InvalidTag")
		})

		t.Run("Tag value too long", func(t *testing.T) {
			_, err := client.PutBucketTaggingWithContext(ctx, &s3.PutBucketTaggingInput{
				Bucket: aws.String(bucketName),
				Tagging: &s3.Tagging{
					TagSet: []*s3.Tag{
						{
							Key:   aws.String("key"),
							Value: aws.String(string(testrand.RandAlphaNumeric(257))),
						},
					},
				},
			})
			requireS3Error(t, err, http.StatusBadRequest, "InvalidTag")
		})

		t.Run("Duplicate tag key", func(t *testing.T) {
			_, err := client.PutBucketTaggingWithContext(ctx, &s3.PutBucketTaggingInput{
				Bucket: aws.String(bucketName),
				Tagging: &s3.Tagging{
					TagSet: []*s3.Tag{
						{
							Key:   aws.String("key1"),
							Value: aws.String("value1"),
						},
						{
							Key:   aws.String("key1"),
							Value: aws.String("value2"),
						},
					},
				},
			})
			requireS3Error(t, err, http.StatusBadRequest, "InvalidTag")
		})

		t.Run("Too many tags", func(t *testing.T) {
			var tooManyTags []*s3.Tag

			for range 51 {
				tooManyTags = append(tooManyTags, &s3.Tag{
					Key:   aws.String(string(testrand.RandAlphaNumeric(32))),
					Value: aws.String(string(testrand.RandAlphaNumeric(32))),
				})
			}

			_, err := client.PutBucketTaggingWithContext(ctx, &s3.PutBucketTaggingInput{
				Bucket: aws.String(bucketName),
				Tagging: &s3.Tagging{
					TagSet: tooManyTags,
				},
			})
			requireS3Error(t, err, http.StatusBadRequest, "BadRequest")
		})
	})
}
