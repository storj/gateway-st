// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"storj.io/common/memory"
	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/minio/pkg/bucket/versioning"
	"storj.io/storj/private/testplanet"
)

func TestVersioning(t *testing.T) {
	runIntegrationTest(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.DefaultPathCipher = storj.EncNull
			},
		},
		NonParallel: true,
	}, func(ctx *testcontext.Context, t *testing.T, planet *testplanet.Planet, gw *testGateway) {
		client := gw.client(t)

		getBucketVersioning := func(bucket string) (string, error) {
			resp, err := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: new(bucket)})
			if err != nil {
				return "", err
			}
			return string(resp.Status), nil
		}

		t.Run("bucket versioning enabling-disabling", func(t *testing.T) {
			bucketA := testrand.BucketName()
			require.NoError(t, createBucket(ctx, client, bucketA, false, false))

			v, err := getBucketVersioning(bucketA)
			require.NoError(t, err)
			require.Empty(t, v)

			// try to change bucket state unversioned -> enabled -> suspended
			require.NoError(t, putBucketVersioning(ctx, client, bucketA, types.BucketVersioningStatusEnabled))

			v, err = getBucketVersioning(bucketA)
			require.NoError(t, err)
			require.EqualValues(t, versioning.Enabled, v)

			require.NoError(t, putBucketVersioning(ctx, client, bucketA, types.BucketVersioningStatusSuspended))

			v, err = getBucketVersioning(bucketA)
			require.NoError(t, err)
			require.EqualValues(t, versioning.Suspended, v)

			bucketB := testrand.BucketName()
			require.NoError(t, createBucket(ctx, client, bucketB, false, false))

			// try to change bucket state unversioned -> suspended
			require.NoError(t, putBucketVersioning(ctx, client, bucketB, types.BucketVersioningStatusSuspended))
			v, err = getBucketVersioning(bucketB)
			require.NoError(t, err)
			require.EqualValues(t, versioning.Suspended, v)
		})

		t.Run("check VersionID support for different methods", func(t *testing.T) {
			bucket := testrand.BucketName()

			require.NoError(t, createBucket(ctx, client, bucket, false, false))
			require.NoError(t, putBucketVersioning(ctx, client, bucket, types.BucketVersioningStatusEnabled))

			// upload first version
			expectedContentA1 := testrand.Bytes(5 * memory.KiB)
			uploadInfo, err := putObject(ctx, client, bucket, "objectA", bytes.NewReader(expectedContentA1))
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(uploadInfo.VersionId))

			objectA1VersionID := aws.ToString(uploadInfo.VersionId)

			statInfo, err := headObject(ctx, client, bucket, "objectA", "")
			require.NoError(t, err)
			require.Equal(t, objectA1VersionID, aws.ToString(statInfo.VersionId))

			// the same request but with VersionID specified
			statInfo, err = headObject(ctx, client, bucket, "objectA", objectA1VersionID)
			require.NoError(t, err)
			require.Equal(t, objectA1VersionID, aws.ToString(statInfo.VersionId))

			tags := map[string]string{
				"key1": "tag1",
			}
			_, err = client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
				Bucket:  new(bucket),
				Key:     new("objectA"),
				Tagging: &types.Tagging{TagSet: []types.Tag{{Key: new("key1"), Value: new("tag1")}}},
			})
			require.NoError(t, err)

			// upload second version
			expectedContentA2 := testrand.Bytes(5 * memory.KiB)
			uploadInfo, err = putObject(ctx, client, bucket, "objectA", bytes.NewReader(expectedContentA2))
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(uploadInfo.VersionId))

			objectA2VersionID := aws.ToString(uploadInfo.VersionId)

			statInfo, err = headObject(ctx, client, bucket, "objectA", "")
			require.NoError(t, err)
			require.Equal(t, objectA2VersionID, aws.ToString(statInfo.VersionId))

			// the same request but with VersionID specified
			statInfo, err = headObject(ctx, client, bucket, "objectA", objectA2VersionID)
			require.NoError(t, err)
			require.Equal(t, objectA2VersionID, aws.ToString(statInfo.VersionId))

			// // check that we have two different versions
			contentA1, err := getObjectData(ctx, client, bucket, "objectA", objectA1VersionID)
			require.NoError(t, err)
			require.Equal(t, expectedContentA1, contentA1)

			contentA2, err := getObjectData(ctx, client, bucket, "objectA", objectA2VersionID)
			require.NoError(t, err)
			require.Equal(t, expectedContentA2, contentA2)

			tagsInfo, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
				Bucket:    new(bucket),
				Key:       new("objectA"),
				VersionId: new(objectA1VersionID),
			})
			require.NoError(t, err)
			require.EqualValues(t, tags, tagSetToMap(tagsInfo.TagSet))

			// TODO(ver): add test for setting tag for specific version when implemented
		})

		t.Run("check VersionID while completing multipart upload", func(t *testing.T) {
			bucket := testrand.BucketName()

			require.NoError(t, createBucket(ctx, client, bucket, false, false))
			require.NoError(t, putBucketVersioning(ctx, client, bucket, types.BucketVersioningStatusEnabled))

			expectedContent := testrand.Bytes(500 * memory.KiB)
			uploadInfo, err := putObjectMultipart(ctx, client, &s3.CreateMultipartUploadInput{
				Bucket: new(bucket),
				Key:    new("objectA"),
			}, expectedContent)
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(uploadInfo.VersionId))
		})

		t.Run("check VersionID with delete object and delete objects", func(t *testing.T) {
			bucket := testrand.BucketName()

			require.NoError(t, createBucket(ctx, client, bucket, false, false))
			require.NoError(t, putBucketVersioning(ctx, client, bucket, types.BucketVersioningStatusEnabled))

			var versionIDs []string

			for range 5 {
				expectedContent := testrand.Bytes(5 * memory.KiB)
				uploadInfo, err := putObject(ctx, client, bucket, "objectA", bytes.NewReader(expectedContent))
				require.NoError(t, err)
				require.NotEmpty(t, aws.ToString(uploadInfo.VersionId))
				versionIDs = append(versionIDs, aws.ToString(uploadInfo.VersionId))
			}

			require.NoError(t, deleteObject(ctx, client, bucket, "objectA", versionIDs[0]))

			listing, err := listObjectVersions(ctx, client, &s3.ListObjectVersionsInput{Bucket: new(bucket)})
			require.NoError(t, err)
			for _, listed := range listing {
				if listed.VersionID == versionIDs[len(versionIDs)-1] {
					require.True(t, listed.IsLatest)
				} else {
					require.False(t, listed.IsLatest)
				}
			}

			expected := make([]removeObjectResult, 0, len(versionIDs)-1)
			for i := 1; i < len(versionIDs); i++ {
				expected = append(expected, removeObjectResult{
					ObjectName:      "objectA",
					ObjectVersionID: versionIDs[i],
					DeleteMarker:    false,
				})
			}

			removed := make([]removeObjectResult, 0, len(versionIDs)-1)
			removed = append(removed, deleteObjects(ctx, client, bucket, "objectA", versionIDs[1:])...)

			require.ElementsMatch(t, expected, removed)

			listing, err = listObjectVersions(ctx, client, &s3.ListObjectVersionsInput{Bucket: new(bucket)})
			require.NoError(t, err)
			for range listing {
				require.Fail(t, "no objects to list")
			}

			// create one object and one delete marker
			_, err = putObjectMultipart(ctx, client, &s3.CreateMultipartUploadInput{
				Bucket: new(bucket),
				Key:    new("objectA"),
			}, testrand.Bytes(5*memory.KiB))
			require.NoError(t, err)

			require.NoError(t, deleteObject(ctx, client, bucket, "objectA", ""))

			removed = nil
			removed = append(removed, deleteObjects(ctx, client, bucket, "objectA", []string{""})...)

			require.Len(t, removed, 1)
			require.NoError(t, removed[0].Err)
			require.Equal(t, "objectA", removed[0].ObjectName)
			require.Empty(t, removed[0].ObjectVersionID)
			require.True(t, removed[0].DeleteMarker)
			require.NotEmpty(t, removed[0].DeleteMarkerVersionID)

			var objects []listedVersion
			versionIDs = nil
			versionIDOrder := make(map[string]int)

			listing, err = listObjectVersions(ctx, client, &s3.ListObjectVersionsInput{Bucket: new(bucket)})
			require.NoError(t, err)
			for _, obj := range listing {
				objects = append(objects, obj)
				versionIDOrder[obj.VersionID] = len(versionIDs)
				versionIDs = append(versionIDs, obj.VersionID)
			}

			require.Len(t, objects, 3)
			require.Equal(t, removed[0].DeleteMarkerVersionID, objects[0].VersionID)
			require.True(t, objects[0].IsLatest)

			removed = []removeObjectResult{}
			removed = append(removed, deleteObjects(ctx, client, bucket, "objectA", versionIDs)...)

			sort.Slice(removed, func(i, j int) bool {
				ordi, ordiOk := versionIDOrder[removed[i].ObjectVersionID]
				ordj, ordjOk := versionIDOrder[removed[j].ObjectVersionID]
				if ordiOk {
					if ordjOk {
						return ordi < ordj
					}
					return true
				}
				return false
			})

			require.Len(t, removed, 3)

			for i := range 2 {
				result := removed[i]
				require.NoError(t, result.Err)
				require.Equal(t, "objectA", result.ObjectName)
				require.True(t, result.DeleteMarker)
				require.Equal(t, objects[i].VersionID, result.DeleteMarkerVersionID)
			}

			result := removed[2]
			require.NoError(t, result.Err)
			require.Equal(t, "objectA", result.ObjectName)
			require.False(t, result.DeleteMarker)
			require.Empty(t, result.DeleteMarkerVersionID)
		})

		t.Run("ListObjectVersions", func(t *testing.T) {
			bucketA := testrand.BucketName()
			bucketB := testrand.BucketName()
			bucketC := testrand.BucketName()

			require.NoError(t, createBucket(ctx, client, bucketA, false, false))
			require.NoError(t, putBucketVersioning(ctx, client, bucketA, types.BucketVersioningStatusEnabled))

			require.NoError(t, createBucket(ctx, client, bucketB, false, false))
			require.NoError(t, putBucketVersioning(ctx, client, bucketB, types.BucketVersioningStatusEnabled))

			require.NoError(t, createBucket(ctx, client, bucketC, false, false))
			require.NoError(t, putBucketVersioning(ctx, client, bucketC, types.BucketVersioningStatusEnabled))

			listing, err := listObjectVersions(ctx, client, &s3.ListObjectVersionsInput{Bucket: new(bucketA)})
			require.NoError(t, err)
			for range listing {
				require.Fail(t, "no objects to list")
			}

			listing, err = listObjectVersions(ctx, client, &s3.ListObjectVersionsInput{Bucket: new(bucketB)})
			require.NoError(t, err)
			for range listing {
				require.Fail(t, "no objects to list")
			}

			listing, err = listObjectVersions(ctx, client, &s3.ListObjectVersionsInput{Bucket: new(bucketC)})
			require.NoError(t, err)
			for range listing {
				require.Fail(t, "no objects to list")
			}

			expectedContent := testrand.Bytes(5 * memory.KiB)

			type uploadEntry struct {
				key    string
				action string
			}

			type objectVersion struct {
				key       string
				versionID string
			}

			type testCase struct {
				name      string
				prefix    string
				recursive bool
				versions  []objectVersion
			}

			upload := func(t *testing.T, bucket string, entries []uploadEntry) map[string][]string {
				versions := map[string][]string{}
				for _, upload := range entries {
					if upload.action == "deletemarker" {
						require.NoError(t, deleteObject(ctx, client, bucket, upload.key, ""))
						// The listing is only checked to have a delete marker here, not its version ID.
						versions[upload.key] = append(versions[upload.key], "deletemarker")
					} else {
						uploadInfo, err := putObject(ctx, client, bucket, upload.key, bytes.NewReader(expectedContent))
						require.NoError(t, err)
						versions[upload.key] = append(versions[upload.key], aws.ToString(uploadInfo.VersionId))
					}
				}
				for k := range versions {
					slices.Reverse(versions[k])
				}
				return versions
			}

			// TODO test more edge cases, such as listing from a key and version ID marker.

			checkListing := func(t *testing.T, bucket string, testCases []testCase) {
				for _, tc := range testCases {
					t.Run(tc.name, func(t *testing.T) {
						for _, maxKeys := range []int32{0, 1, 2, 1000} {
							input := s3.ListObjectVersionsInput{
								Bucket: new(bucket),
								Prefix: new(tc.prefix),
							}
							if !tc.recursive {
								input.Delimiter = new("/")
							}
							if maxKeys > 0 {
								input.MaxKeys = new(maxKeys)
							}
							listing, err := listObjectVersions(ctx, client, &input)
							require.NoError(t, err)

							count := 0
							for _, objectInfo := range listing {

								// TODO(ver) check IsLatest field when it will be implemented

								version := tc.versions[count]
								require.Equal(t, version.key, objectInfo.Key)
								switch {
								case version.versionID == "prefix":
									require.True(t, strings.HasSuffix(objectInfo.Key, "/"))
								case version.versionID == "deletemarker":
									require.True(t, objectInfo.IsDeleteMarker)
								default:
									require.Equal(t, version.versionID, objectInfo.VersionID, "key %s", objectInfo.Key)
								}
								count++
							}
							require.Equal(t, len(tc.versions), count)
						}
					})
				}
			}

			t.Run("A", func(t *testing.T) {
				versions := upload(t, bucketA, []uploadEntry{
					{key: "objectA"},
					{key: "objectA", action: "deletemarker"},
					{key: "objectA"},
					{key: "objectB"},
					{key: "objectA", action: "deletemarker"},
					{key: "xprefix/objectC"},
					{key: "objectA"},
				})

				checkListing(t, bucketA, []testCase{
					{"empty prefix", "", false, []objectVersion{
						{"objectA", versions["objectA"][0]},
						{"objectA", versions["objectA"][1]},
						{"objectA", versions["objectA"][2]},
						{"objectA", versions["objectA"][3]},
						{"objectA", versions["objectA"][4]},
						{"objectB", versions["objectB"][0]},
						{"xprefix/", "prefix"},
					}},
					{"objectA", "objectA", false, []objectVersion{
						{"objectA", versions["objectA"][0]},
						{"objectA", versions["objectA"][1]},
						{"objectA", versions["objectA"][2]},
						{"objectA", versions["objectA"][3]},
						{"objectA", versions["objectA"][4]},
					}},
					{"objectB", "objectB", false, []objectVersion{
						{"objectB", versions["objectB"][0]},
					}},
					{"xprefix/objectC", "xprefix/", false, []objectVersion{
						{"xprefix/objectC", versions["xprefix/objectC"][0]},
					}},
					{"xprefix/recursive", "xprefix/", true, []objectVersion{
						{"xprefix/objectC", versions["xprefix/objectC"][0]},
					}},
					{"empty-prefix/recursive", "", true, []objectVersion{
						{"objectA", versions["objectA"][0]},
						{"objectA", versions["objectA"][1]},
						{"objectA", versions["objectA"][2]},
						{"objectA", versions["objectA"][3]},
						{"objectA", versions["objectA"][4]},
						{"objectB", versions["objectB"][0]},
						{"xprefix/objectC", versions["xprefix/objectC"][0]},
					}},
				})
			})

			t.Run("B", func(t *testing.T) {
				versions := upload(t, bucketB, []uploadEntry{
					{key: "prefixO/O"},
					{key: "prefixZ/Z"},
					{key: "prefixZ/Z"},
					{key: "prefixD/D"},
					{key: "prefixD/D"},
					{key: "prefixD/D", action: "deletemarker"},
					{key: "prefixC/C"},

					{key: "aprefix/0"},
					{key: "aprefix/1"},
					{key: "aprefix/2"},
				})

				checkListing(t, bucketB, []testCase{
					{"empty prefix", "", false, []objectVersion{
						{"aprefix/", "prefix"},
						{"prefixC/", "prefix"},
						{"prefixD/", "prefix"},
						{"prefixO/", "prefix"},
						{"prefixZ/", "prefix"},
					}},
					{"prefixD/D", "prefixD/D", false, []objectVersion{
						{"prefixD/D", versions["prefixD/D"][0]},
						{"prefixD/D", versions["prefixD/D"][1]},
						{"prefixD/D", versions["prefixD/D"][2]},
					}},
					{"aprefix/", "aprefix/", false, []objectVersion{
						{"aprefix/0", versions["aprefix/0"][0]},
						{"aprefix/1", versions["aprefix/1"][0]},
						{"aprefix/2", versions["aprefix/2"][0]},
					}},
				})
			})

			t.Run("specific to single object optimization", func(t *testing.T) {
				versions := upload(t, bucketC, []uploadEntry{
					{key: "p/a"},
					{key: "p/b"},
					{key: "p/b"},
					{key: "p/aa"},
					{key: "p/aaa"},
					{key: "a"},
					{key: "p/aaa"},
					{key: "p/aa"},
					{key: "p/a"},
					{key: "p/a"},
					{key: "a"},
					{key: "p/a", action: "deletemarker"},
					{key: "p/aaa", action: "deletemarker"},
				})

				checkListing(t, bucketC, []testCase{
					{"p/a as a single object", "p/a", true, []objectVersion{
						{"p/a", versions["p/a"][0]},
						{"p/a", versions["p/a"][1]},
						{"p/a", versions["p/a"][2]},
						{"p/a", versions["p/a"][3]},
						{"p/aa", versions["p/aa"][0]},
						{"p/aa", versions["p/aa"][1]},
						{"p/aaa", versions["p/aaa"][0]},
						{"p/aaa", versions["p/aaa"][1]},
						{"p/aaa", versions["p/aaa"][2]},
					}},
					{"p/aa as a single object", "p/aa", true, []objectVersion{
						{"p/aa", versions["p/aa"][0]},
						{"p/aa", versions["p/aa"][1]},
						{"p/aaa", versions["p/aaa"][0]},
						{"p/aaa", versions["p/aaa"][1]},
						{"p/aaa", versions["p/aaa"][2]},
					}},
					{"p/aaa as a single object", "p/aaa", true, []objectVersion{
						{"p/aaa", versions["p/aaa"][0]},
						{"p/aaa", versions["p/aaa"][1]},
						{"p/aaa", versions["p/aaa"][2]},
					}},

					{"p/aaaa as a single object", "p/aaaa", true, nil},

					{"p/b as a single object", "p/b", true, []objectVersion{
						{"p/b", versions["p/b"][0]},
						{"p/b", versions["p/b"][1]},
					}},

					{"p/bb as a single object", "p/bb", true, nil},

					{"p as a single object", "p", true, []objectVersion{
						{"p/a", versions["p/a"][0]},
						{"p/a", versions["p/a"][1]},
						{"p/a", versions["p/a"][2]},
						{"p/a", versions["p/a"][3]},
						{"p/aa", versions["p/aa"][0]},
						{"p/aa", versions["p/aa"][1]},
						{"p/aaa", versions["p/aaa"][0]},
						{"p/aaa", versions["p/aaa"][1]},
						{"p/aaa", versions["p/aaa"][2]},
						{"p/b", versions["p/b"][0]},
						{"p/b", versions["p/b"][1]},
					}},

					{"a as a single object", "a", true, []objectVersion{
						{"a", versions["a"][0]},
						{"a", versions["a"][1]},
					}},

					{"aa as a single object", "aa", true, nil},
				})
			})
		})

		t.Run("checks MethodNotAllowed error on retrieve (head/download) the object using its delete marker", func(t *testing.T) {
			bucket := testrand.BucketName()

			require.NoError(t, createBucket(ctx, client, bucket, false, false))
			require.NoError(t, putBucketVersioning(ctx, client, bucket, types.BucketVersioningStatusEnabled))

			expectedContent := testrand.Bytes(5 * memory.KiB)
			_, err := putObject(ctx, client, bucket, "objectA", bytes.NewReader(expectedContent))
			require.NoError(t, err)

			require.NoError(t, deleteObject(ctx, client, bucket, "objectA", ""))

			var deleteMarkerVersionID string
			listing, err := listObjectVersions(ctx, client, &s3.ListObjectVersionsInput{Bucket: new(bucket)})
			require.NoError(t, err)
			for _, objectInfo := range listing {
				if objectInfo.IsDeleteMarker {
					deleteMarkerVersionID = objectInfo.VersionID
				}
			}
			require.NotEmpty(t, deleteMarkerVersionID)

			_, err = headObject(ctx, client, bucket, "objectA", deleteMarkerVersionID)
			require.Error(t, err)
		})

		t.Run("CopyObject with source VersionID", func(t *testing.T) {
			bucket := testrand.BucketName()

			require.NoError(t, createBucket(ctx, client, bucket, false, false))
			require.NoError(t, putBucketVersioning(ctx, client, bucket, types.BucketVersioningStatusEnabled))

			expectedSize := 5 * memory.KiB
			expectedContent := testrand.Bytes(expectedSize)
			info, err := putObject(ctx, client, bucket, "objectA", bytes.NewReader(expectedContent))
			require.NoError(t, err)

			_, err = putObject(ctx, client, bucket, "objectA", bytes.NewReader(testrand.Bytes(expectedSize)))
			require.NoError(t, err)

			copyInfo, err := copyObject(ctx, client, bucket, "objectA", aws.ToString(info.VersionId), bucket, "objectA-copy")
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(copyInfo.VersionId))
			require.NotEqual(t, aws.ToString(info.VersionId), aws.ToString(copyInfo.VersionId))

			data, err := getObjectData(ctx, client, bucket, "objectA-copy", "")
			require.NoError(t, err)
			require.Equal(t, expectedContent, data)

			nonExistingVersionID := randomVersionID()
			_, err = copyObject(ctx, client, bucket, "objectA", nonExistingVersionID, bucket, "objectA-copy")

			if gw.kind == apiGateway {
				// Like S3, the api package answers a missing source version with NoSuchVersion.
				require.Equal(t, "NoSuchVersion", errorCode(err))
			} else {
				// NoSuchKey is the code that minio's client maps to ObjectNotFound.
				require.Equal(t, "NoSuchKey", errorCode(err))
			}

			// copy objectA with VersionID into objectA
			copyInfo, err = copyObject(ctx, client, bucket, "objectA", aws.ToString(info.VersionId), bucket, "objectA")
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(copyInfo.VersionId))
			require.NotEqual(t, aws.ToString(info.VersionId), aws.ToString(copyInfo.VersionId))
		})
	})
}

// listedVersion is an entry of a versions listing: a version, a delete marker, or a common prefix
// with only its Key set.
type listedVersion struct {
	Key            string
	VersionID      string
	IsLatest       bool
	IsDeleteMarker bool
}

// listObjectVersions lists all pages of versions for input. aws-sdk-go-v2 returns the versions and
// delete markers of a page in separate lists, so each page's entries are read from its response
// body in their listed order instead, versions and delete markers first and then common prefixes.
func listObjectVersions(ctx context.Context, client *s3.Client, input *s3.ListObjectVersionsInput) ([]listedVersion, error) {
	pageInput := *input
	var listing []listedVersion
	for {
		var body []byte
		resp, err := client.ListObjectVersions(ctx, &pageInput, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
				return stack.Deserialize.Add(middleware.DeserializeMiddlewareFunc("readBody", func(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (middleware.DeserializeOutput, middleware.Metadata, error) {
					out, metadata, err := next.HandleDeserialize(ctx, in)
					if resp, ok := out.RawResponse.(*smithyhttp.Response); ok && err == nil {
						if body, err = io.ReadAll(resp.Body); err == nil {
							resp.Body = io.NopCloser(bytes.NewReader(body))
						}
					}
					return out, metadata, err
				}), middleware.After)
			})
		})
		if err != nil {
			return nil, err
		}

		page, err := decodeVersionsPage(body)
		if err != nil {
			return nil, err
		}
		listing = append(listing, page...)

		if !aws.ToBool(resp.IsTruncated) {
			return listing, nil
		}
		pageInput.KeyMarker, pageInput.VersionIdMarker = resp.NextKeyMarker, resp.NextVersionIdMarker
	}
}

// decodeVersionsPage returns the entries of a ListObjectVersions response body in their listed
// order, versions and delete markers first and then common prefixes.
func decodeVersionsPage(body []byte) ([]listedVersion, error) {
	var page struct {
		Entries []struct {
			XMLName   xml.Name
			Key       string
			VersionID string `xml:"VersionId"`
			IsLatest  bool
			Prefix    string
		} `xml:",any"`
	}
	if err := xml.Unmarshal(body, &page); err != nil {
		return nil, err
	}
	var versions, prefixes []listedVersion
	for _, entry := range page.Entries {
		switch entry.XMLName.Local {
		case "Version", "DeleteMarker":
			versions = append(versions, listedVersion{
				Key:            entry.Key,
				VersionID:      entry.VersionID,
				IsLatest:       entry.IsLatest,
				IsDeleteMarker: entry.XMLName.Local == "DeleteMarker",
			})
		case "CommonPrefixes":
			prefixes = append(prefixes, listedVersion{Key: entry.Prefix})
		}
	}
	return append(versions, prefixes...), nil
}

// removeObjectResult is the result for one of the objects of a DeleteObjects request.
type removeObjectResult struct {
	ObjectName            string
	ObjectVersionID       string
	DeleteMarker          bool
	DeleteMarkerVersionID string
	Err                   error
}

// deleteObjects deletes versions of key, or creates a delete marker for an empty version ID, and
// returns a result for each, with the error of the request if it failed.
func deleteObjects(ctx context.Context, client *s3.Client, bucket, key string, versionIDs []string) []removeObjectResult {
	objects := make([]types.ObjectIdentifier, 0, len(versionIDs))
	for _, versionID := range versionIDs {
		object := types.ObjectIdentifier{Key: new(key)}
		if versionID != "" {
			object.VersionId = new(versionID)
		}
		objects = append(objects, object)
	}

	resp, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: new(bucket),
		Delete: &types.Delete{Objects: objects},
	})
	if err != nil {
		results := make([]removeObjectResult, 0, len(versionIDs))
		for _, versionID := range versionIDs {
			results = append(results, removeObjectResult{ObjectName: key, ObjectVersionID: versionID, Err: err})
		}
		return results
	}

	var results []removeObjectResult
	for _, deleted := range resp.Deleted {
		results = append(results, removeObjectResult{
			ObjectName:            aws.ToString(deleted.Key),
			ObjectVersionID:       aws.ToString(deleted.VersionId),
			DeleteMarker:          aws.ToBool(deleted.DeleteMarker),
			DeleteMarkerVersionID: aws.ToString(deleted.DeleteMarkerVersionId),
		})
	}
	for _, failed := range resp.Errors {
		results = append(results, removeObjectResult{
			ObjectName:      aws.ToString(failed.Key),
			ObjectVersionID: aws.ToString(failed.VersionId),
			Err:             errors.New(aws.ToString(failed.Code) + ": " + aws.ToString(failed.Message)),
		})
	}
	return results
}

// tagSetToMap returns the tags of tagSet by key.
func tagSetToMap(tagSet []types.Tag) map[string]string {
	tags := make(map[string]string, len(tagSet))
	for _, tag := range tagSet {
		tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	return tags
}
