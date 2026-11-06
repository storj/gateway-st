// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"bytes"
	"io"
	"slices"
	"sort"
	"strings"
	"testing"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"storj.io/common/memory"
	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/gateway/internal/minioclient"
	miniocmd "storj.io/minio/cmd"
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
		client := gw.minioClient(t)
		var err error

		rawClient, ok := client.(*minioclient.Minio)
		require.True(t, ok)

		t.Run("bucket versioning enabling-disabling", func(t *testing.T) {
			bucketA := testrand.BucketName()
			err = client.MakeBucket(ctx, bucketA)
			require.NoError(t, err)

			v, err := client.GetBucketVersioning(ctx, bucketA)
			require.NoError(t, err)
			require.Empty(t, v)

			// try to change bucket state unversioned -> enabled -> suspended
			require.NoError(t, client.EnableVersioning(ctx, bucketA))

			v, err = client.GetBucketVersioning(ctx, bucketA)
			require.NoError(t, err)
			require.EqualValues(t, versioning.Enabled, v)

			require.NoError(t, client.DisableVersioning(ctx, bucketA))

			v, err = client.GetBucketVersioning(ctx, bucketA)
			require.NoError(t, err)
			require.EqualValues(t, versioning.Suspended, v)

			bucketB := testrand.BucketName()
			err = client.MakeBucket(ctx, bucketB)
			require.NoError(t, err)

			// try to change bucket state unversioned -> suspended
			require.NoError(t, client.DisableVersioning(ctx, bucketB))
			v, err = client.GetBucketVersioning(ctx, bucketB)
			require.NoError(t, err)
			require.EqualValues(t, versioning.Suspended, v)
		})

		t.Run("check VersionID support for different methods", func(t *testing.T) {
			bucket := testrand.BucketName()

			require.NoError(t, client.MakeBucket(ctx, bucket))
			require.NoError(t, client.EnableVersioning(ctx, bucket))

			// upload first version
			expectedContentA1 := testrand.Bytes(5 * memory.KiB)
			uploadInfo, err := rawClient.API.PutObject(ctx, bucket, "objectA", bytes.NewReader(expectedContentA1), int64(len(expectedContentA1)), minio.PutObjectOptions{})
			require.NoError(t, err)
			require.NotEmpty(t, uploadInfo.VersionID)

			objectA1VersionID := uploadInfo.VersionID

			statInfo, err := rawClient.API.StatObject(ctx, bucket, "objectA", minio.GetObjectOptions{})
			require.NoError(t, err)
			require.Equal(t, objectA1VersionID, statInfo.VersionID)

			// the same request but with VersionID specified
			statInfo, err = rawClient.API.StatObject(ctx, bucket, "objectA", minio.GetObjectOptions{
				VersionID: objectA1VersionID,
			})
			require.NoError(t, err)
			require.Equal(t, objectA1VersionID, statInfo.VersionID)

			tags, err := tags.NewTags(map[string]string{
				"key1": "tag1",
			}, true)
			require.NoError(t, err)
			err = rawClient.API.PutObjectTagging(ctx, bucket, "objectA", tags, minio.PutObjectTaggingOptions{})
			require.NoError(t, err)

			// upload second version
			expectedContentA2 := testrand.Bytes(5 * memory.KiB)
			uploadInfo, err = rawClient.API.PutObject(ctx, bucket, "objectA", bytes.NewReader(expectedContentA2), int64(len(expectedContentA2)), minio.PutObjectOptions{})
			require.NoError(t, err)
			require.NotEmpty(t, uploadInfo.VersionID)

			objectA2VersionID := uploadInfo.VersionID

			statInfo, err = rawClient.API.StatObject(ctx, bucket, "objectA", minio.GetObjectOptions{})
			require.NoError(t, err)
			require.Equal(t, objectA2VersionID, statInfo.VersionID)

			// the same request but with VersionID specified
			statInfo, err = rawClient.API.StatObject(ctx, bucket, "objectA", minio.GetObjectOptions{
				VersionID: objectA2VersionID,
			})
			require.NoError(t, err)
			require.Equal(t, objectA2VersionID, statInfo.VersionID)

			// // check that we have two different versions
			object, err := rawClient.API.GetObject(ctx, bucket, "objectA", minio.GetObjectOptions{
				VersionID: objectA1VersionID,
			})
			require.NoError(t, err)

			contentA1, err := io.ReadAll(object)
			require.NoError(t, err)
			require.Equal(t, expectedContentA1, contentA1)

			object, err = rawClient.API.GetObject(ctx, bucket, "objectA", minio.GetObjectOptions{
				VersionID: objectA2VersionID,
			})
			require.NoError(t, err)

			contentA2, err := io.ReadAll(object)
			require.NoError(t, err)
			require.Equal(t, expectedContentA2, contentA2)

			tagsInfo, err := rawClient.API.GetObjectTagging(ctx, bucket, "objectA", minio.GetObjectTaggingOptions{
				VersionID: objectA1VersionID,
			})
			require.NoError(t, err)
			require.EqualValues(t, tags.ToMap(), tagsInfo.ToMap())

			// TODO(ver): add test for setting tag for specific version when implemented
		})

		t.Run("check VersionID while completing multipart upload", func(t *testing.T) {
			bucket := testrand.BucketName()

			require.NoError(t, client.MakeBucket(ctx, bucket))
			require.NoError(t, client.EnableVersioning(ctx, bucket))

			expectedContent := testrand.Bytes(500 * memory.KiB)
			uploadInfo, err := rawClient.API.PutObject(ctx, bucket, "objectA", bytes.NewReader(expectedContent), -1, minio.PutObjectOptions{})
			require.NoError(t, err)
			require.NotEmpty(t, uploadInfo.VersionID)
		})

		t.Run("check VersionID with delete object and delete objects", func(t *testing.T) {
			bucket := testrand.BucketName()

			require.NoError(t, client.MakeBucket(ctx, bucket))
			require.NoError(t, client.EnableVersioning(ctx, bucket))

			var versionIDs []string

			for range 5 {
				expectedContent := testrand.Bytes(5 * memory.KiB)
				uploadInfo, err := rawClient.API.PutObject(ctx, bucket, "objectA", bytes.NewReader(expectedContent), int64(len(expectedContent)), minio.PutObjectOptions{})
				require.NoError(t, err)
				require.NotEmpty(t, uploadInfo.VersionID)
				versionIDs = append(versionIDs, uploadInfo.VersionID)
			}

			err := rawClient.API.RemoveObject(ctx, bucket, "objectA", minio.RemoveObjectOptions{
				VersionID: versionIDs[0],
			})
			require.NoError(t, err)

			for listed := range rawClient.API.ListObjects(ctx, bucket, minio.ListObjectsOptions{
				WithVersions: true,
			}) {
				if listed.VersionID == versionIDs[len(versionIDs)-1] {
					require.True(t, listed.IsLatest)
				} else {
					require.False(t, listed.IsLatest)
				}
			}

			removeObjects := func(versionIDs []string) <-chan minio.RemoveObjectResult {
				objectsCh := make(chan minio.ObjectInfo)
				ctx.Go(func() error {
					defer close(objectsCh)
					for _, versionID := range versionIDs {
						objectsCh <- minio.ObjectInfo{
							Key:       "objectA",
							VersionID: versionID,
						}
					}
					return nil
				})

				return rawClient.API.RemoveObjectsWithResult(ctx, bucket, objectsCh, minio.RemoveObjectsOptions{})
			}

			expected := make([]minio.RemoveObjectResult, 0, len(versionIDs)-1)
			for i := 1; i < len(versionIDs); i++ {
				expected = append(expected, minio.RemoveObjectResult{
					ObjectName:      "objectA",
					ObjectVersionID: versionIDs[i],
					DeleteMarker:    false,
				})
			}

			removed := make([]minio.RemoveObjectResult, 0, len(versionIDs)-1)
			for result := range removeObjects(versionIDs[1:]) {
				removed = append(removed, result)
			}

			require.ElementsMatch(t, expected, removed)

			for range rawClient.API.ListObjects(ctx, bucket, minio.ListObjectsOptions{
				WithVersions: true,
			}) {
				require.Fail(t, "no objects to list")
			}

			// create one object and one delete marker
			_, err = rawClient.API.PutObject(ctx, bucket, "objectA", bytes.NewReader(testrand.Bytes(5*memory.KiB)), -1, minio.PutObjectOptions{})
			require.NoError(t, err)

			err = rawClient.API.RemoveObject(ctx, bucket, "objectA", minio.RemoveObjectOptions{})
			require.NoError(t, err)

			removed = nil
			for result := range removeObjects([]string{""}) {
				removed = append(removed, result)
			}

			require.Len(t, removed, 1)
			require.NoError(t, removed[0].Err)
			require.Equal(t, "objectA", removed[0].ObjectName)
			require.Empty(t, removed[0].ObjectVersionID)
			require.True(t, removed[0].DeleteMarker)
			require.NotEmpty(t, removed[0].DeleteMarkerVersionID)

			var objects []minio.ObjectInfo
			versionIDs = nil
			versionIDOrder := make(map[string]int)

			for obj := range rawClient.API.ListObjects(ctx, bucket, minio.ListObjectsOptions{
				WithVersions: true,
			}) {
				objects = append(objects, obj)
				versionIDOrder[obj.VersionID] = len(versionIDs)
				versionIDs = append(versionIDs, obj.VersionID)
			}

			require.Len(t, objects, 3)
			require.Equal(t, removed[0].DeleteMarkerVersionID, objects[0].VersionID)
			require.True(t, objects[0].IsLatest)

			removed = []minio.RemoveObjectResult{}
			for result := range removeObjects(versionIDs) {
				removed = append(removed, result)
			}

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

			require.NoError(t, client.MakeBucket(ctx, bucketA))
			require.NoError(t, client.EnableVersioning(ctx, bucketA))

			require.NoError(t, client.MakeBucket(ctx, bucketB))
			require.NoError(t, client.EnableVersioning(ctx, bucketB))

			require.NoError(t, client.MakeBucket(ctx, bucketC))
			require.NoError(t, client.EnableVersioning(ctx, bucketC))

			for range rawClient.API.ListObjects(ctx, bucketA, minio.ListObjectsOptions{
				WithVersions: true,
			}) {
				require.Fail(t, "no objects to list")
			}

			for range rawClient.API.ListObjects(ctx, bucketB, minio.ListObjectsOptions{
				WithVersions: true,
			}) {
				require.Fail(t, "no objects to list")
			}

			for range rawClient.API.ListObjects(ctx, bucketC, minio.ListObjectsOptions{
				WithVersions: true,
			}) {
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
						err = rawClient.API.RemoveObject(ctx, bucket, upload.key, minio.RemoveObjectOptions{})
						require.NoError(t, err)
						// Minio doesn't give easy access to delete marker after RemoveObject so just leave it empty
						versions[upload.key] = append(versions[upload.key], "deletemarker")
					} else {
						uploadInfo, err := rawClient.API.PutObject(ctx, bucket, upload.key, bytes.NewReader(expectedContent), int64(len(expectedContent)), minio.PutObjectOptions{})
						require.NoError(t, err)
						versions[upload.key] = append(versions[upload.key], uploadInfo.VersionID)
					}
				}
				for k := range versions {
					slices.Reverse(versions[k])
				}
				return versions
			}

			// TODO minio client ListObjects provides limited options list e.g. marker cannot be set
			// so we cannot test many edge cases, we should figure out how to improve it

			checkListing := func(t *testing.T, bucket string, testCases []testCase) {
				for _, tc := range testCases {
					t.Run(tc.name, func(t *testing.T) {
						for _, maxKeys := range []int{0, 1, 2, 1000} {
							count := 0
							for objectInfo := range rawClient.API.ListObjects(ctx, bucket, minio.ListObjectsOptions{
								Prefix:       tc.prefix,
								Recursive:    tc.recursive,
								MaxKeys:      maxKeys,
								WithVersions: true,
							}) {
								require.NoError(t, objectInfo.Err)

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

			require.NoError(t, client.MakeBucket(ctx, bucket))
			require.NoError(t, client.EnableVersioning(ctx, bucket))

			expectedContent := testrand.Bytes(5 * memory.KiB)
			_, err := rawClient.API.PutObject(ctx, bucket, "objectA", bytes.NewReader(expectedContent), int64(len(expectedContent)), minio.PutObjectOptions{})
			require.NoError(t, err)

			err = rawClient.API.RemoveObject(ctx, bucket, "objectA", minio.RemoveObjectOptions{})
			require.NoError(t, err)

			var deleteMarkerVersionID string
			for objectInfo := range rawClient.API.ListObjects(ctx, bucket, minio.ListObjectsOptions{
				WithVersions: true,
			}) {
				if objectInfo.IsDeleteMarker {
					deleteMarkerVersionID = objectInfo.VersionID
				}
			}
			require.NotEmpty(t, deleteMarkerVersionID)

			_, err = rawClient.API.StatObject(ctx, bucket, "objectA", minio.StatObjectOptions{
				VersionID: deleteMarkerVersionID,
			})
			require.Error(t, err)
		})

		t.Run("CopyObject with source VersionID", func(t *testing.T) {
			bucket := testrand.BucketName()

			require.NoError(t, client.MakeBucket(ctx, bucket))
			require.NoError(t, client.EnableVersioning(ctx, bucket))

			expectedSize := 5 * memory.KiB
			expectedContent := testrand.Bytes(expectedSize)
			info, err := rawClient.API.PutObject(ctx, bucket, "objectA", bytes.NewReader(expectedContent), expectedSize.Int64(), minio.PutObjectOptions{})
			require.NoError(t, err)

			_, err = rawClient.API.PutObject(ctx, bucket, "objectA", bytes.NewReader(testrand.Bytes(expectedSize)), expectedSize.Int64(), minio.PutObjectOptions{})
			require.NoError(t, err)

			copyInfo, err := rawClient.API.CopyObject(ctx, minio.CopyDestOptions{
				Bucket: bucket,
				Object: "objectA-copy",
			}, minio.CopySrcOptions{
				Bucket:    bucket,
				Object:    "objectA",
				VersionID: info.VersionID,
			})
			require.NoError(t, err)
			require.NotEmpty(t, copyInfo.VersionID)
			require.NotEqual(t, info.VersionID, copyInfo.VersionID)

			download, err := rawClient.API.GetObject(ctx, bucket, "objectA-copy", minio.GetObjectOptions{})
			require.NoError(t, err)
			defer ctx.Check(download.Close)

			data, err := io.ReadAll(download)
			require.NoError(t, err)
			require.Equal(t, expectedContent, data)

			nonExistingVersionID := randomVersionID()
			_, err = rawClient.API.CopyObject(ctx, minio.CopyDestOptions{
				Bucket: bucket,
				Object: "objectA-copy",
			}, minio.CopySrcOptions{
				Bucket:    bucket,
				Object:    "objectA",
				VersionID: nonExistingVersionID,
			})

			var objNotFound miniocmd.ObjectNotFound
			require.ErrorAs(t, miniocmd.ErrorRespToObjectError(err, bucket, "objectA-copy"), &objNotFound)

			// copy objectA with VersionID into objectA
			copyInfo, err = rawClient.API.CopyObject(ctx, minio.CopyDestOptions{
				Bucket: bucket,
				Object: "objectA",
			}, minio.CopySrcOptions{
				Bucket:    bucket,
				Object:    "objectA",
				VersionID: info.VersionID,
			})
			require.NoError(t, err)
			require.NotEmpty(t, copyInfo.VersionID)
			require.NotEqual(t, info.VersionID, copyInfo.VersionID)
		})
	})
}
