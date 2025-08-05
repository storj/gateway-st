// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os/exec"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"storj.io/common/base58"
	"storj.io/common/memory"
	"storj.io/common/processgroup"
	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/gateway/internal/minioclient"
	miniocmd "storj.io/minio/cmd"
	"storj.io/minio/pkg/bucket/versioning"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite"
	"storj.io/storj/satellite/buckets"
)

func TestUploadDownload(t *testing.T) {
	var counter int64
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		NonParallel: true,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		oldAccess := planet.Uplinks[0].Access[planet.Satellites[0].ID()]

		access, err := oldAccess.Serialize()
		require.NoError(t, err)

		index := atomic.AddInt64(&counter, 1)
		// TODO: make address not hardcoded the address selection here
		// may conflict with some automatically bound address.
		gatewayAddr := fmt.Sprintf("127.0.0.1:1100%d", index)

		gatewayAccessKey := base58.Encode(testrand.BytesInt(20))
		gatewaySecretKey := base58.Encode(testrand.BytesInt(20))

		gatewayExe := ctx.CompileAt("../..", "storj.io/gateway")

		client, err := minioclient.NewMinio(minioclient.Config{
			S3Gateway:     gatewayAddr,
			Satellite:     planet.Satellites[0].Addr(),
			AccessKey:     gatewayAccessKey,
			SecretKey:     gatewaySecretKey,
			APIKey:        planet.Uplinks[0].APIKey[planet.Satellites[0].ID()].Serialize(),
			EncryptionKey: "fake-encryption-key",
			NoSSL:         true,
		})
		require.NoError(t, err)

		gateway, err := startGateway(t, ctx, client, gatewayExe, access, gatewayAddr, gatewayAccessKey, gatewaySecretKey)
		require.NoError(t, err)
		defer func() { processgroup.Kill(gateway) }()

		{ // normal upload
			bucket := "bucket"

			err = client.MakeBucket(ctx, bucket)
			require.NoError(t, err)

			// generate enough data for a remote segment
			data := testrand.BytesInt(5000)
			objectName := "testdata"

			rawClient, ok := client.(*minioclient.Minio)
			require.True(t, ok)

			expectedMetadata := map[string]string{
				"foo": "bar",
			}
			err = rawClient.Upload(ctx, bucket, objectName, data, expectedMetadata)
			require.NoError(t, err)

			object, err := rawClient.API.StatObject(ctx, bucket, objectName, minio.StatObjectOptions{})
			require.NoError(t, err)
			// TODO figure out why it returns "Foo:bar", instead "foo:bar"
			require.EqualValues(t, map[string]string{
				"Foo": "bar",
			}, object.UserMetadata)

			buffer := make([]byte, len(data))

			bytes, err := client.Download(ctx, bucket, objectName, buffer)
			require.NoError(t, err)

			require.Equal(t, data, bytes)

			{ // try to access the content as static website - expect forbidden error
				response, err := httpGet(ctx, fmt.Sprintf("http://%s/%s", gatewayAddr, bucket))
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				require.NoError(t, response.Body.Close())

				response, err = httpGet(ctx, fmt.Sprintf("http://%s/%s/%s", gatewayAddr, bucket, objectName))
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				require.NoError(t, response.Body.Close())
			}

			{ // restart the gateway with the --website flag and try again - expect success
				err = stopGateway(ctx, gateway, gatewayAddr)
				require.NoError(t, err)
				gateway, err = startGateway(t, ctx, client, gatewayExe, access, gatewayAddr, gatewayAccessKey, gatewaySecretKey, "--website")
				require.NoError(t, err)

				response, err := httpGet(ctx, fmt.Sprintf("http://%s/%s", gatewayAddr, bucket))
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				require.NoError(t, response.Body.Close())

				response, err = httpGet(ctx, fmt.Sprintf("http://%s/%s/%s", gatewayAddr, bucket, objectName))
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				readData, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				require.Equal(t, data, readData)
				require.NoError(t, response.Body.Close())
			}
		}

		{ // multipart upload
			bucket := "bucket-multipart"

			err = client.MakeBucket(ctx, bucket)
			require.NoError(t, err)

			// minimum single part size is 5mib
			size := 8 * memory.MiB
			data := testrand.Bytes(size)
			objectName := "testdata"
			partSize := 5 * memory.MiB

			part1MD5 := md5.Sum(data[:partSize])
			part2MD5 := md5.Sum(data[partSize:])
			parts := append([]byte{}, part1MD5[:]...)
			parts = append(parts, part2MD5[:]...)
			partsMD5 := md5.Sum(parts)
			expectedETag := hex.EncodeToString(partsMD5[:]) + "-2"

			rawClient, ok := client.(*minioclient.Minio)
			require.True(t, ok)

			expectedMetadata := map[string]string{
				"foo": "bar",
			}
			err = rawClient.UploadMultipart(ctx, bucket, objectName, data, partSize.Int(), 0, expectedMetadata)
			require.NoError(t, err)

			// TODO find out why with prefix set its hanging test
			for message := range rawClient.API.ListObjects(ctx, bucket, minio.ListObjectsOptions{WithMetadata: true}) {
				require.Equal(t, objectName, message.Key)
				require.NotEmpty(t, message.ETag)

				// Minio adds a double quote to ETag, sometimes.
				// Remove the potential quote from either end.
				etag := strings.TrimPrefix(message.ETag, `"`)
				etag = strings.TrimSuffix(etag, `"`)

				require.Equal(t, expectedETag, etag)
				break
			}

			object, err := rawClient.API.StatObject(ctx, bucket, objectName, minio.StatObjectOptions{})
			require.NoError(t, err)
			// TODO figure out why it returns "Foo:bar", instead "foo:bar"
			require.EqualValues(t, map[string]string{
				"Foo": "bar",
			}, object.UserMetadata)

			buffer := make([]byte, len(data))
			bytes, err := client.Download(ctx, bucket, objectName, buffer)
			require.NoError(t, err)

			require.Equal(t, data, bytes)
		}
		{
			uplink := planet.Uplinks[0]
			satellite := planet.Satellites[0]
			info, err := satellite.DB.Buckets().GetBucket(ctx, []byte("bucket"), uplink.Projects[0].ID)
			require.NoError(t, err)
			require.Contains(t, string(info.UserAgent), "Zenko")
		}
	})
}

func httpGet(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

func startGateway(t *testing.T, ctx *testcontext.Context, client minioclient.Client, exe, access, address, accessKey, secretKey string, moreFlags ...string) (*exec.Cmd, error) {
	args := append([]string{"run",
		"--config-dir", ctx.Dir("gateway"),
		"--access", access,
		"--server.address", address,
		"--minio.access-key", accessKey,
		"--minio.secret-key", secretKey,
		"--client.user-agent", "Zenko",
	}, moreFlags...)

	gateway := exec.Command(exe, args...)
	processgroup.Setup(gateway)

	log := zaptest.NewLogger(t)
	gateway.Stdout = logWriter{log.Named("gateway:stdout")}
	gateway.Stderr = logWriter{log.Named("gateway:stderr")}

	err := gateway.Start()
	if err != nil {
		return nil, err
	}

	err = waitToStart(ctx, client, address, 5*time.Second)
	if err != nil {
		killErr := gateway.Process.Kill()
		return nil, errs.Combine(err, killErr)
	}

	return gateway, nil
}

func stopGateway(ctx context.Context, gateway *exec.Cmd, address string) error {
	err := gateway.Process.Kill()
	if err != nil {
		return err
	}

	start := time.Now()
	maxStopWait := 5 * time.Second
	for {
		if !tryConnect(ctx, address) {
			return nil
		}

		// wait a bit before retrying to reduce load
		time.Sleep(50 * time.Millisecond)

		if time.Since(start) > maxStopWait {
			return fmt.Errorf("%s did not stop in required time %v", address, maxStopWait)
		}
	}
}

// waitToStart will monitor starting when we are able to start the process.
func waitToStart(ctx context.Context, client minioclient.Client, address string, maxStartupWait time.Duration) error {
	start := time.Now()
	for {
		_, err := client.ListBuckets(ctx)
		if err == nil {
			return nil
		}

		// wait a bit before retrying to reduce load
		time.Sleep(50 * time.Millisecond)

		if time.Since(start) > maxStartupWait {
			return fmt.Errorf("%s did not start in required time %v", address, maxStartupWait)
		}
	}
}

// tryConnect will try to connect to the process public address.
func tryConnect(ctx context.Context, address string) bool {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return false
	}
	// write empty byte slice to trigger refresh on connection
	_, _ = conn.Write([]byte{})
	// ignoring errors, because we only care about being able to connect
	_ = conn.Close()
	return true
}

type logWriter struct{ log *zap.Logger }

func (log logWriter) Write(p []byte) (n int, err error) {
	log.log.Debug(string(p))
	return len(p), nil
}

func TestVersioning(t *testing.T) {
	var counter int64
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.DefaultPathCipher = storj.EncNull
			},
		},
		NonParallel: true,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		oldAccess := planet.Uplinks[0].Access[planet.Satellites[0].ID()]

		access, err := oldAccess.Serialize()
		require.NoError(t, err)

		index := atomic.AddInt64(&counter, 1)
		// TODO: make address not hardcoded the address selection here
		// may conflict with some automatically bound address.
		gatewayAddr := fmt.Sprintf("127.0.0.1:1100%d", index)

		gatewayAccessKey := base58.Encode(testrand.BytesInt(20))
		gatewaySecretKey := base58.Encode(testrand.BytesInt(20))

		gatewayExe := ctx.CompileAt("../..", "storj.io/gateway")

		client, err := minioclient.NewMinio(minioclient.Config{
			S3Gateway:     gatewayAddr,
			Satellite:     planet.Satellites[0].Addr(),
			AccessKey:     gatewayAccessKey,
			SecretKey:     gatewaySecretKey,
			APIKey:        planet.Uplinks[0].APIKey[planet.Satellites[0].ID()].Serialize(),
			EncryptionKey: "fake-encryption-key",
			NoSSL:         true,
		})
		require.NoError(t, err)

		gateway, err := startGateway(t, ctx, client, gatewayExe, access, gatewayAddr, gatewayAccessKey, gatewaySecretKey)
		require.NoError(t, err)
		defer func() { processgroup.Kill(gateway) }()

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

			versionIDs := make([]string, 5)

			for i := range versionIDs {
				expectedContent := testrand.Bytes(5 * memory.KiB)
				uploadInfo, err := rawClient.API.PutObject(ctx, bucket, "objectA", bytes.NewReader(expectedContent), int64(len(expectedContent)), minio.PutObjectOptions{})
				require.NoError(t, err)
				require.NotEmpty(t, uploadInfo.VersionID)
				versionIDs[i] = uploadInfo.VersionID
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

			var latestDeleteMarkerVersionID string
			for result := range removeObjects([]string{""}) {
				require.NoError(t, result.Err)
				require.Equal(t, "objectA", result.ObjectName)
				require.NotEmpty(t, result.ObjectVersionID)
				require.True(t, result.DeleteMarker)
				require.NotEmpty(t, result.DeleteMarkerVersionID)
				latestDeleteMarkerVersionID = result.DeleteMarkerVersionID
			}

			listedIDs := []string{}
			for listed := range rawClient.API.ListObjects(ctx, bucket, minio.ListObjectsOptions{
				WithVersions: true,
			}) {
				if listed.VersionID == latestDeleteMarkerVersionID {
					require.True(t, listed.IsLatest)
				} else {
					require.False(t, listed.IsLatest)
				}
				listedIDs = append(listedIDs, listed.VersionID)
			}

			removed = removed[:0]
			for result := range removeObjects(listedIDs) {
				removed = append(removed, result)
			}
			sort.Slice(removed, func(i, j int) bool {
				return removed[i].DeleteMarker
			})

			for i := 0; i < 2; i++ {
				result := removed[i]
				require.NoError(t, result.Err)
				require.Equal(t, "objectA", result.ObjectName)
				require.True(t, result.DeleteMarker)
				require.NotEmpty(t, result.DeleteMarkerVersionID)
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

func TestBucketTagging(t *testing.T) {
	var counter int64
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.BucketTaggingEnabled = true
			},
		},
		NonParallel: true,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]
		upl := planet.Uplinks[0]
		bucketsDB := sat.DB.Buckets()
		projectID := upl.Projects[0].ID

		project, err := upl.OpenProject(ctx, sat)
		require.NoError(t, err)

		defer ctx.Check(project.Close)

		access, err := planet.Uplinks[0].Access[planet.Satellites[0].ID()].Serialize()
		require.NoError(t, err)

		index := atomic.AddInt64(&counter, 1)
		// TODO: make address not hardcoded the address selection here
		// may conflict with some automatically bound address.
		gatewayAddr := fmt.Sprintf("127.0.0.1:1100%d", index)

		gatewayAccessKey := base58.Encode(testrand.BytesInt(20))
		gatewaySecretKey := base58.Encode(testrand.BytesInt(20))

		gatewayExe := ctx.CompileAt("../..", "storj.io/gateway")

		minioClient, err := minioclient.NewMinio(minioclient.Config{
			S3Gateway:     gatewayAddr,
			Satellite:     planet.Satellites[0].Addr(),
			AccessKey:     gatewayAccessKey,
			SecretKey:     gatewaySecretKey,
			APIKey:        planet.Uplinks[0].APIKey[planet.Satellites[0].ID()].Serialize(),
			EncryptionKey: "fake-encryption-key",
			NoSSL:         true,
		})
		require.NoError(t, err)

		gateway, err := startGateway(t, ctx, minioClient, gatewayExe, access, gatewayAddr, gatewayAccessKey, gatewaySecretKey)
		require.NoError(t, err)
		defer func() { processgroup.Kill(gateway) }()

		client := createS3Client(t, gatewayAddr, gatewayAccessKey, gatewaySecretKey)

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

func createS3Client(t *testing.T, gatewayAddr, accessKeyID, secretKey string) *s3.S3 {
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String("global"),
		Credentials:      credentials.NewStaticCredentials(accessKeyID, secretKey, ""),
		Endpoint:         aws.String("http://" + gatewayAddr),
		S3ForcePathStyle: aws.Bool(true),
	})
	require.NoError(t, err)

	return s3.New(sess)
}

func errorCode(err error) string {
	var awsErr awserr.Error
	if errors.As(err, &awsErr) {
		return awsErr.Code()
	}
	return ""
}

func statusCode(err error) int {
	var reqErr awserr.RequestFailure
	if errors.As(err, &reqErr) {
		return reqErr.StatusCode()
	}
	return 0
}

func requireS3Error(t *testing.T, err error, status int, code string) {
	require.Error(t, err)
	require.Equal(t, status, statusCode(err))
	require.Equal(t, code, errorCode(err))
}

func randomVersionID() string {
	version := testrand.UUID()
	binary.BigEndian.PutUint64(version[:8], uint64(rand.Uint32()))
	return hex.EncodeToString(version.Bytes())
}
