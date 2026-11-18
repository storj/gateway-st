// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite"
)

// checksumsPlanet is a planet whose satellite allows checksums, so that the
// gateways store them.
var checksumsPlanet = testplanet.Config{
	SatelliteCount:   1,
	StorageNodeCount: 4,
	UplinkCount:      1,
	Reconfigure: testplanet.Reconfigure{
		Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
			config.Metainfo.ChecksumsEnabled = true
		},
	},
}

var checksumAlgorithms = []types.ChecksumAlgorithm{
	types.ChecksumAlgorithmCrc32, types.ChecksumAlgorithmCrc32c, types.ChecksumAlgorithmCrc64nvme,
	types.ChecksumAlgorithmSha1, types.ChecksumAlgorithmSha256,
}

// withoutChecksum makes the SDK send no checksum unless the request sets one,
// so that the gateway's default applies.
func withoutChecksum(o *s3.Options) {
	o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
}

// TestChecksums checks the checksums that the gateways store and report. The
// gateway binary's minio handlers neither report checksums nor pass on the ones
// clients send, so with it the test only checks that clients sending them keep
// working.
func TestChecksums(t *testing.T) {
	runIntegrationTest(t, checksumsPlanet, func(ctx *testcontext.Context, t *testing.T, planet *testplanet.Planet, gw *testGateway) {
		client := gw.client(t)

		bucket := testrand.BucketName()
		require.NoError(t, createBucket(ctx, client, bucket, false, false))

		data := testrand.BytesInt(memory.KiB.Int())
		parts := [][]byte{testrand.BytesInt(5 * memory.MiB.Int()), testrand.BytesInt(5 * memory.MiB.Int()), []byte("hello world")}
		whole := bytes.Join(parts, nil)

		if gw.kind == binaryGateway {
			for _, algorithm := range checksumAlgorithms {
				key := "put-" + string(algorithm)
				_, err := client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: new(bucket), Key: new(key), Body: bytes.NewReader(data), ChecksumAlgorithm: algorithm,
				})
				require.NoError(t, err)
				requireObjectData(ctx, t, client, bucket, key, data)

				key = "multipart-" + string(algorithm)
				_, err = putObjectMultipart(ctx, client, &s3.CreateMultipartUploadInput{
					Bucket: new(bucket), Key: new(key), ChecksumAlgorithm: algorithm,
				}, parts...)
				require.NoError(t, err)
				requireObjectData(ctx, t, client, bucket, key, whole)
			}
			return
		}

		t.Run("PutObject", func(t *testing.T) {
			for _, algorithm := range checksumAlgorithms {
				key := "put-" + string(algorithm)
				want := checksumOf(algorithm, data)

				put, err := client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: new(bucket), Key: new(key), Body: bytes.NewReader(data), ChecksumAlgorithm: algorithm,
				})
				require.NoError(t, err)
				assert.Equal(t, want, pickChecksum(algorithm, put.ChecksumCRC32, put.ChecksumCRC32C, put.ChecksumCRC64NVME, put.ChecksumSHA1, put.ChecksumSHA256))
				assert.Equal(t, types.ChecksumTypeFullObject, put.ChecksumType)

				head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: new(bucket), Key: new(key), ChecksumMode: types.ChecksumModeEnabled})
				require.NoError(t, err)
				assert.Equal(t, want, pickChecksum(algorithm, head.ChecksumCRC32, head.ChecksumCRC32C, head.ChecksumCRC64NVME, head.ChecksumSHA1, head.ChecksumSHA256))
				assert.Equal(t, types.ChecksumTypeFullObject, head.ChecksumType)

				// The SDK validates the data against the checksum in the response.
				get, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: new(bucket), Key: new(key), ChecksumMode: types.ChecksumModeEnabled})
				require.NoError(t, err)
				assert.Equal(t, want, pickChecksum(algorithm, get.ChecksumCRC32, get.ChecksumCRC32C, get.ChecksumCRC64NVME, get.ChecksumSHA1, get.ChecksumSHA256))
				require.NoError(t, get.Body.Close())
				requireObjectData(ctx, t, client, bucket, key, data)
			}

			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: new(bucket), Key: new("bad-digest"), Body: bytes.NewReader(data), ChecksumCRC32: new("AAAAAA=="),
			})
			requireS3Error(t, err, http.StatusBadRequest, "BadDigest")
		})

		// Like S3, a range only gets the object's checksum when it covers the whole object.
		t.Run("Ranges", func(t *testing.T) {
			key := "put-" + string(types.ChecksumAlgorithmCrc32c)
			want := checksumOf(types.ChecksumAlgorithmCrc32c, data)
			for _, tc := range []struct {
				rangeHeader string
				data        []byte
				checksum    string
			}{
				{"bytes=0-1", data[:2], ""},
				{"bytes=1-", data[1:], ""},
				{"bytes=0-", data, want},
				{fmt.Sprintf("bytes=0-%d", len(data)-1), data, want},
				{fmt.Sprintf("bytes=-%d", len(data)), data, want},
			} {
				get, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: new(bucket), Key: new(key), Range: new(tc.rangeHeader), ChecksumMode: types.ChecksumModeEnabled,
				})
				require.NoError(t, err, tc.rangeHeader)
				body, err := io.ReadAll(get.Body)
				require.NoError(t, err, tc.rangeHeader)
				require.NoError(t, get.Body.Close())
				assert.Equal(t, tc.data, body, tc.rangeHeader)
				assert.Equal(t, tc.checksum, aws.ToString(get.ChecksumCRC32C), tc.rangeHeader)
			}
		})

		t.Run("DefaultChecksum", func(t *testing.T) {
			_, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: new(bucket), Key: new("default"), Body: bytes.NewReader(data)}, withoutChecksum)
			require.NoError(t, err)

			head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: new(bucket), Key: new("default"), ChecksumMode: types.ChecksumModeEnabled})
			require.NoError(t, err)
			assert.Equal(t, checksumOf(types.ChecksumAlgorithmCrc64nvme, data), aws.ToString(head.ChecksumCRC64NVME))
			assert.Equal(t, types.ChecksumTypeFullObject, head.ChecksumType)
		})

		t.Run("ListObjects", func(t *testing.T) {
			list, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: new(bucket), Prefix: new("put-SHA256")})
			require.NoError(t, err)
			require.Len(t, list.Contents, 1)
			assert.Equal(t, []types.ChecksumAlgorithm{types.ChecksumAlgorithmSha256}, list.Contents[0].ChecksumAlgorithm)
			assert.Equal(t, types.ChecksumTypeFullObject, list.Contents[0].ChecksumType)
		})

		t.Run("CopyObject", func(t *testing.T) {
			copied, err := copyObject(ctx, client, bucket, "put-SHA256", "", bucket, "copy")
			require.NoError(t, err)
			require.NotNil(t, copied.CopyObjectResult)
			assert.Equal(t, checksumOf(types.ChecksumAlgorithmSha256, data), aws.ToString(copied.CopyObjectResult.ChecksumSHA256))

			// Unlike S3, the gateway can't compute another checksum of the source.
			_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket: new(bucket), Key: new("copy-crc32"), CopySource: new(copySource(bucket, "put-SHA256", "")),
				ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
			})
			requireS3Error(t, err, http.StatusNotImplemented, "NotImplemented")
		})

		t.Run("ListObjectVersions", func(t *testing.T) {
			list, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: new(bucket), Prefix: new("put-CRC32C")})
			require.NoError(t, err)
			require.Len(t, list.Versions, 1)
			assert.Equal(t, []types.ChecksumAlgorithm{types.ChecksumAlgorithmCrc32c}, list.Versions[0].ChecksumAlgorithm)
			assert.Equal(t, types.ChecksumTypeFullObject, list.Versions[0].ChecksumType)
		})

		t.Run("MetadataUpdates", func(t *testing.T) {
			key := "put-SHA256"
			want := checksumOf(types.ChecksumAlgorithmSha256, data)
			requireChecksum := func(t *testing.T) *s3.HeadObjectOutput {
				head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: new(bucket), Key: new(key), ChecksumMode: types.ChecksumModeEnabled})
				require.NoError(t, err)
				assert.Equal(t, want, aws.ToString(head.ChecksumSHA256))
				return head
			}

			_, err := client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
				Bucket: new(bucket), Key: new(key),
				Tagging: &types.Tagging{TagSet: []types.Tag{{Key: new("key"), Value: new("value")}}},
			})
			require.NoError(t, err)
			requireChecksum(t)

			_, err = client.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{Bucket: new(bucket), Key: new(key)})
			require.NoError(t, err)
			requireChecksum(t)

			// Copying an object onto itself replaces its metadata.
			_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket: new(bucket), Key: new(key), CopySource: new(copySource(bucket, key, "")),
				MetadataDirective: types.MetadataDirectiveReplace, Metadata: map[string]string{"key": "value"},
			})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"key": "value"}, requireChecksum(t).Metadata)
		})

		t.Run("ListMultipartUploads", func(t *testing.T) {
			// Like S3, an upload with the default algorithm is listed without checksums.
			for _, tc := range []struct {
				key          string
				algorithm    types.ChecksumAlgorithm
				checksumType types.ChecksumType
			}{
				{"listed-composite", types.ChecksumAlgorithmCrc32c, types.ChecksumTypeComposite},
				{"listed-full-object", types.ChecksumAlgorithmCrc64nvme, types.ChecksumTypeFullObject},
				{"listed-default", "", ""},
			} {
				_, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
					Bucket: new(bucket), Key: new(tc.key), ChecksumAlgorithm: tc.algorithm,
				}, withoutChecksum)
				require.NoError(t, err)

				list, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: new(bucket), Prefix: new(tc.key)})
				require.NoError(t, err)
				require.Len(t, list.Uploads, 1, tc.key)
				assert.Equal(t, tc.algorithm, list.Uploads[0].ChecksumAlgorithm, tc.key)
				assert.Equal(t, tc.checksumType, list.Uploads[0].ChecksumType, tc.key)

				_, err = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: new(bucket), Key: new(tc.key), UploadId: list.Uploads[0].UploadId})
				require.NoError(t, err)
			}
		})

		t.Run("CompositeMultipartUpload", func(t *testing.T) {
			for _, algorithm := range []types.ChecksumAlgorithm{
				types.ChecksumAlgorithmCrc32, types.ChecksumAlgorithmCrc32c, types.ChecksumAlgorithmSha1, types.ChecksumAlgorithmSha256,
			} {
				t.Run(string(algorithm), func(t *testing.T) {
					key := "composite-" + string(algorithm)
					create, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
						Bucket: new(bucket), Key: new(key), ChecksumAlgorithm: algorithm,
					})
					require.NoError(t, err)
					assert.Equal(t, types.ChecksumTypeComposite, create.ChecksumType)

					var completed []types.CompletedPart
					var partSums []string
					for i, part := range parts {
						partNumber := new(int32(i + 1))
						uploaded, err := client.UploadPart(ctx, &s3.UploadPartInput{
							Bucket: new(bucket), Key: new(key), UploadId: create.UploadId, PartNumber: partNumber,
							Body: bytes.NewReader(part), ChecksumAlgorithm: algorithm,
						})
						require.NoError(t, err)
						partSum := pickChecksum(algorithm, uploaded.ChecksumCRC32, uploaded.ChecksumCRC32C, uploaded.ChecksumCRC64NVME, uploaded.ChecksumSHA1, uploaded.ChecksumSHA256)
						assert.Equal(t, checksumOf(algorithm, part), partSum)
						partSums = append(partSums, partSum)
						completed = append(completed, types.CompletedPart{
							PartNumber: partNumber, ETag: uploaded.ETag,
							ChecksumCRC32: uploaded.ChecksumCRC32, ChecksumCRC32C: uploaded.ChecksumCRC32C,
							ChecksumSHA1: uploaded.ChecksumSHA1, ChecksumSHA256: uploaded.ChecksumSHA256,
						})
					}

					listed, err := client.ListParts(ctx, &s3.ListPartsInput{Bucket: new(bucket), Key: new(key), UploadId: create.UploadId})
					require.NoError(t, err)
					assert.Equal(t, algorithm, listed.ChecksumAlgorithm)
					require.Len(t, listed.Parts, len(parts))
					for i, part := range listed.Parts {
						assert.Equal(t, partSums[i], pickChecksum(algorithm, part.ChecksumCRC32, part.ChecksumCRC32C, part.ChecksumCRC64NVME, part.ChecksumSHA1, part.ChecksumSHA256))
					}

					composite := compositeChecksum(t, algorithm, partSums) + "-3"
					complete, err := client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
						Bucket: new(bucket), Key: new(key), UploadId: create.UploadId,
						MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
					})
					require.NoError(t, err)
					assert.Equal(t, composite, pickChecksum(algorithm, complete.ChecksumCRC32, complete.ChecksumCRC32C, complete.ChecksumCRC64NVME, complete.ChecksumSHA1, complete.ChecksumSHA256))
					assert.Equal(t, types.ChecksumTypeComposite, complete.ChecksumType)

					head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: new(bucket), Key: new(key), ChecksumMode: types.ChecksumModeEnabled})
					require.NoError(t, err)
					assert.Equal(t, composite, pickChecksum(algorithm, head.ChecksumCRC32, head.ChecksumCRC32C, head.ChecksumCRC64NVME, head.ChecksumSHA1, head.ChecksumSHA256))
					assert.Equal(t, types.ChecksumTypeComposite, head.ChecksumType)
					requireValidatedObjectData(ctx, t, client, bucket, key, whole)

					t.Run("PartNumber", func(t *testing.T) {
						get, err := client.GetObject(ctx, &s3.GetObjectInput{
							Bucket: new(bucket), Key: new(key), PartNumber: new(int32(2)), ChecksumMode: types.ChecksumModeEnabled,
						})
						require.NoError(t, err)
						assert.Equal(t, int32(3), aws.ToInt32(get.PartsCount))
						assert.Equal(t, "bytes 5242880-10485759/10485771", aws.ToString(get.ContentRange))
						assert.Equal(t, partSums[1], pickChecksum(algorithm, get.ChecksumCRC32, get.ChecksumCRC32C, get.ChecksumCRC64NVME, get.ChecksumSHA1, get.ChecksumSHA256))
						var body bytes.Buffer
						_, err = body.ReadFrom(get.Body)
						require.NoError(t, err)
						require.NoError(t, get.Body.Close())
						assert.Equal(t, parts[1], body.Bytes())

						head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: new(bucket), Key: new(key), PartNumber: new(int32(3))})
						require.NoError(t, err)
						assert.Equal(t, int64(len(parts[2])), aws.ToInt64(head.ContentLength))
						assert.Equal(t, int32(3), aws.ToInt32(head.PartsCount))

						_, err = client.GetObject(ctx, &s3.GetObjectInput{Bucket: new(bucket), Key: new(key), PartNumber: new(int32(4))})
						requireS3Error(t, err, http.StatusRequestedRangeNotSatisfiable, "InvalidPartNumber")
					})
				})
			}
		})

		t.Run("FullObjectMultipartUpload", func(t *testing.T) {
			for _, tc := range []struct {
				name string
				// algorithm is empty for the gateway's default.
				algorithm types.ChecksumAlgorithm
			}{
				{name: "CRC32", algorithm: types.ChecksumAlgorithmCrc32},
				{name: "CRC32C", algorithm: types.ChecksumAlgorithmCrc32c},
				{name: "CRC64NVME", algorithm: types.ChecksumAlgorithmCrc64nvme},
				{name: "Default"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					key := "full-object-" + tc.name
					upload := client
					algorithm := tc.algorithm
					create := &s3.CreateMultipartUploadInput{Bucket: new(bucket), Key: new(key)}
					if algorithm == "" {
						upload = s3.New(client.Options(), withoutChecksum)
						algorithm = types.ChecksumAlgorithmCrc64nvme
					} else {
						create.ChecksumAlgorithm = algorithm
						create.ChecksumType = types.ChecksumTypeFullObject
					}
					want := checksumOf(algorithm, whole)

					created, err := upload.CreateMultipartUpload(ctx, create)
					require.NoError(t, err)
					var completed []types.CompletedPart
					for i, part := range parts {
						partNumber := new(int32(i + 1))
						uploaded, err := upload.UploadPart(ctx, &s3.UploadPartInput{
							Bucket: new(bucket), Key: new(key), UploadId: created.UploadId, PartNumber: partNumber,
							Body: bytes.NewReader(part), ChecksumAlgorithm: create.ChecksumAlgorithm,
						})
						require.NoError(t, err)
						completed = append(completed, types.CompletedPart{PartNumber: partNumber, ETag: uploaded.ETag})
					}

					// The gateway checks the whole object's checksum against the one it combines from
					// the parts'. An upload with the default algorithm ignores it, like S3.
					input := &s3.CompleteMultipartUploadInput{
						Bucket: new(bucket), Key: new(key), UploadId: created.UploadId,
						MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
					}
					if tc.algorithm != "" {
						input.ChecksumType = types.ChecksumTypeFullObject
						switch algorithm {
						case types.ChecksumAlgorithmCrc32:
							input.ChecksumCRC32 = new(want)
						case types.ChecksumAlgorithmCrc32c:
							input.ChecksumCRC32C = new(want)
						default:
							input.ChecksumCRC64NVME = new(want)
						}
					}
					complete, err := upload.CompleteMultipartUpload(ctx, input)
					require.NoError(t, err)
					assert.Equal(t, want, pickChecksum(algorithm, complete.ChecksumCRC32, complete.ChecksumCRC32C, complete.ChecksumCRC64NVME, nil, nil))
					assert.Equal(t, types.ChecksumTypeFullObject, complete.ChecksumType)

					head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: new(bucket), Key: new(key), ChecksumMode: types.ChecksumModeEnabled})
					require.NoError(t, err)
					assert.Equal(t, want, pickChecksum(algorithm, head.ChecksumCRC32, head.ChecksumCRC32C, head.ChecksumCRC64NVME, nil, nil))
					assert.Equal(t, types.ChecksumTypeFullObject, head.ChecksumType)
					requireValidatedObjectData(ctx, t, client, bucket, key, whole)
				})
			}
		})
	})
}

func requireObjectData(ctx *testcontext.Context, t *testing.T, client *s3.Client, bucket, key string, want []byte) {
	data, err := getObjectData(ctx, client, bucket, key, "")
	require.NoError(t, err)
	require.Equal(t, want, data)
}

func checksumOf(algorithm types.ChecksumAlgorithm, data []byte) string {
	var h hash.Hash
	switch algorithm {
	case types.ChecksumAlgorithmCrc32:
		h = crc32.NewIEEE()
	case types.ChecksumAlgorithmCrc32c:
		h = crc32.New(crc32.MakeTable(crc32.Castagnoli))
	case types.ChecksumAlgorithmCrc64nvme:
		h = crc64.New(crc64.MakeTable(0x9a6c9329ac4bc9b5))
	case types.ChecksumAlgorithmSha1:
		h = sha1.New()
	case types.ChecksumAlgorithmSha256:
		h = sha256.New()
	default:
		panic("unknown checksum algorithm " + string(algorithm))
	}
	_, _ = h.Write(data)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// compositeChecksum returns the checksum with algorithm of the parts' checksums, without the
// part count.
func compositeChecksum(t *testing.T, algorithm types.ChecksumAlgorithm, partSums []string) string {
	var raw []byte
	for _, sum := range partSums {
		value, err := base64.StdEncoding.DecodeString(sum)
		require.NoError(t, err)
		raw = append(raw, value...)
	}
	return checksumOf(algorithm, raw)
}

// requireValidatedObjectData reads an object with checksum mode enabled, so that the SDK checks the
// data against the object's checksum where it can.
func requireValidatedObjectData(ctx *testcontext.Context, t *testing.T, client *s3.Client, bucket, key string, want []byte) {
	get, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: new(bucket), Key: new(key), ChecksumMode: types.ChecksumModeEnabled})
	require.NoError(t, err)
	data, err := io.ReadAll(get.Body)
	require.NoError(t, err)
	require.NoError(t, get.Body.Close())
	require.Equal(t, want, data)
}

func pickChecksum(algorithm types.ChecksumAlgorithm, crc32, crc32c, crc64nvme, sha1, sha256 *string) string {
	return aws.ToString(map[types.ChecksumAlgorithm]*string{
		types.ChecksumAlgorithmCrc32:     crc32,
		types.ChecksumAlgorithmCrc32c:    crc32c,
		types.ChecksumAlgorithmCrc64nvme: crc64nvme,
		types.ChecksumAlgorithmSha1:      sha1,
		types.ChecksumAlgorithmSha256:    sha256,
	}[algorithm])
}
