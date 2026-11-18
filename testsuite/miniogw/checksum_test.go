// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"io"
	"slices"
	"testing"

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/gateway/api/objectlayer"
	"storj.io/gateway/miniogw"
	"storj.io/gateway/miniogw/minioobjectlayer"
	minio "storj.io/minio/cmd"
	"storj.io/minio/pkg/hash"
	"storj.io/storj/private/testplanet"
)

// checksumReader is a hash.Reader of data that computes its MD5 and CRC32, like the api
// package's reader of a request with an X-Amz-Checksum-Crc32 header.
type checksumReader struct {
	*bytes.Reader
	data []byte
}

func newChecksumReader(data []byte) *objectlayer.PutObjReader {
	return objectlayer.NewPutObjReader(checksumReader{Reader: bytes.NewReader(data), data: data})
}

func (r checksumReader) Size() int64       { return int64(len(r.data)) }
func (r checksumReader) ActualSize() int64 { return int64(len(r.data)) }

func (r checksumReader) Checksums() (map[hash.Algorithm][]byte, error) {
	sum := md5.Sum(r.data)
	return map[hash.Algorithm][]byte{
		hash.AlgorithmMD5:   sum[:],
		hash.AlgorithmCRC32: binary.BigEndian.AppendUint32(nil, crc32.ChecksumIEEE(r.data)),
	}, nil
}

func TestPutObjectChecksum(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
	}, func(t *testing.T, tCtx *testcontext.Context, planet *testplanet.Planet) {
		sat, upl := planet.Satellites[0], planet.Uplinks[0]

		project, err := upl.OpenProject(tCtx, sat)
		require.NoError(t, err)
		defer tCtx.Check(project.Close)

		ctx := miniogw.WithCredentials(tCtx, project, miniogw.CredentialsInfo{})
		_, err = project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		for _, size := range []memory.Size{memory.KiB, 10 * memory.KiB} {
			data := testrand.BytesInt(size.Int())
			crc32Value := base64.StdEncoding.EncodeToString(binary.BigEndian.AppendUint32(nil, crc32.ChecksumIEEE(data)))

			for _, tc := range []struct {
				satellite, gateway bool
			}{
				{satellite: true, gateway: true},
				{satellite: true, gateway: false},
				{satellite: false, gateway: false},
				{satellite: false, gateway: true},
			} {
				t.Run(fmt.Sprintf("%s satellite %t gateway %t", size, tc.satellite, tc.gateway), func(t *testing.T) {
					sat.API.Metainfo.Endpoint.TestingSetChecksumsEnabled(tc.satellite)

					config := defaultS3CompatibilityConfig
					config.ObjectChecksums = tc.gateway
					layer, err := miniogw.NewStorjGateway(config).NewObjectLayer()
					require.NoError(t, err)
					defer func() { require.NoError(t, layer.Shutdown(ctx)) }()

					key := fmt.Sprintf("%s-%t-%t", size, tc.satellite, tc.gateway)
					info, err := layer.PutObject(ctx, testBucket, key, newChecksumReader(data), objectlayer.ObjectOptions{
						ChecksumAlgorithm: objectlayer.ChecksumAlgorithmCRC32,
					})
					if tc.gateway && !tc.satellite {
						require.ErrorAs(t, err, new(minio.NotImplemented))

						// Nothing is left behind.
						_, err = layer.GetObjectInfo(ctx, testBucket, key, objectlayer.ObjectOptions{})
						require.ErrorAs(t, err, new(minio.ObjectNotFound))
						return
					}
					require.NoError(t, err)

					var want objectlayer.Checksum
					if tc.gateway {
						want = objectlayer.Checksum{Algorithm: objectlayer.ChecksumAlgorithmCRC32, Value: crc32Value}
					}
					require.Equal(t, want, info.Checksum)

					info, err = layer.GetObjectInfo(ctx, testBucket, key, objectlayer.ObjectOptions{})
					require.NoError(t, err)
					require.Equal(t, want, info.Checksum)

					reader, err := layer.GetObjectNInfo(ctx, testBucket, key, nil, nil, 0, objectlayer.ObjectOptions{})
					require.NoError(t, err)
					require.Equal(t, want, reader.ObjInfo.Checksum)
					require.NoError(t, reader.Close())
				})
			}
		}
	})
}

func TestPutObjectDefaultChecksum(t *testing.T) {
	_, err := miniogw.NewStorjGateway(miniogw.S3CompatibilityConfig{ObjectChecksums: true, DefaultChecksumAlgorithm: "MD5"}).NewObjectLayer()
	require.Error(t, err)

	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
	}, func(t *testing.T, tCtx *testcontext.Context, planet *testplanet.Planet) {
		sat, upl := planet.Satellites[0], planet.Uplinks[0]
		sat.API.Metainfo.Endpoint.TestingSetChecksumsEnabled(true)

		project, err := upl.OpenProject(tCtx, sat)
		require.NoError(t, err)
		defer tCtx.Check(project.Close)

		ctx := miniogw.WithCredentials(tCtx, project, miniogw.CredentialsInfo{})
		_, err = project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		data := testrand.BytesInt(10 * memory.KiB.Int())
		crc32Value := base64.StdEncoding.EncodeToString(binary.BigEndian.AppendUint32(nil, crc32.ChecksumIEEE(data)))
		crc64Value := base64.StdEncoding.EncodeToString(binary.BigEndian.AppendUint64(nil, crc64.Checksum(data, crc64.MakeTable(0x9a6c9329ac4bc9b5))))

		for _, tc := range []struct {
			name             string
			objectChecksums  bool
			defaultAlgorithm string
			requested        objectlayer.ChecksumAlgorithm
			want             objectlayer.Checksum
		}{
			{"default", true, "CRC64NVME", objectlayer.ChecksumAlgorithmNone, objectlayer.Checksum{Algorithm: objectlayer.ChecksumAlgorithmCRC64NVME, Value: crc64Value}},
			{"default with checksums off", false, "CRC64NVME", objectlayer.ChecksumAlgorithmNone, objectlayer.Checksum{}},
			{"no default", true, "", objectlayer.ChecksumAlgorithmNone, objectlayer.Checksum{}},
			{"requested over default", true, "CRC64NVME", objectlayer.ChecksumAlgorithmCRC32, objectlayer.Checksum{Algorithm: objectlayer.ChecksumAlgorithmCRC32, Value: crc32Value}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				config := defaultS3CompatibilityConfig
				config.ObjectChecksums = tc.objectChecksums
				config.DefaultChecksumAlgorithm = tc.defaultAlgorithm
				layer, err := miniogw.NewStorjGateway(config).NewObjectLayer()
				require.NoError(t, err)
				defer func() { require.NoError(t, layer.Shutdown(ctx)) }()

				info, err := layer.PutObject(ctx, testBucket, tc.name, newChecksumReader(data), objectlayer.ObjectOptions{ChecksumAlgorithm: tc.requested})
				require.NoError(t, err)
				require.Equal(t, tc.want, info.Checksum)

				info, err = layer.GetObjectInfo(ctx, testBucket, tc.name, objectlayer.ObjectOptions{})
				require.NoError(t, err)
				require.Equal(t, tc.want, info.Checksum)
			})
		}
	})
}

// TestObjectChecksumsOff checks that without object checksums, the checksums of objects put while
// they were on aren't reported, like before the gateway stored any, and are kept.
func TestObjectChecksumsOff(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
	}, func(t *testing.T, tCtx *testcontext.Context, planet *testplanet.Planet) {
		sat, upl := planet.Satellites[0], planet.Uplinks[0]
		sat.API.Metainfo.Endpoint.TestingSetChecksumsEnabled(true)

		project, err := upl.OpenProject(tCtx, sat)
		require.NoError(t, err)
		defer tCtx.Check(project.Close)

		ctx := miniogw.WithCredentials(tCtx, project, miniogw.CredentialsInfo{})
		_, err = project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		config := defaultS3CompatibilityConfig
		config.ObjectChecksums = true
		on, err := miniogw.NewStorjGateway(config).NewObjectLayer()
		require.NoError(t, err)
		defer func() { require.NoError(t, on.Shutdown(ctx)) }()
		config.ObjectChecksums = false
		off, err := miniogw.NewStorjGateway(config).NewObjectLayer()
		require.NoError(t, err)
		defer func() { require.NoError(t, off.Shutdown(ctx)) }()

		_, err = on.PutObject(ctx, testBucket, testFile, newChecksumReader([]byte("hello world")), objectlayer.ObjectOptions{
			ChecksumAlgorithm: objectlayer.ChecksumAlgorithmCRC32,
		})
		require.NoError(t, err)
		want := objectlayer.Checksum{Algorithm: objectlayer.ChecksumAlgorithmCRC32, Value: "DUoRhQ=="}

		info, err := off.GetObjectInfo(ctx, testBucket, testFile, objectlayer.ObjectOptions{})
		require.NoError(t, err)
		require.Equal(t, objectlayer.Checksum{}, info.Checksum)
		reader, err := off.GetObjectNInfo(ctx, testBucket, testFile, nil, nil, 0, objectlayer.ObjectOptions{})
		require.NoError(t, err)
		require.Equal(t, objectlayer.Checksum{}, reader.ObjInfo.Checksum)
		require.NoError(t, reader.Close())

		_, err = off.PutObjectTags(ctx, testBucket, testFile, "key=value", objectlayer.ObjectOptions{})
		require.NoError(t, err)
		info, err = on.GetObjectInfo(ctx, testBucket, testFile, objectlayer.ObjectOptions{})
		require.NoError(t, err)
		require.Equal(t, want, info.Checksum)
	})
}

func TestListObjectChecksums(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
	}, func(t *testing.T, tCtx *testcontext.Context, planet *testplanet.Planet) {
		sat, upl := planet.Satellites[0], planet.Uplinks[0]
		sat.API.Metainfo.Endpoint.TestingSetChecksumsEnabled(true)

		project, err := upl.OpenProject(tCtx, sat)
		require.NoError(t, err)
		defer tCtx.Check(project.Close)

		ctx := miniogw.WithCredentials(tCtx, project, miniogw.CredentialsInfo{})
		_, err = project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		config := defaultS3CompatibilityConfig
		config.ObjectChecksums = true
		layer, err := miniogw.NewStorjGateway(config).NewObjectLayer()
		require.NoError(t, err)
		defer func() { require.NoError(t, layer.Shutdown(ctx)) }()
		config.ObjectChecksums = false
		off, err := miniogw.NewStorjGateway(config).NewObjectLayer()
		require.NoError(t, err)
		defer func() { require.NoError(t, off.Shutdown(ctx)) }()

		data := []byte("hello world")
		_, err = layer.PutObject(ctx, testBucket, "dir/object", newChecksumReader(data), objectlayer.ObjectOptions{
			ChecksumAlgorithm: objectlayer.ChecksumAlgorithmCRC32,
		})
		require.NoError(t, err)
		want := objectlayer.Checksum{Algorithm: objectlayer.ChecksumAlgorithmCRC32, Value: "DUoRhQ=="}

		for _, tc := range []struct {
			name, prefix, delimiter string
		}{
			{"fast", "", ""},
			{"fast with delimiter", "dir/", "/"},
			{"single", "dir/object", "/"},
			{"exhaustive", "", "-"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				list, err := layer.ListObjectsV2(ctx, testBucket, tc.prefix, "", tc.delimiter, 1000, false, "")
				require.NoError(t, err)
				require.Len(t, list.Objects, 1)
				require.Equal(t, want, list.Objects[0].Checksum)

				// Without object checksums, they aren't listed.
				list, err = off.ListObjectsV2(ctx, testBucket, tc.prefix, "", tc.delimiter, 1000, false, "")
				require.NoError(t, err)
				require.Len(t, list.Objects, 1)
				require.Equal(t, objectlayer.Checksum{}, list.Objects[0].Checksum)
			})
		}

		versions, err := layer.ListObjectVersions(ctx, testBucket, "", "", "", "", 1000)
		require.NoError(t, err)
		require.Len(t, versions.Objects, 1)
		require.Equal(t, want, versions.Objects[0].Checksum)
		versions, err = off.ListObjectVersions(ctx, testBucket, "", "", "", "", 1000)
		require.NoError(t, err)
		require.Len(t, versions.Objects, 1)
		require.Equal(t, objectlayer.Checksum{}, versions.Objects[0].Checksum)
	})
}

func TestMetadataUpdatesKeepChecksum(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
	}, func(t *testing.T, tCtx *testcontext.Context, planet *testplanet.Planet) {
		sat, upl := planet.Satellites[0], planet.Uplinks[0]
		sat.API.Metainfo.Endpoint.TestingSetChecksumsEnabled(true)

		project, err := upl.OpenProject(tCtx, sat)
		require.NoError(t, err)
		defer tCtx.Check(project.Close)

		ctx := miniogw.WithCredentials(tCtx, project, miniogw.CredentialsInfo{})
		_, err = project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		config := defaultS3CompatibilityConfig
		config.ObjectChecksums = true
		layer, err := miniogw.NewStorjGateway(config).NewObjectLayer()
		require.NoError(t, err)
		defer func() { require.NoError(t, layer.Shutdown(ctx)) }()

		info, err := layer.PutObject(ctx, testBucket, testFile, newChecksumReader([]byte("hello world")), objectlayer.ObjectOptions{
			ChecksumAlgorithm: objectlayer.ChecksumAlgorithmCRC32,
		})
		require.NoError(t, err)
		want := objectlayer.Checksum{Algorithm: objectlayer.ChecksumAlgorithmCRC32, Value: "DUoRhQ=="}
		require.Equal(t, want, info.Checksum)

		requireChecksum := func(t *testing.T) {
			t.Helper()
			info, err := layer.GetObjectInfo(ctx, testBucket, testFile, objectlayer.ObjectOptions{})
			require.NoError(t, err)
			require.Equal(t, want, info.Checksum)
		}

		_, err = layer.PutObjectTags(ctx, testBucket, testFile, "key=value", objectlayer.ObjectOptions{})
		require.NoError(t, err)
		requireChecksum(t)

		_, err = layer.DeleteObjectTags(ctx, testBucket, testFile, objectlayer.ObjectOptions{})
		require.NoError(t, err)
		requireChecksum(t)

		// Copying an object onto itself replaces its metadata.
		info.UserDefined = map[string]string{"x-amz-meta-key": "value"}
		_, err = layer.CopyObject(ctx, testBucket, testFile, testBucket, testFile, info, objectlayer.ObjectOptions{}, objectlayer.ObjectOptions{})
		require.NoError(t, err)
		requireChecksum(t)
	})
}

func TestCopyObjectChecksum(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
	}, func(t *testing.T, tCtx *testcontext.Context, planet *testplanet.Planet) {
		sat, upl := planet.Satellites[0], planet.Uplinks[0]
		sat.API.Metainfo.Endpoint.TestingSetChecksumsEnabled(true)

		project, err := upl.OpenProject(tCtx, sat)
		require.NoError(t, err)
		defer tCtx.Check(project.Close)

		ctx := miniogw.WithCredentials(tCtx, project, miniogw.CredentialsInfo{})
		_, err = project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		want := objectlayer.Checksum{Algorithm: objectlayer.ChecksumAlgorithmCRC32, Value: "DUoRhQ=="}
		newLayer := func(t *testing.T, objectChecksums bool) minioobjectlayer.ObjectLayer {
			config := defaultS3CompatibilityConfig
			config.ObjectChecksums = objectChecksums
			layer, err := miniogw.NewStorjGateway(config).NewObjectLayer()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, layer.Shutdown(ctx)) })
			return layer
		}
		layer := newLayer(t, true)
		srcInfo, err := layer.PutObject(ctx, testBucket, testFile, newChecksumReader([]byte("hello world")), objectlayer.ObjectOptions{
			ChecksumAlgorithm: objectlayer.ChecksumAlgorithmCRC32,
		})
		require.NoError(t, err)

		copyWith := func(layer minioobjectlayer.ObjectLayer, dst string, algorithm objectlayer.ChecksumAlgorithm) (objectlayer.ObjectInfo, error) {
			return layer.CopyObject(ctx, testBucket, testFile, testBucket, dst, srcInfo, objectlayer.ObjectOptions{}, objectlayer.ObjectOptions{ChecksumAlgorithm: algorithm})
		}

		info, err := copyWith(layer, "copy", objectlayer.ChecksumAlgorithmNone)
		require.NoError(t, err)
		require.Equal(t, want, info.Checksum)
		info, err = layer.GetObjectInfo(ctx, testBucket, "copy", objectlayer.ObjectOptions{})
		require.NoError(t, err)
		require.Equal(t, want, info.Checksum)

		info, err = copyWith(layer, "copy-same", objectlayer.ChecksumAlgorithmCRC32)
		require.NoError(t, err)
		require.Equal(t, want, info.Checksum)

		_, err = copyWith(layer, "copy-different", objectlayer.ChecksumAlgorithmSHA256)
		require.ErrorIs(t, err, objectlayer.ErrCopyChecksumAlgorithm)
		_, err = layer.GetObjectInfo(ctx, testBucket, "copy-different", objectlayer.ObjectOptions{})
		require.ErrorAs(t, err, new(minio.ObjectNotFound))

		// Copying an object onto itself takes another path.
		srcInfo.UserDefined = map[string]string{"x-amz-meta-key": "value"}
		_, err = copyWith(layer, testFile, objectlayer.ChecksumAlgorithmSHA256)
		require.ErrorIs(t, err, objectlayer.ErrCopyChecksumAlgorithm)

		// Without object checksums, the requested algorithm is ignored, and the copy keeps the
		// source's checksum without reporting it.
		info, err = copyWith(newLayer(t, false), "copy-ignored", objectlayer.ChecksumAlgorithmSHA256)
		require.NoError(t, err)
		require.Equal(t, objectlayer.Checksum{}, info.Checksum)
		info, err = layer.GetObjectInfo(ctx, testBucket, "copy-ignored", objectlayer.ObjectOptions{})
		require.NoError(t, err)
		require.Equal(t, want, info.Checksum)
	})
}

func TestMultipartChecksums(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		// "upload lookups" counts satellite calls in monkit's stats, which other tests running at
		// the same time would add to.
		NonParallel: true,
	}, func(t *testing.T, tCtx *testcontext.Context, planet *testplanet.Planet) {
		sat, upl := planet.Satellites[0], planet.Uplinks[0]
		sat.API.Metainfo.Endpoint.TestingSetChecksumsEnabled(true)

		project, err := upl.OpenProject(tCtx, sat)
		require.NoError(t, err)
		defer tCtx.Check(project.Close)

		ctx := miniogw.WithCredentials(tCtx, project, miniogw.CredentialsInfo{Access: upl.Access[sat.ID()]})
		_, err = project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		newLayer := func(t *testing.T, objectChecksums bool) minioobjectlayer.ObjectLayer {
			config := defaultS3CompatibilityConfig
			config.ObjectChecksums = objectChecksums
			config.DefaultChecksumAlgorithm = "CRC64NVME"
			layer, err := miniogw.NewStorjGateway(config).NewObjectLayer()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, layer.Shutdown(ctx)) })
			return layer
		}
		layer := newLayer(t, true)

		parts := [][]byte{testrand.BytesInt(10 * memory.KiB.Int()), []byte("hello world")}
		whole := bytes.Join(parts, nil)
		b64 := base64.StdEncoding.EncodeToString
		crc32Of := func(data []byte) []byte { return binary.BigEndian.AppendUint32(nil, crc32.ChecksumIEEE(data)) }
		crc64Of := func(data []byte) []byte {
			return binary.BigEndian.AppendUint64(nil, crc64.Checksum(data, crc64.MakeTable(0x9a6c9329ac4bc9b5)))
		}
		const crc32Algorithm = objectlayer.ChecksumAlgorithmCRC32

		// upload puts parts with a CRC32 checksum, or without one, and returns the parts to name.
		upload := func(t *testing.T, layer minioobjectlayer.ObjectLayer, key string, opts objectlayer.ObjectOptions, requested objectlayer.ChecksumAlgorithm) (string, []objectlayer.CompletePart) {
			created, err := layer.NewMultipartUpload(ctx, testBucket, key, opts)
			require.NoError(t, err)
			uploadID := created.UploadID
			var completeParts []objectlayer.CompletePart
			for i, part := range parts {
				info, err := layer.PutObjectPart(ctx, testBucket, key, uploadID, i+1, newChecksumReader(part), objectlayer.ObjectOptions{ChecksumAlgorithm: requested})
				require.NoError(t, err)
				completeParts = append(completeParts, objectlayer.CompletePart{PartNumber: i + 1, ETag: info.ETag, Checksum: info.Checksum})
			}
			return uploadID, completeParts
		}

		t.Run("composite", func(t *testing.T) {
			opts := objectlayer.ObjectOptions{ChecksumAlgorithm: crc32Algorithm}
			uploadID, completeParts := upload(t, layer, "composite", opts, crc32Algorithm)

			list, err := layer.ListObjectParts(ctx, testBucket, "composite", uploadID, 0, 1000, objectlayer.ObjectOptions{})
			require.NoError(t, err)
			require.Equal(t, objectlayer.Checksum{Algorithm: crc32Algorithm, Composite: true}, list.Checksum)
			require.Len(t, list.Parts, 2)
			for i, part := range list.Parts {
				require.Equal(t, objectlayer.Checksum{Algorithm: crc32Algorithm, Value: b64(crc32Of(parts[i]))}, part.Checksum)
			}
			// Like S3, a page without parts still has the upload's checksum.
			list, err = layer.ListObjectParts(ctx, testBucket, "composite", uploadID, 2, 1000, objectlayer.ObjectOptions{})
			require.NoError(t, err)
			require.Empty(t, list.Parts)
			require.Equal(t, objectlayer.Checksum{Algorithm: crc32Algorithm, Composite: true}, list.Checksum)

			uploads, err := layer.ListMultipartUploads(ctx, testBucket, "composite", "", "", "", 1000)
			require.NoError(t, err)
			require.Len(t, uploads.Uploads, 1)
			require.Equal(t, objectlayer.Checksum{Algorithm: crc32Algorithm, Composite: true}, uploads.Uploads[0].Checksum)

			// A part of an upload with composite checksums needs one.
			_, err = layer.PutObjectPart(ctx, testBucket, "composite", uploadID, 3, newChecksumReader(parts[1]), objectlayer.ObjectOptions{})
			require.ErrorAs(t, err, new(objectlayer.PartChecksumAlgorithmMismatch))

			withoutChecksums := slices.Clone(completeParts)
			withoutChecksums[0].Checksum = objectlayer.Checksum{}
			_, err = layer.CompleteMultipartUpload(ctx, testBucket, "composite", uploadID, withoutChecksums, objectlayer.ObjectOptions{})
			require.ErrorAs(t, err, new(objectlayer.MissingPartChecksum))

			_, err = layer.CompleteMultipartUpload(ctx, testBucket, "composite", uploadID, completeParts, objectlayer.ObjectOptions{ChecksumType: objectlayer.ChecksumTypeFullObject})
			require.ErrorAs(t, err, new(objectlayer.ChecksumTypeMismatch))

			want := objectlayer.Checksum{Algorithm: crc32Algorithm, Composite: true, Value: b64(crc32Of(append(crc32Of(parts[0]), crc32Of(parts[1])...))) + "-2"}
			info, err := layer.CompleteMultipartUpload(ctx, testBucket, "composite", uploadID, completeParts, objectlayer.ObjectOptions{})
			require.NoError(t, err)
			require.Equal(t, want, info.Checksum)
			info, err = layer.GetObjectInfo(ctx, testBucket, "composite", objectlayer.ObjectOptions{})
			require.NoError(t, err)
			require.Equal(t, want, info.Checksum)
		})

		t.Run("full object", func(t *testing.T) {
			opts := objectlayer.ObjectOptions{ChecksumAlgorithm: crc32Algorithm, ChecksumType: objectlayer.ChecksumTypeFullObject}
			uploadID, completeParts := upload(t, layer, "full", opts, objectlayer.ChecksumAlgorithmNone)
			for i, part := range completeParts {
				require.Equal(t, objectlayer.Checksum{Algorithm: crc32Algorithm, Value: b64(crc32Of(parts[i]))}, part.Checksum)
			}

			// Like S3, a part requested with another algorithm is refused.
			_, err := layer.PutObjectPart(ctx, testBucket, "full", uploadID, 3, newChecksumReader(parts[1]), objectlayer.ObjectOptions{ChecksumAlgorithm: objectlayer.ChecksumAlgorithmSHA256})
			require.ErrorAs(t, err, new(objectlayer.PartChecksumAlgorithmMismatch))

			info, err := layer.CompleteMultipartUpload(ctx, testBucket, "full", uploadID, completeParts, objectlayer.ObjectOptions{
				ChecksumAlgorithm: crc32Algorithm, ChecksumType: objectlayer.ChecksumTypeFullObject, ChecksumValue: b64(crc32Of(whole)),
			})
			require.NoError(t, err)
			require.Equal(t, objectlayer.Checksum{Algorithm: crc32Algorithm, Value: b64(crc32Of(whole))}, info.Checksum)
		})

		t.Run("default", func(t *testing.T) {
			const crc64nvmeAlgorithm = objectlayer.ChecksumAlgorithmCRC64NVME
			uploadDefault := objectlayer.Checksum{Algorithm: crc64nvmeAlgorithm, Default: true}

			// A part with another checksum still gets one with the upload's algorithm.
			uploadID, completeParts := upload(t, layer, "default", objectlayer.ObjectOptions{UserDefined: map[string]string{"foo": "bar"}}, crc32Algorithm)
			for i, part := range completeParts {
				require.Equal(t, objectlayer.Checksum{Algorithm: crc64nvmeAlgorithm, Value: b64(crc64Of(parts[i])), Default: true}, part.Checksum)
			}

			list, err := layer.ListObjectParts(ctx, testBucket, "default", uploadID, 0, 1000, objectlayer.ObjectOptions{})
			require.NoError(t, err)
			require.Equal(t, uploadDefault, list.Checksum)
			for i, part := range list.Parts {
				require.Equal(t, objectlayer.Checksum{Algorithm: crc64nvmeAlgorithm, Value: b64(crc64Of(parts[i])), Default: true}, part.Checksum)
			}
			uploads, err := layer.ListMultipartUploads(ctx, testBucket, "default", "", "", "", 1000)
			require.NoError(t, err)
			require.Len(t, uploads.Uploads, 1)
			require.Equal(t, uploadDefault, uploads.Uploads[0].Checksum)

			// Like S3, which reports the upload as without checksums.
			_, err = layer.CompleteMultipartUpload(ctx, testBucket, "default", uploadID, completeParts, objectlayer.ObjectOptions{})
			require.ErrorAs(t, err, new(objectlayer.InvalidPart))

			for i := range completeParts {
				completeParts[i].Checksum = objectlayer.Checksum{}
			}
			info, err := layer.CompleteMultipartUpload(ctx, testBucket, "default", uploadID, completeParts, objectlayer.ObjectOptions{})
			require.NoError(t, err)
			require.Equal(t, objectlayer.Checksum{Algorithm: crc64nvmeAlgorithm, Value: b64(crc64Of(whole))}, info.Checksum)

			info, err = layer.GetObjectInfo(ctx, testBucket, "default", objectlayer.ObjectOptions{})
			require.NoError(t, err)
			require.Equal(t, "bar", info.UserDefined["foo"])
		})

		// A part put through a gateway with object checksums off has none, which leaves the upload's
		// checksums incomplete, so the upload is completed without one rather than never.
		t.Run("part without checksum", func(t *testing.T) {
			off := newLayer(t, false)
			for _, requested := range []objectlayer.ChecksumAlgorithm{objectlayer.ChecksumAlgorithmNone, crc32Algorithm} {
				key := "partial-" + requested.String()
				created, err := layer.NewMultipartUpload(ctx, testBucket, key, objectlayer.ObjectOptions{ChecksumAlgorithm: requested})
				require.NoError(t, err)
				uploadID := created.UploadID
				var completeParts []objectlayer.CompletePart
				for i, part := range parts {
					partLayer := layer
					if i == 1 {
						partLayer = off
					}
					info, err := partLayer.PutObjectPart(ctx, testBucket, key, uploadID, i+1, newChecksumReader(part), objectlayer.ObjectOptions{ChecksumAlgorithm: requested})
					require.NoError(t, err)
					completePart := objectlayer.CompletePart{PartNumber: i + 1, ETag: info.ETag}
					if requested != objectlayer.ChecksumAlgorithmNone {
						completePart.Checksum = objectlayer.Checksum{Algorithm: requested, Value: b64(crc32Of(part))}
					}
					completeParts = append(completeParts, completePart)
				}

				info, err := layer.CompleteMultipartUpload(ctx, testBucket, key, uploadID, completeParts, objectlayer.ObjectOptions{})
				require.NoError(t, err, requested)
				require.Equal(t, objectlayer.Checksum{}, info.Checksum, requested)
				info, err = layer.GetObjectInfo(ctx, testBucket, key, objectlayer.ObjectOptions{})
				require.NoError(t, err, requested)
				require.Equal(t, objectlayer.Checksum{}, info.Checksum, requested)
			}
		})

		// Parts and part listings read the upload's checksums from its upload ID instead of
		// looking the upload up on the satellite.
		t.Run("upload lookups", func(t *testing.T) {
			lookups := func() (total float64) {
				monkit.Default.Stats(func(key monkit.SeriesKey, field string, value float64) {
					if key.Measurement == "function" && field == "total" &&
						key.Tags.Get("scope") == "storj.io/storj/satellite/metainfo" &&
						key.Tags.Get("name") == "(*Endpoint).GetPendingObjectMetadata" {
						total += value
					}
				})
				return total
			}
			for _, requested := range []objectlayer.ChecksumAlgorithm{crc32Algorithm, objectlayer.ChecksumAlgorithmNone} {
				before := lookups()
				uploadID, completeParts := upload(t, layer, "lookups", objectlayer.ObjectOptions{ChecksumAlgorithm: requested}, requested)
				_, err := layer.ListObjectParts(ctx, testBucket, "lookups", uploadID, 0, 1000, objectlayer.ObjectOptions{})
				require.NoError(t, err)
				require.Equal(t, before, lookups(), requested)

				// Completing still looks the upload up, for its metadata, which shows the count works.
				if requested == objectlayer.ChecksumAlgorithmNone {
					for i := range completeParts {
						completeParts[i].Checksum = objectlayer.Checksum{}
					}
				}
				_, err = layer.CompleteMultipartUpload(ctx, testBucket, "lookups", uploadID, completeParts, objectlayer.ObjectOptions{})
				require.NoError(t, err)
				require.Greater(t, lookups(), before, requested)
			}
		})

		t.Run("copy part", func(t *testing.T) {
			src, err := layer.PutObject(ctx, testBucket, "copy-source", newChecksumReader(whole), objectlayer.ObjectOptions{})
			require.NoError(t, err)

			opts := objectlayer.ObjectOptions{ChecksumAlgorithm: crc32Algorithm, ChecksumType: objectlayer.ChecksumTypeFullObject}
			created, err := layer.NewMultipartUpload(ctx, testBucket, "copy", opts)
			require.NoError(t, err)
			require.Equal(t, objectlayer.Checksum{Algorithm: crc32Algorithm}, created.Checksum)
			info, err := layer.CopyObjectPart(ctx, testBucket, "copy-source", testBucket, "copy", created.UploadID, 1, 0, int64(len(parts[0])), src, objectlayer.ObjectOptions{}, objectlayer.ObjectOptions{})
			require.NoError(t, err)
			require.Equal(t, objectlayer.Checksum{Algorithm: crc32Algorithm, Value: b64(crc32Of(parts[0]))}, info.Checksum)
		})

		t.Run("without object checksums", func(t *testing.T) {
			layer := newLayer(t, false)
			opts := objectlayer.ObjectOptions{ChecksumAlgorithm: crc32Algorithm}
			created, err := layer.NewMultipartUpload(ctx, testBucket, "off", opts)
			require.NoError(t, err)
			require.Equal(t, objectlayer.Checksum{}, created.Checksum)

			uploadID, completeParts := upload(t, layer, "off", opts, objectlayer.ChecksumAlgorithmNone)
			list, err := layer.ListObjectParts(ctx, testBucket, "off", uploadID, 0, 1000, objectlayer.ObjectOptions{})
			require.NoError(t, err)
			require.Equal(t, objectlayer.Checksum{}, list.Checksum)
			info, err := layer.CompleteMultipartUpload(ctx, testBucket, "off", uploadID, completeParts, objectlayer.ObjectOptions{ChecksumType: objectlayer.ChecksumTypeFullObject})
			require.NoError(t, err)
			require.Equal(t, objectlayer.Checksum{}, info.Checksum)
		})
	})
}

func TestObjectParts(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
	}, func(t *testing.T, tCtx *testcontext.Context, planet *testplanet.Planet) {
		sat, upl := planet.Satellites[0], planet.Uplinks[0]
		sat.API.Metainfo.Endpoint.TestingSetChecksumsEnabled(true)

		project, err := upl.OpenProject(tCtx, sat)
		require.NoError(t, err)
		defer tCtx.Check(project.Close)

		ctx := miniogw.WithCredentials(tCtx, project, miniogw.CredentialsInfo{Access: upl.Access[sat.ID()]})
		_, err = project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		config := defaultS3CompatibilityConfig
		config.ObjectChecksums = true
		layer, err := miniogw.NewStorjGateway(config).NewObjectLayer()
		require.NoError(t, err)
		defer func() { require.NoError(t, layer.Shutdown(ctx)) }()
		config.ObjectChecksums = false
		off, err := miniogw.NewStorjGateway(config).NewObjectLayer()
		require.NoError(t, err)
		defer func() { require.NoError(t, off.Shutdown(ctx)) }()

		parts := [][]byte{testrand.BytesInt(10 * memory.KiB.Int()), testrand.BytesInt(memory.KiB.Int()), []byte("hello world")}
		crc32Of := func(data []byte) string {
			return base64.StdEncoding.EncodeToString(binary.BigEndian.AppendUint32(nil, crc32.ChecksumIEEE(data)))
		}

		created, err := layer.NewMultipartUpload(ctx, testBucket, "multipart", objectlayer.ObjectOptions{ChecksumAlgorithm: objectlayer.ChecksumAlgorithmCRC32})
		require.NoError(t, err)
		uploadID := created.UploadID
		var completeParts []objectlayer.CompletePart
		for i, part := range parts {
			info, err := layer.PutObjectPart(ctx, testBucket, "multipart", uploadID, i+1, newChecksumReader(part), objectlayer.ObjectOptions{ChecksumAlgorithm: objectlayer.ChecksumAlgorithmCRC32})
			require.NoError(t, err)
			completeParts = append(completeParts, objectlayer.CompletePart{PartNumber: i + 1, ETag: info.ETag, Checksum: info.Checksum})
		}
		_, err = layer.CompleteMultipartUpload(ctx, testBucket, "multipart", uploadID, completeParts, objectlayer.ObjectOptions{})
		require.NoError(t, err)

		wantPart := objectlayer.ObjectPart{
			PartNumber: 2,
			Offset:     int64(len(parts[0])),
			Size:       int64(len(parts[1])),
			Checksum:   objectlayer.Checksum{Algorithm: objectlayer.ChecksumAlgorithmCRC32, Value: crc32Of(parts[1])},
		}

		whole, err := layer.GetObjectInfo(ctx, testBucket, "multipart", objectlayer.ObjectOptions{})
		require.NoError(t, err)
		require.NotEmpty(t, whole.Checksum.Value)
		info, err := layer.GetObjectInfo(ctx, testBucket, "multipart", objectlayer.ObjectOptions{PartNumber: 2})
		require.NoError(t, err)
		require.Equal(t, 3, info.PartsCount)
		require.Equal(t, []objectlayer.ObjectPart{wantPart}, info.Parts)
		// A part read reports the object's checksum, which the api package sends for an only part.
		require.Equal(t, whole.Checksum, info.Checksum)

		reader, err := layer.GetObjectNInfo(ctx, testBucket, "multipart", nil, nil, 0, objectlayer.ObjectOptions{PartNumber: 2})
		require.NoError(t, err)
		require.Equal(t, []objectlayer.ObjectPart{wantPart}, reader.ObjInfo.Parts)
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		require.Equal(t, parts[1], data)

		// Without object checksums, neither the object nor its parts have checksums.
		info, err = off.GetObjectInfo(ctx, testBucket, "multipart", objectlayer.ObjectOptions{PartNumber: 2})
		require.NoError(t, err)
		require.Equal(t, objectlayer.Checksum{}, info.Checksum)
		require.Equal(t, []objectlayer.ObjectPart{{PartNumber: 2, Offset: wantPart.Offset, Size: wantPart.Size}}, info.Parts)

		// A part that doesn't exist is left for the caller to answer.
		info, err = layer.GetObjectInfo(ctx, testBucket, "multipart", objectlayer.ObjectOptions{PartNumber: 4})
		require.NoError(t, err)
		require.Equal(t, 3, info.PartsCount)
		require.Empty(t, info.Parts)

		info, err = layer.GetObjectInfo(ctx, testBucket, "multipart", objectlayer.ObjectOptions{
			ObjectParts: &objectlayer.ObjectPartsOptions{PartNumberMarker: 1, MaxParts: 1},
		})
		require.NoError(t, err)
		require.Equal(t, []objectlayer.ObjectPart{wantPart}, info.Parts)
		require.True(t, info.PartsTruncated)

		// An empty part reads as empty, at the end of the object.
		created, err = layer.NewMultipartUpload(ctx, testBucket, "empty-part", objectlayer.ObjectOptions{})
		require.NoError(t, err)
		completeParts = nil
		for i, part := range [][]byte{parts[0], nil} {
			info, err := layer.PutObjectPart(ctx, testBucket, "empty-part", created.UploadID, i+1, newChecksumReader(part), objectlayer.ObjectOptions{})
			require.NoError(t, err)
			completeParts = append(completeParts, objectlayer.CompletePart{PartNumber: i + 1, ETag: info.ETag})
		}
		_, err = layer.CompleteMultipartUpload(ctx, testBucket, "empty-part", created.UploadID, completeParts, objectlayer.ObjectOptions{})
		require.NoError(t, err)
		reader, err = layer.GetObjectNInfo(ctx, testBucket, "empty-part", nil, nil, 0, objectlayer.ObjectOptions{PartNumber: 2})
		require.NoError(t, err)
		require.Len(t, reader.ObjInfo.Parts, 1)
		require.Zero(t, reader.ObjInfo.Parts[0].Size)
		data, err = io.ReadAll(reader)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		require.Empty(t, data)

		// An object that wasn't uploaded in parts has none, and part 1 is the entire object.
		_, err = layer.PutObject(ctx, testBucket, "single", newChecksumReader(parts[2]), objectlayer.ObjectOptions{})
		require.NoError(t, err)
		reader, err = layer.GetObjectNInfo(ctx, testBucket, "single", nil, nil, 0, objectlayer.ObjectOptions{PartNumber: 1})
		require.NoError(t, err)
		require.Zero(t, reader.ObjInfo.PartsCount)
		data, err = io.ReadAll(reader)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		require.Equal(t, parts[2], data)
	})
}
