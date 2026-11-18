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
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/gateway/api/objectlayer"
	"storj.io/gateway/miniogw"
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
