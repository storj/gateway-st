// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package objectlayer_test

import (
	"encoding/base64"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/gateway/api/objectlayer"
	minioHash "storj.io/minio/pkg/hash"
)

func TestParseChecksumAlgorithm(t *testing.T) {
	for name, want := range map[string]objectlayer.ChecksumAlgorithm{
		"":          objectlayer.ChecksumAlgorithmNone,
		"CRC32":     objectlayer.ChecksumAlgorithmCRC32,
		"crc32c":    objectlayer.ChecksumAlgorithmCRC32C,
		"CRC64NVME": objectlayer.ChecksumAlgorithmCRC64NVME,
		"Sha1":      objectlayer.ChecksumAlgorithmSHA1,
		"SHA256":    objectlayer.ChecksumAlgorithmSHA256,
	} {
		got, err := objectlayer.ParseChecksumAlgorithm(name)
		require.NoError(t, err, name)
		assert.Equal(t, want, got, name)
	}
	_, err := objectlayer.ParseChecksumAlgorithm("MD5")
	require.Error(t, err)
}

// hashReader is a minioHash.Reader of data that computed the checksums in sums.
type hashReader struct {
	*strings.Reader
	sums map[minioHash.Algorithm][]byte
}

func (r hashReader) Size() int64                                        { return r.Reader.Size() }
func (r hashReader) ActualSize() int64                                  { return r.Reader.Size() }
func (r hashReader) Checksums() (map[minioHash.Algorithm][]byte, error) { return r.sums, nil }

func TestPutChecksum(t *testing.T) {
	// The checksums of "hello world" that S3 returns.
	values := map[objectlayer.ChecksumAlgorithm]string{
		objectlayer.ChecksumAlgorithmCRC32:     "DUoRhQ==",
		objectlayer.ChecksumAlgorithmCRC32C:    "yZRlqg==",
		objectlayer.ChecksumAlgorithmCRC64NVME: "jSnVw/bqjr4=",
		objectlayer.ChecksumAlgorithmSHA1:      "Kq5sNclPz7QV2+lfQIuc6R7oRu0=",
		objectlayer.ChecksumAlgorithmSHA256:    "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=",
	}
	read := func(t *testing.T, checksum *objectlayer.PutChecksum) objectlayer.Checksum {
		data, err := io.ReadAll(checksum.Reader())
		require.NoError(t, err)
		require.Equal(t, "hello world", string(data))
		got, err := checksum.Checksum()
		require.NoError(t, err)
		return got
	}

	t.Run("default", func(t *testing.T) {
		for algorithm, value := range values {
			data := objectlayer.NewPutObjReader(hashReader{Reader: strings.NewReader("hello world")})
			got := read(t, objectlayer.NewPutChecksum(data, objectlayer.ChecksumAlgorithmNone, algorithm))
			assert.Equal(t, objectlayer.Checksum{Algorithm: algorithm, Value: value}, got, algorithm)
		}
	})

	t.Run("requested", func(t *testing.T) {
		crc32, err := base64.StdEncoding.DecodeString(values[objectlayer.ChecksumAlgorithmCRC32])
		require.NoError(t, err)
		data := objectlayer.NewPutObjReader(hashReader{
			Reader: strings.NewReader("hello world"),
			sums:   map[minioHash.Algorithm][]byte{minioHash.AlgorithmCRC32: crc32},
		})
		// The requested checksum is the one the data computed, not the default.
		got := read(t, objectlayer.NewPutChecksum(data, objectlayer.ChecksumAlgorithmCRC32, objectlayer.ChecksumAlgorithmCRC64NVME))
		assert.Equal(t, objectlayer.Checksum{Algorithm: objectlayer.ChecksumAlgorithmCRC32, Value: values[objectlayer.ChecksumAlgorithmCRC32]}, got)
	})

	t.Run("requested but not computed", func(t *testing.T) {
		data := objectlayer.NewPutObjReader(hashReader{Reader: strings.NewReader("hello world")})
		checksum := objectlayer.NewPutChecksum(data, objectlayer.ChecksumAlgorithmSHA1, objectlayer.ChecksumAlgorithmNone)
		_, err := io.ReadAll(checksum.Reader())
		require.NoError(t, err)
		_, err = checksum.Checksum()
		require.Error(t, err)
	})

	t.Run("none", func(t *testing.T) {
		data := objectlayer.NewPutObjReader(hashReader{Reader: strings.NewReader("hello world")})
		got := read(t, objectlayer.NewPutChecksum(data, objectlayer.ChecksumAlgorithmNone, objectlayer.ChecksumAlgorithmNone))
		assert.Equal(t, objectlayer.Checksum{}, got)
	})
}
