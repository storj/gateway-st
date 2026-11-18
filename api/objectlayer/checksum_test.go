// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package objectlayer_test

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/testrand"
	"storj.io/gateway/api/objectlayer"
	minioHash "storj.io/minio/pkg/hash"
)

// TestCombineChecksumValues checks the checksums that S3 returned for an upload of 5 MiB of 'a'
// and then "hello world".
func TestCombineChecksumValues(t *testing.T) {
	parts := [][]byte{bytes.Repeat([]byte("a"), 5<<20), []byte("hello world")}

	sums := func(newHash func() hash.Hash) []objectlayer.PartChecksum {
		var checksums []objectlayer.PartChecksum
		for _, part := range parts {
			h := newHash()
			_, _ = h.Write(part)
			checksums = append(checksums, objectlayer.PartChecksum{Value: base64.StdEncoding.EncodeToString(h.Sum(nil)), Size: int64(len(part))})
		}
		return checksums
	}
	crc64NVME := crc64.MakeTable(0x9a6c9329ac4bc9b5)

	got, err := objectlayer.FullObjectChecksum(objectlayer.ChecksumAlgorithmCRC32, sums(func() hash.Hash { return crc32.NewIEEE() }))
	require.NoError(t, err)
	assert.Equal(t, objectlayer.Checksum{Algorithm: objectlayer.ChecksumAlgorithmCRC32, Value: "WrTTSg=="}, got)

	got, err = objectlayer.CompositeChecksum(objectlayer.ChecksumAlgorithmCRC32, sums(func() hash.Hash { return crc32.NewIEEE() }))
	require.NoError(t, err)
	assert.Equal(t, objectlayer.Checksum{Algorithm: objectlayer.ChecksumAlgorithmCRC32, Composite: true, Value: "NSvTHQ==-2"}, got)

	got, err = objectlayer.FullObjectChecksum(objectlayer.ChecksumAlgorithmCRC64NVME, sums(func() hash.Hash { return crc64.New(crc64NVME) }))
	require.NoError(t, err)
	assert.Equal(t, objectlayer.Checksum{Algorithm: objectlayer.ChecksumAlgorithmCRC64NVME, Value: "tUz56p+TDl0="}, got)
}

func TestCombineChecksums(t *testing.T) {
	crc64NVME := crc64.MakeTable(0x9a6c9329ac4bc9b5)
	for _, tc := range []struct {
		algorithm objectlayer.ChecksumAlgorithm
		newHash   func() hash.Hash
	}{
		{objectlayer.ChecksumAlgorithmCRC32, func() hash.Hash { return crc32.NewIEEE() }},
		{objectlayer.ChecksumAlgorithmCRC32C, func() hash.Hash { return crc32.New(crc32.MakeTable(crc32.Castagnoli)) }},
		{objectlayer.ChecksumAlgorithmCRC64NVME, func() hash.Hash { return crc64.New(crc64NVME) }},
		{objectlayer.ChecksumAlgorithmSHA1, sha1.New},
		{objectlayer.ChecksumAlgorithmSHA256, sha256.New},
	} {
		t.Run(tc.algorithm.String(), func(t *testing.T) {
			sum := func(data []byte) []byte {
				h := tc.newHash()
				_, _ = h.Write(data)
				return h.Sum(nil)
			}

			// Empty parts and sizes that aren't multiples of 8 bytes are allowed.
			var data []byte
			var parts []objectlayer.PartChecksum
			var partSums []byte
			for _, size := range []int{0, 1, 1000, 7, 0, 4097} {
				part := testrand.BytesInt(size)
				data = append(data, part...)
				parts = append(parts, objectlayer.PartChecksum{Value: base64.StdEncoding.EncodeToString(sum(part)), Size: int64(size)})
				partSums = append(partSums, sum(part)...)
			}

			composite, err := objectlayer.CompositeChecksum(tc.algorithm, parts)
			if tc.algorithm == objectlayer.ChecksumAlgorithmCRC64NVME {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, base64.StdEncoding.EncodeToString(sum(partSums))+"-6", composite.Value)
			}

			full, err := objectlayer.FullObjectChecksum(tc.algorithm, parts)
			switch tc.algorithm {
			case objectlayer.ChecksumAlgorithmSHA1, objectlayer.ChecksumAlgorithmSHA256:
				require.Error(t, err)
			default:
				require.NoError(t, err)
				assert.Equal(t, base64.StdEncoding.EncodeToString(sum(data)), full.Value)
			}
		})
	}

	t.Run("wrong size", func(t *testing.T) {
		_, err := objectlayer.FullObjectChecksum(objectlayer.ChecksumAlgorithmCRC32, []objectlayer.PartChecksum{
			{Value: base64.StdEncoding.EncodeToString(binary.BigEndian.AppendUint64(nil, 1)), Size: 1},
		})
		require.Error(t, err)
	})
}

// TestCombineChecksumsOfLargeParts checks a part of 512 MiB or more, whose bit length is 2^32 or
// more, beyond the width of a 32-bit CRC.
func TestCombineChecksumsOfLargeParts(t *testing.T) {
	const size = 1<<29 + 5
	crc64NVME := crc64.MakeTable(0x9a6c9329ac4bc9b5)
	for _, tc := range []struct {
		algorithm objectlayer.ChecksumAlgorithm
		newHash   func() hash.Hash
	}{
		{objectlayer.ChecksumAlgorithmCRC32, func() hash.Hash { return crc32.NewIEEE() }},
		{objectlayer.ChecksumAlgorithmCRC32C, func() hash.Hash { return crc32.New(crc32.MakeTable(crc32.Castagnoli)) }},
		{objectlayer.ChecksumAlgorithmCRC64NVME, func() hash.Hash { return crc64.New(crc64NVME) }},
	} {
		t.Run(tc.algorithm.String(), func(t *testing.T) {
			first, second, whole := tc.newHash(), tc.newHash(), tc.newHash()
			_, _ = first.Write([]byte("x"))
			_, _ = whole.Write([]byte("x"))
			zeros := make([]byte, 1<<20)
			for written := 0; written < size; written += len(zeros) {
				chunk := zeros[:min(len(zeros), size-written)]
				_, _ = second.Write(chunk)
				_, _ = whole.Write(chunk)
			}

			full, err := objectlayer.FullObjectChecksum(tc.algorithm, []objectlayer.PartChecksum{
				{Value: base64.StdEncoding.EncodeToString(first.Sum(nil)), Size: 1},
				{Value: base64.StdEncoding.EncodeToString(second.Sum(nil)), Size: size},
			})
			require.NoError(t, err)
			assert.Equal(t, base64.StdEncoding.EncodeToString(whole.Sum(nil)), full.Value)
		})
	}
}

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
