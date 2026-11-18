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

func TestNewUploadChecksum(t *testing.T) {
	const (
		none      = objectlayer.ChecksumAlgorithmNone
		crc32     = objectlayer.ChecksumAlgorithmCRC32
		crc64nvme = objectlayer.ChecksumAlgorithmCRC64NVME
		sha256    = objectlayer.ChecksumAlgorithmSHA256
	)
	for _, tc := range []struct {
		algorithm, defaultAlgorithm objectlayer.ChecksumAlgorithm
		checksumType                objectlayer.ChecksumType
		want                        objectlayer.Checksum
	}{
		{none, crc64nvme, objectlayer.ChecksumTypeNone, objectlayer.Checksum{Algorithm: crc64nvme, Default: true}},
		{none, none, objectlayer.ChecksumTypeNone, objectlayer.Checksum{}},
		{crc32, crc64nvme, objectlayer.ChecksumTypeNone, objectlayer.Checksum{Algorithm: crc32, Composite: true}},
		{crc32, crc64nvme, objectlayer.ChecksumTypeFullObject, objectlayer.Checksum{Algorithm: crc32}},
		{crc32, crc64nvme, objectlayer.ChecksumTypeComposite, objectlayer.Checksum{Algorithm: crc32, Composite: true}},
		{sha256, none, objectlayer.ChecksumTypeNone, objectlayer.Checksum{Algorithm: sha256, Composite: true}},
		{crc64nvme, none, objectlayer.ChecksumTypeNone, objectlayer.Checksum{Algorithm: crc64nvme}},
	} {
		assert.Equal(t, tc.want, objectlayer.NewUploadChecksum(tc.algorithm, tc.checksumType, tc.defaultAlgorithm), "%+v", tc)
	}
}

func TestNewPartChecksum(t *testing.T) {
	crc32Value := base64.StdEncoding.EncodeToString(binary.BigEndian.AppendUint32(nil, crc32.ChecksumIEEE([]byte("hello world"))))
	newData := func() *objectlayer.PutObjReader {
		sum, err := base64.StdEncoding.DecodeString(crc32Value)
		require.NoError(t, err)
		return objectlayer.NewPutObjReader(hashReader{
			Reader: strings.NewReader("hello world"),
			sums:   map[minioHash.Algorithm][]byte{minioHash.AlgorithmCRC32: sum},
		})
	}
	read := func(t *testing.T, checksum *objectlayer.PutChecksum) objectlayer.Checksum {
		_, err := io.ReadAll(checksum.Reader())
		require.NoError(t, err)
		got, err := checksum.Checksum()
		require.NoError(t, err)
		return got
	}
	composite := objectlayer.Checksum{Algorithm: objectlayer.ChecksumAlgorithmCRC32, Composite: true}
	full := objectlayer.Checksum{Algorithm: objectlayer.ChecksumAlgorithmCRC64NVME}

	checksum, err := objectlayer.NewPartChecksum(newData(), composite, objectlayer.ChecksumAlgorithmCRC32)
	require.NoError(t, err)
	assert.Equal(t, objectlayer.Checksum{Algorithm: objectlayer.ChecksumAlgorithmCRC32, Value: crc32Value}, read(t, checksum))

	_, err = objectlayer.NewPartChecksum(newData(), composite, objectlayer.ChecksumAlgorithmNone)
	require.ErrorIs(t, err, objectlayer.PartChecksumAlgorithmMismatch{Expected: objectlayer.ChecksumAlgorithmCRC32, Actual: objectlayer.ChecksumAlgorithmNone})
	_, err = objectlayer.NewPartChecksum(newData(), composite, objectlayer.ChecksumAlgorithmSHA256)
	require.ErrorIs(t, err, objectlayer.PartChecksumAlgorithmMismatch{Expected: objectlayer.ChecksumAlgorithmCRC32, Actual: objectlayer.ChecksumAlgorithmSHA256})

	// A part of an upload with full object checksums requested without one gets one computed.
	checksum, err = objectlayer.NewPartChecksum(newData(), full, objectlayer.ChecksumAlgorithmNone)
	require.NoError(t, err)
	assert.Equal(t, objectlayer.Checksum{Algorithm: objectlayer.ChecksumAlgorithmCRC64NVME, Value: "jSnVw/bqjr4="}, read(t, checksum))
	_, err = objectlayer.NewPartChecksum(newData(), full, objectlayer.ChecksumAlgorithmCRC32)
	require.ErrorIs(t, err, objectlayer.PartChecksumAlgorithmMismatch{Expected: objectlayer.ChecksumAlgorithmCRC64NVME, Actual: objectlayer.ChecksumAlgorithmCRC32})

	// A part of an upload with a default algorithm gets one computed, whatever it's requested with.
	defaulted := objectlayer.Checksum{Algorithm: objectlayer.ChecksumAlgorithmCRC64NVME, Default: true}
	for _, requested := range []objectlayer.ChecksumAlgorithm{objectlayer.ChecksumAlgorithmNone, objectlayer.ChecksumAlgorithmCRC32} {
		checksum, err = objectlayer.NewPartChecksum(newData(), defaulted, requested)
		require.NoError(t, err)
		assert.Equal(t, objectlayer.Checksum{Algorithm: objectlayer.ChecksumAlgorithmCRC64NVME, Value: "jSnVw/bqjr4="}, read(t, checksum))
	}

	checksum, err = objectlayer.NewPartChecksum(newData(), objectlayer.Checksum{}, objectlayer.ChecksumAlgorithmCRC32)
	require.NoError(t, err)
	assert.Equal(t, objectlayer.Checksum{}, read(t, checksum))
}

// TestCompleteChecksum checks the answers of S3 to CompleteMultipartUpload requests for an upload
// of 5 MiB of 'a' and then "hello world".
func TestCompleteChecksum(t *testing.T) {
	stored := []objectlayer.PartChecksum{{Value: "r/zBbw==", Size: 5 << 20}, {Value: "DUoRhQ==", Size: 11}}
	named := func(algorithm objectlayer.ChecksumAlgorithm, values ...string) []objectlayer.CompletePart {
		parts := make([]objectlayer.CompletePart, len(values))
		for i, value := range values {
			parts[i] = objectlayer.CompletePart{PartNumber: i + 1}
			if value != "" {
				parts[i].Checksum = objectlayer.Checksum{Algorithm: algorithm, Value: value}
			}
		}
		return parts
	}
	const crc32 = objectlayer.ChecksumAlgorithmCRC32
	composite := objectlayer.Checksum{Algorithm: crc32, Composite: true}
	full := objectlayer.Checksum{Algorithm: crc32}
	defaulted := objectlayer.Checksum{Algorithm: crc32, Default: true}
	compositeWant := objectlayer.Checksum{Algorithm: crc32, Composite: true, Value: "NSvTHQ==-2"}
	fullWant := objectlayer.Checksum{Algorithm: crc32, Value: "WrTTSg=="}

	for _, tc := range []struct {
		name   string
		upload objectlayer.Checksum
		parts  []objectlayer.CompletePart
		opts   objectlayer.ObjectOptions
		want   objectlayer.Checksum
		err    error
	}{
		{name: "composite", upload: composite, parts: named(crc32, "r/zBbw==", "DUoRhQ=="), want: compositeWant},
		{name: "composite without part checksums", upload: composite, parts: named(crc32, "", ""),
			err: objectlayer.MissingPartChecksum{Algorithm: crc32, PartNumber: 1}},
		{name: "composite wrong part checksum", upload: composite, parts: named(crc32, "yZRlqg==", "DUoRhQ=="), err: objectlayer.InvalidPart{PartNumber: 1}},
		{name: "composite other algorithm part checksum", upload: composite, parts: named(objectlayer.ChecksumAlgorithmSHA256, "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=", "x"),
			err: objectlayer.PartChecksumMismatch{Algorithm: objectlayer.ChecksumAlgorithmSHA256, PartNumber: 1}},
		{name: "composite type FULL_OBJECT", upload: composite, parts: named(crc32, "r/zBbw==", "DUoRhQ=="), opts: objectlayer.ObjectOptions{ChecksumType: objectlayer.ChecksumTypeFullObject},
			err: objectlayer.ChecksumTypeMismatch{Upload: objectlayer.ChecksumTypeComposite}},
		{name: "composite whole checksum", upload: composite, parts: named(crc32, "r/zBbw==", "DUoRhQ=="),
			opts: objectlayer.ObjectOptions{ChecksumAlgorithm: crc32, ChecksumValue: "NSvTHQ==-2"}, want: compositeWant},
		{name: "composite whole checksum without part count", upload: composite, parts: named(crc32, "r/zBbw==", "DUoRhQ=="),
			opts: objectlayer.ObjectOptions{ChecksumAlgorithm: crc32, ChecksumValue: "NSvTHQ=="}, want: compositeWant},
		{name: "composite whole checksum wrong part count", upload: composite, parts: named(crc32, "r/zBbw==", "DUoRhQ=="),
			opts: objectlayer.ObjectOptions{ChecksumAlgorithm: crc32, ChecksumValue: "NSvTHQ==-3"},
			err:  objectlayer.ChecksumMismatch{Algorithm: crc32}},
		{name: "full", upload: full, parts: named(crc32, "", ""), want: fullWant},
		{name: "full with part checksums", upload: full, parts: named(crc32, "r/zBbw==", "DUoRhQ=="), want: fullWant},
		{name: "full whole checksum", upload: full, parts: named(crc32, "", ""),
			opts: objectlayer.ObjectOptions{ChecksumAlgorithm: crc32, ChecksumValue: "WrTTSg==", ChecksumType: objectlayer.ChecksumTypeFullObject}, want: fullWant},
		{name: "full wrong whole checksum", upload: full, parts: named(crc32, "", ""),
			opts: objectlayer.ObjectOptions{ChecksumAlgorithm: crc32, ChecksumValue: "DUoRhQ==", ChecksumType: objectlayer.ChecksumTypeFullObject},
			err:  objectlayer.ChecksumMismatch{Algorithm: crc32}},
		// Like S3, which refuses even the right one without the type.
		{name: "full whole checksum without type", upload: full, parts: named(crc32, "", ""),
			opts: objectlayer.ObjectOptions{ChecksumAlgorithm: crc32, ChecksumValue: "WrTTSg=="},
			err:  objectlayer.ChecksumMismatch{Algorithm: crc32}},
		{name: "default", upload: defaulted, parts: named(crc32, "", ""), want: fullWant},
		{name: "default whole checksum without type", upload: defaulted, parts: named(crc32, "", ""),
			opts: objectlayer.ObjectOptions{ChecksumAlgorithm: crc32, ChecksumValue: "DUoRhQ=="}, want: fullWant},
		{name: "default with part checksums", upload: defaulted, parts: named(crc32, "r/zBbw==", "DUoRhQ=="),
			err: objectlayer.InvalidPart{PartNumber: 1}},
		{name: "default type FULL_OBJECT", upload: defaulted, parts: named(crc32, "", ""), opts: objectlayer.ObjectOptions{ChecksumType: objectlayer.ChecksumTypeFullObject},
			err: objectlayer.ChecksumTypeMismatch{Upload: objectlayer.ChecksumTypeNone}},
		{name: "full type COMPOSITE", upload: full, parts: named(crc32, "", ""), opts: objectlayer.ObjectOptions{ChecksumType: objectlayer.ChecksumTypeComposite},
			err: objectlayer.ChecksumTypeMismatch{Upload: objectlayer.ChecksumTypeFullObject}},
		{name: "no checksums", upload: objectlayer.Checksum{}, parts: named(crc32, "x", "y"),
			opts: objectlayer.ObjectOptions{ChecksumType: objectlayer.ChecksumTypeFullObject, ChecksumAlgorithm: crc32, ChecksumValue: "z"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := objectlayer.CompleteChecksum(tc.upload, tc.parts, stored, tc.opts)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	// A part stored without a checksum leaves the upload's checksums incomplete, so the object is
	// completed without one, and the request's checksums aren't checked.
	partial := []objectlayer.PartChecksum{stored[0], {Size: 11}}
	for _, upload := range []objectlayer.Checksum{composite, full, defaulted} {
		opts := objectlayer.ObjectOptions{ChecksumAlgorithm: crc32, ChecksumValue: "WrTTSg==", ChecksumType: objectlayer.ChecksumTypeFullObject}
		got, err := objectlayer.CompleteChecksum(upload, named(crc32, "r/zBbw==", "DUoRhQ=="), partial, opts)
		require.NoError(t, err, upload)
		require.Equal(t, objectlayer.Checksum{}, got, upload)
	}
}

// TestChecksumErrorMessages checks that the errors of multipart upload checksums have the messages
// that S3 answers them with.
func TestChecksumErrorMessages(t *testing.T) {
	const (
		none      = objectlayer.ChecksumAlgorithmNone
		crc32     = objectlayer.ChecksumAlgorithmCRC32
		crc64nvme = objectlayer.ChecksumAlgorithmCRC64NVME
		sha256    = objectlayer.ChecksumAlgorithmSHA256
	)
	for _, tc := range []struct {
		err  error
		want string
	}{
		{objectlayer.PartChecksumAlgorithmMismatch{Expected: crc32, Actual: none}, "Checksum Type mismatch occurred, expected checksum Type: crc32, actual checksum Type: null"},
		{objectlayer.PartChecksumAlgorithmMismatch{Expected: crc32, Actual: sha256}, "Checksum Type mismatch occurred, expected checksum Type: crc32, actual checksum Type: sha256"},
		{objectlayer.PartChecksumAlgorithmMismatch{Expected: crc64nvme, Actual: crc32}, "Checksum Type mismatch occurred, expected checksum Type: crc64nvme, actual checksum Type: crc32"},
		{objectlayer.MissingPartChecksum{Algorithm: crc32, PartNumber: 1}, "The upload was created using a crc32 checksum. The complete request must include the checksum for each part. It was missing for part 1 in the request."},
		{objectlayer.PartChecksumMismatch{Algorithm: sha256, PartNumber: 1}, "The sha256 you specified for part 1 did not match what we received."},
		{objectlayer.ChecksumTypeMismatch{Upload: objectlayer.ChecksumTypeComposite}, "The upload was created using the COMPOSITE checksum mode. The complete request must use the same checksum mode."},
		{objectlayer.ChecksumTypeMismatch{Upload: objectlayer.ChecksumTypeFullObject}, "The upload was created using the FULL_OBJECT checksum mode. The complete request must use the same checksum mode."},
		{objectlayer.ChecksumTypeMismatch{Upload: objectlayer.ChecksumTypeNone}, "The upload was created using the null checksum mode. The complete request must use the same checksum mode."},
		{objectlayer.ChecksumMismatch{Algorithm: crc32}, "The crc32 you specified did not match the calculated checksum."},
	} {
		assert.Equal(t, tc.want, tc.err.Error())
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
