// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package objectlayer

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	stdhash "hash"
	"hash/crc32"
	"hash/crc64"
	"io"
	"strings"

	"storj.io/minio/pkg/hash"
)

// ChecksumAlgorithm is the algorithm of an object's checksum.
type ChecksumAlgorithm int

// The checksum algorithms that objects can be stored with.
const (
	ChecksumAlgorithmNone ChecksumAlgorithm = iota
	ChecksumAlgorithmCRC32
	ChecksumAlgorithmCRC32C
	ChecksumAlgorithmCRC64NVME
	ChecksumAlgorithmSHA1
	ChecksumAlgorithmSHA256
)

// String returns the algorithm's name in S3, which is empty for ChecksumAlgorithmNone.
func (a ChecksumAlgorithm) String() string {
	switch a {
	case ChecksumAlgorithmCRC32:
		return "CRC32"
	case ChecksumAlgorithmCRC32C:
		return "CRC32C"
	case ChecksumAlgorithmCRC64NVME:
		return "CRC64NVME"
	case ChecksumAlgorithmSHA1:
		return "SHA1"
	case ChecksumAlgorithmSHA256:
		return "SHA256"
	default:
		return ""
	}
}

// Checksum is the checksum of an object.
type Checksum struct {
	Algorithm ChecksumAlgorithm
	// Composite is whether it's computed from the checksums of the object's parts rather than
	// from its data.
	Composite bool
	// Value is base64-encoded. A composite checksum ends with -<number of parts>.
	Value string
}

// ParseChecksumAlgorithm returns the algorithm named name, in any case. An empty name is
// ChecksumAlgorithmNone.
func ParseChecksumAlgorithm(name string) (ChecksumAlgorithm, error) {
	if name == "" {
		return ChecksumAlgorithmNone, nil
	}
	for algorithm := ChecksumAlgorithmCRC32; algorithm <= ChecksumAlgorithmSHA256; algorithm++ {
		if strings.EqualFold(name, algorithm.String()) {
			return algorithm, nil
		}
	}
	return ChecksumAlgorithmNone, fmt.Errorf("objectlayer: unknown checksum algorithm %q", name)
}

// PutChecksum is the checksum to store with an object that's put.
type PutChecksum struct {
	data      *PutObjReader
	algorithm ChecksumAlgorithm
	// hash computes the default checksum, which data doesn't.
	hash stdhash.Hash
}

// NewPutChecksum returns the checksum of data to store: the requested one, which data computes,
// or without one, a checksum with defaultAlgorithm, which PutChecksum computes while Reader is
// read. Either algorithm may be ChecksumAlgorithmNone.
func NewPutChecksum(data *PutObjReader, requested, defaultAlgorithm ChecksumAlgorithm) *PutChecksum {
	c := &PutChecksum{data: data, algorithm: requested}
	if requested == ChecksumAlgorithmNone && defaultAlgorithm != ChecksumAlgorithmNone {
		c.algorithm = defaultAlgorithm
		c.hash = newChecksumHash(defaultAlgorithm)
	}
	return c
}

// Reader returns the reader of the data to store.
func (c *PutChecksum) Reader() io.Reader {
	if c.hash == nil {
		return c.data
	}
	return io.TeeReader(c.data, c.hash)
}

// Checksum returns the checksum once Reader has been read to the end. It's empty if there's no
// checksum to store.
func (c *PutChecksum) Checksum() (Checksum, error) {
	if c.algorithm == ChecksumAlgorithmNone {
		return Checksum{}, nil
	}
	if c.hash != nil {
		return Checksum{Algorithm: c.algorithm, Value: base64.StdEncoding.EncodeToString(c.hash.Sum(nil))}, nil
	}

	hashAlgorithm, ok := map[ChecksumAlgorithm]hash.Algorithm{
		ChecksumAlgorithmCRC32:     hash.AlgorithmCRC32,
		ChecksumAlgorithmCRC32C:    hash.AlgorithmCRC32C,
		ChecksumAlgorithmCRC64NVME: hash.AlgorithmCRC64NVME,
		ChecksumAlgorithmSHA1:      hash.AlgorithmSHA1,
		ChecksumAlgorithmSHA256:    hash.AlgorithmSHA256,
	}[c.algorithm]
	if !ok {
		return Checksum{}, fmt.Errorf("objectlayer: no checksum algorithm %d", c.algorithm)
	}
	checksums, err := c.data.Checksums()
	if err != nil {
		return Checksum{}, err
	}
	sum, ok := checksums[hashAlgorithm]
	if !ok {
		return Checksum{}, fmt.Errorf("objectlayer: the data didn't compute a %s checksum", c.algorithm)
	}
	return Checksum{Algorithm: c.algorithm, Value: base64.StdEncoding.EncodeToString(sum)}, nil
}

// crc64NVMETable is the table of the CRC-64/NVME polynomial.
var crc64NVMETable = crc64.MakeTable(0x9a6c9329ac4bc9b5)

func newChecksumHash(algorithm ChecksumAlgorithm) stdhash.Hash {
	switch algorithm {
	case ChecksumAlgorithmCRC32:
		return crc32.NewIEEE()
	case ChecksumAlgorithmCRC32C:
		return crc32.New(crc32.MakeTable(crc32.Castagnoli))
	case ChecksumAlgorithmCRC64NVME:
		return crc64.New(crc64NVMETable)
	case ChecksumAlgorithmSHA1:
		return sha1.New()
	case ChecksumAlgorithmSHA256:
		return sha256.New()
	default:
		panic(fmt.Sprintf("objectlayer: no checksum algorithm %d", algorithm))
	}
}
