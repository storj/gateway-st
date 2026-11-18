// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package objectlayer

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	stdhash "hash"
	"hash/crc32"
	"hash/crc64"
	"io"
	"strconv"
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
	// Default is whether the algorithm is the gateway's default for a multipart upload requested
	// without one. Like S3, such an upload is reported as without checksums until it's completed.
	Default bool
}

// ErrCopyChecksumAlgorithm is the error of CopyObject for a checksum algorithm other than the
// source's, which would need the data to compute.
var ErrCopyChecksumAlgorithm = NotImplemented{Message: "Specifying a checksum algorithm for the destination object that differs from that of the source object is not supported."}

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

// PutChecksum is the checksum to store with data that's put.
type PutChecksum struct {
	source io.Reader
	// data computes the requested checksum. It's nil for a copy.
	data      *PutObjReader
	algorithm ChecksumAlgorithm
	// hash computes a checksum that data doesn't.
	hash stdhash.Hash
}

// NewPutChecksum returns the checksum of data to store: the requested one, which data computes,
// or without one, a checksum with defaultAlgorithm, which PutChecksum computes while Reader is
// read. Either algorithm may be ChecksumAlgorithmNone.
func NewPutChecksum(data *PutObjReader, requested, defaultAlgorithm ChecksumAlgorithm) *PutChecksum {
	c := &PutChecksum{source: data, data: data, algorithm: requested}
	if requested == ChecksumAlgorithmNone && defaultAlgorithm != ChecksumAlgorithmNone {
		c.algorithm = defaultAlgorithm
		c.hash = newChecksumHash(defaultAlgorithm)
	}
	return c
}

// NewCopyChecksum returns the checksum with algorithm, which may be ChecksumAlgorithmNone, of
// copied data, which PutChecksum computes while Reader is read.
func NewCopyChecksum(source io.Reader, algorithm ChecksumAlgorithm) *PutChecksum {
	c := &PutChecksum{source: source, algorithm: algorithm}
	if algorithm != ChecksumAlgorithmNone {
		c.hash = newChecksumHash(algorithm)
	}
	return c
}

// Reader returns the reader of the data to store.
func (c *PutChecksum) Reader() io.Reader {
	if c.hash == nil {
		return c.source
	}
	return io.TeeReader(c.source, c.hash)
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

// ChecksumType is the requested type of a multipart upload's checksum.
type ChecksumType int

// The checksum types. ChecksumTypeNone is a request that doesn't name one.
const (
	ChecksumTypeNone ChecksumType = iota
	ChecksumTypeComposite
	ChecksumTypeFullObject
)

// String returns the type's name in S3, or "null" for ChecksumTypeNone, like S3's messages.
func (t ChecksumType) String() string {
	switch t {
	case ChecksumTypeComposite:
		return "COMPOSITE"
	case ChecksumTypeFullObject:
		return "FULL_OBJECT"
	default:
		return "null"
	}
}

// checksumAlgorithmName returns the name of algorithm in S3's multipart upload messages.
func checksumAlgorithmName(algorithm ChecksumAlgorithm) string {
	if algorithm == ChecksumAlgorithmNone {
		return "null"
	}
	return strings.ToLower(algorithm.String())
}

// PartChecksumAlgorithmMismatch is the error of PutObjectPart for a part put with a checksum
// algorithm other than its upload's, or without a checksum when the upload's are composite.
type PartChecksumAlgorithmMismatch struct {
	Expected, Actual ChecksumAlgorithm
}

func (e PartChecksumAlgorithmMismatch) Error() string {
	return "Checksum Type mismatch occurred, expected checksum Type: " + checksumAlgorithmName(e.Expected) + ", actual checksum Type: " + checksumAlgorithmName(e.Actual)
}

// MissingPartChecksum is the error of CompleteMultipartUpload for a part named without its
// checksum when the upload's checksums are composite.
type MissingPartChecksum struct {
	Algorithm  ChecksumAlgorithm
	PartNumber int
}

func (e MissingPartChecksum) Error() string {
	return "The upload was created using a " + checksumAlgorithmName(e.Algorithm) + " checksum. The complete request must include the checksum for each part. It was missing for part " + strconv.Itoa(e.PartNumber) + " in the request."
}

// PartChecksumMismatch is the error of CompleteMultipartUpload for a part named with a checksum
// whose algorithm isn't its upload's.
type PartChecksumMismatch struct {
	Algorithm  ChecksumAlgorithm
	PartNumber int
}

func (e PartChecksumMismatch) Error() string {
	return "The " + checksumAlgorithmName(e.Algorithm) + " you specified for part " + strconv.Itoa(e.PartNumber) + " did not match what we received."
}

// ChecksumTypeMismatch is the error of CompleteMultipartUpload for a checksum type other than its
// upload's, whose type is Upload.
type ChecksumTypeMismatch struct {
	Upload ChecksumType
}

func (e ChecksumTypeMismatch) Error() string {
	return "The upload was created using the " + e.Upload.String() + " checksum mode. The complete request must use the same checksum mode."
}

// ChecksumMismatch is the error of CompleteMultipartUpload for a ChecksumValue that isn't the
// checksum of the completed object, which has Algorithm.
type ChecksumMismatch struct {
	Algorithm ChecksumAlgorithm
}

func (e ChecksumMismatch) Error() string {
	return "The " + checksumAlgorithmName(e.Algorithm) + " you specified did not match the calculated checksum."
}

// NewUploadChecksum returns the algorithm and type of the checksums of a multipart upload that
// is requested with algorithm and checksumType. Like S3, an upload's checksums are composite
// unless requested otherwise, except CRC64NVME ones, which can only be full object ones. Without
// an algorithm, the upload has full object checksums with defaultAlgorithm, if it isn't
// ChecksumAlgorithmNone.
//
// It doesn't check the combination. Callers must refuse composite CRC64NVME checksums and full
// object checksums with other algorithms than CRC ones, like S3 does when the upload is created,
// because CompleteChecksum can't complete such an upload. defaultAlgorithm must be a CRC
// algorithm or ChecksumAlgorithmNone for the same reason.
func NewUploadChecksum(algorithm ChecksumAlgorithm, checksumType ChecksumType, defaultAlgorithm ChecksumAlgorithm) Checksum {
	switch {
	case algorithm == ChecksumAlgorithmNone:
		return Checksum{Algorithm: defaultAlgorithm, Default: defaultAlgorithm != ChecksumAlgorithmNone}
	case checksumType == ChecksumTypeNone:
		return Checksum{Algorithm: algorithm, Composite: algorithm != ChecksumAlgorithmCRC64NVME}
	default:
		return Checksum{Algorithm: algorithm, Composite: checksumType == ChecksumTypeComposite}
	}
}

// NewPartChecksum returns the checksum to store with a part of an upload with checksums like
// upload, which the part's data is requested with. Like S3, a part of an upload with full object
// checksums gets one computed when requested without one, and a part requested with another
// algorithm is refused, except for an upload with a default algorithm, which gets one computed.
func NewPartChecksum(data *PutObjReader, upload Checksum, requested ChecksumAlgorithm) (*PutChecksum, error) {
	switch {
	case upload.Algorithm == ChecksumAlgorithmNone:
		return NewPutChecksum(data, ChecksumAlgorithmNone, ChecksumAlgorithmNone), nil
	case requested == upload.Algorithm:
		return NewPutChecksum(data, requested, ChecksumAlgorithmNone), nil
	case requested == ChecksumAlgorithmNone && !upload.Composite, upload.Default:
		return NewPutChecksum(data, ChecksumAlgorithmNone, upload.Algorithm), nil
	default:
		return nil, PartChecksumAlgorithmMismatch{Expected: upload.Algorithm, Actual: requested}
	}
}

// CompleteChecksum returns the checksum of an object completed from the parts of an upload with
// checksums like upload, with the stored checksums and sizes of stored, which the request names
// as parts. It checks the checksums that the request names, and the type and value in opts. An
// upload without checksums ignores them, unlike S3, which refuses some.
func CompleteChecksum(upload Checksum, parts []CompletePart, stored []PartChecksum, opts ObjectOptions) (Checksum, error) {
	if upload.Algorithm == ChecksumAlgorithmNone {
		return Checksum{}, nil
	}
	// A part put through a gateway with object checksums off has none, which leaves the upload's
	// checksums incomplete. The object is then completed without one, rather than never.
	for _, part := range stored {
		if part.Value == "" {
			return Checksum{}, nil
		}
	}

	uploadType := ChecksumTypeFullObject
	switch {
	case upload.Default:
		uploadType = ChecksumTypeNone
	case upload.Composite:
		uploadType = ChecksumTypeComposite
	}
	if opts.ChecksumType != ChecksumTypeNone && opts.ChecksumType != uploadType {
		return Checksum{}, ChecksumTypeMismatch{Upload: uploadType}
	}

	for i, part := range parts {
		switch {
		case upload.Default && part.Checksum.Algorithm != ChecksumAlgorithmNone:
			// Like S3, which reports the upload as without checksums.
			return Checksum{}, InvalidPart{PartNumber: part.PartNumber, GotETag: part.ETag}
		case part.Checksum.Algorithm == ChecksumAlgorithmNone:
			if upload.Composite {
				return Checksum{}, MissingPartChecksum{Algorithm: upload.Algorithm, PartNumber: part.PartNumber}
			}
		case part.Checksum.Algorithm != upload.Algorithm:
			return Checksum{}, PartChecksumMismatch{Algorithm: part.Checksum.Algorithm, PartNumber: part.PartNumber}
		case part.Checksum.Value != stored[i].Value:
			return Checksum{}, InvalidPart{PartNumber: part.PartNumber, GotETag: part.ETag}
		}
	}

	combine := FullObjectChecksum
	if upload.Composite {
		combine = CompositeChecksum
	}
	checksum, err := combine(upload.Algorithm, stored)
	if err != nil {
		return Checksum{}, err
	}

	want := checksum.Value
	// Like S3, a composite checksum may leave out the number of parts.
	if upload.Composite && !strings.Contains(opts.ChecksumValue, "-") {
		want, _, _ = strings.Cut(want, "-")
	}
	switch {
	case opts.ChecksumValue == "" || upload.Default:
		// Like S3, which ignores it for an upload requested without an algorithm.
	case !upload.Composite && opts.ChecksumType != ChecksumTypeFullObject,
		opts.ChecksumAlgorithm != upload.Algorithm, opts.ChecksumValue != want:
		// Like S3, which also refuses a full object checksum without the type.
		return Checksum{}, ChecksumMismatch{Algorithm: upload.Algorithm}
	}
	return checksum, nil
}

// PartChecksum is the checksum of a part of a multipart upload, and the part's size.
type PartChecksum struct {
	// Value is base64-encoded.
	Value string
	Size  int64
}

// CompositeChecksum returns the composite checksum of an object completed from parts, which have
// checksums with algorithm: the checksum of the parts' checksums.
func CompositeChecksum(algorithm ChecksumAlgorithm, parts []PartChecksum) (Checksum, error) {
	if algorithm == ChecksumAlgorithmCRC64NVME || algorithm == ChecksumAlgorithmNone {
		return Checksum{}, fmt.Errorf("objectlayer: no composite %s checksums", algorithm)
	}
	sums, err := decodePartChecksums(parts)
	if err != nil {
		return Checksum{}, err
	}
	hash := newChecksumHash(algorithm)
	for _, sum := range sums {
		_, _ = hash.Write(sum)
	}
	value := base64.StdEncoding.EncodeToString(hash.Sum(nil)) + "-" + strconv.Itoa(len(parts))
	return Checksum{Algorithm: algorithm, Composite: true, Value: value}, nil
}

// FullObjectChecksum returns the full object checksum of an object completed from parts, which
// have checksums with algorithm. Only CRC algorithms have them, computed from the parts' checksums
// as if of the whole data.
func FullObjectChecksum(algorithm ChecksumAlgorithm, parts []PartChecksum) (Checksum, error) {
	sums, err := decodePartChecksums(parts)
	if err != nil {
		return Checksum{}, err
	}

	var value []byte
	switch algorithm {
	case ChecksumAlgorithmCRC32, ChecksumAlgorithmCRC32C:
		combiner := crc32IEEECombiner
		if algorithm == ChecksumAlgorithmCRC32C {
			combiner = crc32CastagnoliCombiner
		}
		var crc uint32
		for i, sum := range sums {
			if len(sum) != 4 {
				return Checksum{}, fmt.Errorf("objectlayer: part checksum %q isn't a %s checksum", parts[i].Value, algorithm)
			}
			crc = combiner.combine(crc, binary.BigEndian.Uint32(sum), parts[i].Size)
		}
		value = binary.BigEndian.AppendUint32(nil, crc)
	case ChecksumAlgorithmCRC64NVME:
		var crc uint64
		for i, sum := range sums {
			if len(sum) != 8 {
				return Checksum{}, fmt.Errorf("objectlayer: part checksum %q isn't a %s checksum", parts[i].Value, algorithm)
			}
			crc = crc64NVMECombiner.combine(crc, binary.BigEndian.Uint64(sum), parts[i].Size)
		}
		value = binary.BigEndian.AppendUint64(nil, crc)
	default:
		return Checksum{}, fmt.Errorf("objectlayer: no full object %s checksums", algorithm)
	}
	return Checksum{Algorithm: algorithm, Value: base64.StdEncoding.EncodeToString(value)}, nil
}

func decodePartChecksums(parts []PartChecksum) ([][]byte, error) {
	sums := make([][]byte, len(parts))
	for i, part := range parts {
		sum, err := base64.StdEncoding.DecodeString(part.Value)
		if err != nil {
			return nil, fmt.Errorf("objectlayer: part checksum %q: %w", part.Value, err)
		}
		sums[i] = sum
	}
	return sums, nil
}
