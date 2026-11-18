// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.
// This file incorporates code from MinIO Cloud Storage and includes changes made by Storj Labs, Inc.

/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package objectlayer

import (
	"io"
	"sync"
	"time"

	"storj.io/minio/cmd"
)

// ObjectInfo describes an object, a version of one, or a delete marker.
type ObjectInfo struct {
	Bucket string
	Name   string
	// ModTime is when the object was last modified.
	ModTime time.Time
	Size    int64
	// ETag is the hex-encoded entity tag of the object, without quotes.
	ETag      string
	VersionID string
	// IsLatest is whether this is the latest version, which may be a delete marker.
	IsLatest bool
	// DeleteMarker is whether the version is a delete marker.
	DeleteMarker    bool
	ContentType     string
	ContentEncoding string
	// Expires is when the object stops being cacheable.
	Expires      time.Time
	StorageClass string
	// UserDefined is the object's metadata, including system metadata such as its content type.
	UserDefined map[string]string
	// UserTags is the object's tags, URL-encoded.
	UserTags string
	Checksum Checksum
}

// CheckPreconditionFn is called with the object before it's read, and returns whether a
// precondition of the request failed.
type CheckPreconditionFn func(ObjectInfo) bool

// ObjectOptions are the options of an object operation. Each field is only used by the operations
// that its comment names.
type ObjectOptions struct {
	// VersionID is the version to read, delete or change.
	VersionID string
	// PostPolicy is the policy of a POST upload, used by PutObject.
	PostPolicy PostPolicyForm
	// Retention is the retention to put the object with, used by PutObject, CopyObject,
	// NewMultipartUpload and SetObjectRetention.
	Retention *ObjectRetention
	// BypassGovernanceRetention is used by DeleteObject, DeleteObjects and SetObjectRetention.
	BypassGovernanceRetention bool
	// LegalHold is the legal hold to put the object with, used by PutObject, CopyObject and
	// NewMultipartUpload.
	LegalHold *LegalHoldStatus
	// IfNoneMatch holds the If-None-Match values of a conditional write.
	IfNoneMatch []string
	// UserDefined is the metadata of the object to write.
	UserDefined map[string]string
	// PartNumber is the part to read, used by GetObjectNInfo and GetObjectInfo.
	PartNumber int
	// CheckPrecondFn is used by GetObjectNInfo.
	CheckPrecondFn CheckPreconditionFn
	// Quiet is whether only errors are reported, used by DeleteObjects.
	Quiet bool
	// ChecksumAlgorithm is, for PutObject and PutObjectPart, the algorithm of the checksum that
	// the data is put with, which the data computes. For NewMultipartUpload, it's the algorithm of
	// the upload's checksums, and for CompleteMultipartUpload, ChecksumValue's. CopyObject refuses
	// a copy whose source has a checksum with another algorithm with ErrCopyChecksumAlgorithm.
	ChecksumAlgorithm ChecksumAlgorithm
	// ChecksumType is the requested type of a multipart upload's checksum, used by
	// NewMultipartUpload and CompleteMultipartUpload.
	ChecksumType ChecksumType
	// ChecksumValue is the base64-encoded checksum that the completed object must have, used by
	// CompleteMultipartUpload.
	ChecksumValue string
}

// PostPolicyForm is the policy of a POST upload.
type PostPolicyForm struct {
	Expiration time.Time
	Conditions PostPolicyConditions
}

// PostPolicyConditions are the conditions of a POST policy.
type PostPolicyConditions struct {
	Items              []PostPolicyCondition
	ContentLengthRange ContentLengthRange
}

// PostPolicyCondition is a condition of a POST policy.
type PostPolicyCondition struct {
	Operator string
	Key      string
	Value    string
}

// ContentLengthRange is a constraint on the size of an upload.
type ContentLengthRange struct {
	Min int64
	Max int64
	// Valid is whether the policy has a content-length-range condition.
	Valid bool
}

// GetObjectReader reads an object, and describes it.
type GetObjectReader struct {
	ObjInfo ObjectInfo

	reader   io.Reader
	cleanups []func()
	once     sync.Once
}

// NewGetObjectReader returns a GetObjectReader of the object described by info that reads from r.
// cleanup is called when the reader is closed. If opts.CheckPrecondFn reports a failed
// precondition, cleanup is called and cmd.PreConditionFailed is returned.
func NewGetObjectReader(r io.Reader, info ObjectInfo, opts ObjectOptions, cleanup func()) (*GetObjectReader, error) {
	if opts.CheckPrecondFn != nil && opts.CheckPrecondFn(info) {
		cleanup()
		return nil, cmd.PreConditionFailed{}
	}
	return &GetObjectReader{ObjInfo: info, reader: r, cleanups: []func(){cleanup}}, nil
}

// AppendCleanupFunc adds fn to be called by Close after the reader's other cleanup functions.
func (g *GetObjectReader) AppendCleanupFunc(fn func()) {
	g.cleanups = append(g.cleanups, fn)
}

// Read implements io.Reader.
func (g *GetObjectReader) Read(p []byte) (int, error) { return g.reader.Read(p) }

// Close calls the reader's cleanup functions in the order they were added, once.
func (g *GetObjectReader) Close() error {
	g.once.Do(func() {
		for _, cleanup := range g.cleanups {
			cleanup()
		}
	})
	return nil
}

// ListObjectsInfo is a page of ListObjects.
type ListObjectsInfo struct {
	IsTruncated bool
	NextMarker  string
	Objects     []ObjectInfo
	Prefixes    []string
}

// ListObjectsV2Info is a page of ListObjectsV2.
type ListObjectsV2Info struct {
	IsTruncated           bool
	ContinuationToken     string
	NextContinuationToken string
	Objects               []ObjectInfo
	Prefixes              []string
}

// ListObjectVersionsInfo is a page of ListObjectVersions.
type ListObjectVersionsInfo struct {
	IsTruncated         bool
	NextMarker          string
	NextVersionIDMarker string
	Objects             []ObjectInfo
	Prefixes            []string
}

// MultipartInfo describes a multipart upload.
type MultipartInfo struct {
	Bucket      string
	Object      string
	UploadID    string
	Initiated   time.Time
	UserDefined map[string]string
	// Checksum is the algorithm and type of the upload's checksums, without a value.
	Checksum Checksum
}

// ListMultipartsInfo is a page of ListMultipartUploads.
type ListMultipartsInfo struct {
	KeyMarker          string
	UploadIDMarker     string
	NextKeyMarker      string
	NextUploadIDMarker string
	MaxUploads         int
	IsTruncated        bool
	Uploads            []MultipartInfo
	Prefix             string
	Delimiter          string
	CommonPrefixes     []string
}

// NewMultipartUploadResult is a multipart upload started by NewMultipartUpload.
type NewMultipartUploadResult struct {
	UploadID string
	// Checksum is the algorithm and type of the upload's checksums, without a value. It has no
	// algorithm if the object layer doesn't keep the upload's checksums.
	Checksum Checksum
}

// PartInfo describes a part of a multipart upload.
type PartInfo struct {
	PartNumber   int
	LastModified time.Time
	// ETag is the hex-encoded entity tag of the part, without quotes.
	ETag string
	Size int64
	// ActualSize is the size of the part before any transformation, such as compression.
	ActualSize int64
	// Checksum is the part's checksum, with its upload's algorithm.
	Checksum Checksum
}

// ListPartsInfo is a page of ListObjectParts.
type ListPartsInfo struct {
	Bucket               string
	Object               string
	UploadID             string
	StorageClass         string
	PartNumberMarker     int
	NextPartNumberMarker int
	MaxParts             int
	IsTruncated          bool
	Parts                []PartInfo
	UserDefined          map[string]string
	// Checksum is the algorithm and type of the upload's checksums, without a value.
	Checksum Checksum
}

// CompletePart is a part named in CompleteMultipartUpload.
type CompletePart struct {
	PartNumber int
	ETag       string
	// Checksum is the part's checksum that the request names, if it names one.
	Checksum Checksum
}

// MultipartETag returns the ETag of an object completed from parts.
func MultipartETag(parts []CompletePart) string {
	minioParts := make([]cmd.CompletePart, len(parts))
	for i, part := range parts {
		minioParts[i] = cmd.CompletePart{PartNumber: part.PartNumber, ETag: part.ETag}
	}
	return cmd.ComputeCompleteMultipartMD5(minioParts)
}
