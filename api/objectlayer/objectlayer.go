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

// Package objectlayer contains the object layer interface that the api package serves requests
// with, and the types it uses.
//
// The types are aliases of storj.io/minio types, so that an implementation of minio's
// cmd.ObjectLayer, such as miniogw's, can be used without changes. They become definitions once
// minio's handlers no longer serve requests.
package objectlayer

import (
	"context"
	"io"
	"net/http"

	"storj.io/minio/cmd"
	objectlock "storj.io/minio/pkg/bucket/object/lock"
	"storj.io/minio/pkg/bucket/versioning"
	"storj.io/minio/pkg/event"
	"storj.io/minio/pkg/hash"
	"storj.io/minio/pkg/tags"
)

// ObjectLayer is the storage backend of the api package. It's the subset of minio's
// cmd.ObjectLayer that the api package uses.
type ObjectLayer interface {
	MakeBucketWithLocation(ctx context.Context, bucket string, opts BucketOptions) error
	GetBucketInfo(ctx context.Context, bucket string) (BucketInfo, error)
	ListBuckets(ctx context.Context) ([]BucketInfo, error)
	DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error

	ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error)
	ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (ListObjectsV2Info, error)
	ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (ListObjectVersionsInfo, error)

	SetBucketVersioning(ctx context.Context, bucket string, v *Versioning) error
	GetBucketVersioning(ctx context.Context, bucket string) (*Versioning, error)

	GetBucketTagging(ctx context.Context, bucket string) (*Tags, error)
	SetBucketTagging(ctx context.Context, bucket string, tags *Tags) error

	GetObjectLockConfig(ctx context.Context, bucket string) (*ObjectLockConfig, error)
	SetObjectLockConfig(ctx context.Context, bucket string, config *ObjectLockConfig) error

	GetBucketNotificationConfig(ctx context.Context, bucket string) (*EventConfig, error)
	SetBucketNotificationConfig(ctx context.Context, bucket string, config *EventConfig) error

	GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (*GetObjectReader, error)
	GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error)
	PutObject(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (ObjectInfo, error)
	CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (ObjectInfo, error)
	DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error)
	DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []DeleteObjectsError, error)

	SetObjectRetention(ctx context.Context, bucket, object, versionID string, opts ObjectOptions) error
	GetObjectRetention(ctx context.Context, bucket, object, versionID string) (*ObjectRetention, error)
	SetObjectLegalHold(ctx context.Context, bucket, object, versionID string, legalHold *ObjectLegalHold) error
	GetObjectLegalHold(ctx context.Context, bucket, object, versionID string) (*ObjectLegalHold, error)

	PutObjectTags(ctx context.Context, bucket, object, tags string, opts ObjectOptions) (ObjectInfo, error)
	DeleteObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error)

	ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error)
	NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (uploadID string, err error)
	CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int, startOffset, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (PartInfo, error)
	PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *PutObjReader, opts ObjectOptions) (PartInfo, error)
	ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, opts ObjectOptions) (ListPartsInfo, error)
	AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) error
	CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []CompletePart, opts ObjectOptions) (ObjectInfo, error)
}

//nolint:revive // aliases of minio types, which document them
type (
	BucketInfo             = cmd.BucketInfo
	BucketOptions          = cmd.BucketOptions
	ObjectInfo             = cmd.ObjectInfo
	ObjectOptions          = cmd.ObjectOptions
	ObjectToDelete         = cmd.ObjectToDelete
	DeletedObject          = cmd.DeletedObject
	DeleteObjectsError     = cmd.DeleteObjectsError
	HTTPRangeSpec          = cmd.HTTPRangeSpec
	LockType               = cmd.LockType
	GetObjectReader        = cmd.GetObjectReader
	PutObjReader           = cmd.PutObjReader
	ListObjectsInfo        = cmd.ListObjectsInfo
	ListObjectsV2Info      = cmd.ListObjectsV2Info
	ListObjectVersionsInfo = cmd.ListObjectVersionsInfo
	ListMultipartsInfo     = cmd.ListMultipartsInfo
	MultipartInfo          = cmd.MultipartInfo
	ListPartsInfo          = cmd.ListPartsInfo
	PartInfo               = cmd.PartInfo
	CompletePart           = cmd.CompletePart
)

//nolint:revive // aliases of minio types, which document them
type (
	Versioning       = versioning.Versioning
	Tags             = tags.Tags
	ObjectLockConfig = objectlock.Config
	ObjectRetention  = objectlock.ObjectRetention
	ObjectLegalHold  = objectlock.ObjectLegalHold
	EventConfig      = event.Config
)

//nolint:revive // aliases of minio types, which document them
type (
	BucketNameInvalid              = cmd.BucketNameInvalid
	BucketNotFound                 = cmd.BucketNotFound
	BucketAlreadyExists            = cmd.BucketAlreadyExists
	BucketNotEmpty                 = cmd.BucketNotEmpty
	BucketTaggingNotFound          = cmd.BucketTaggingNotFound
	BucketObjectLockConfigNotFound = cmd.BucketObjectLockConfigNotFound
	ObjectNameInvalid              = cmd.ObjectNameInvalid
	ObjectNameTooLong              = cmd.ObjectNameTooLong
	ObjectNotFound                 = cmd.ObjectNotFound
	VersionNotFound                = cmd.VersionNotFound
	ObjectTooSmall                 = cmd.ObjectTooSmall
	ObjectTooLarge                 = cmd.ObjectTooLarge
	InvalidUploadID                = cmd.InvalidUploadID
	InvalidPart                    = cmd.InvalidPart
	PartTooSmall                   = cmd.PartTooSmall
	InvalidRange                   = cmd.InvalidRange
	InvalidArgument                = cmd.InvalidArgument
	IncompleteBody                 = cmd.IncompleteBody
	NotImplemented                 = cmd.NotImplemented
	MethodNotAllowed               = cmd.MethodNotAllowed
	PrefixAccessDenied             = cmd.PrefixAccessDenied
	OperationTimedOut              = cmd.OperationTimedOut
	SlowDown                       = cmd.SlowDown
)

// NewPutObjReader returns a PutObjReader that reads from r.
func NewPutObjReader(r hash.Reader) *PutObjReader { return cmd.NewPutObjReader(r) }

// NewGetObjectReader returns a GetObjectReader of the object described by info that reads from r.
// cleanup is called when the reader is closed.
func NewGetObjectReader(r io.Reader, info ObjectInfo, opts ObjectOptions, cleanup func()) (*GetObjectReader, error) {
	return cmd.NewGetObjectReaderFromReader(r, info, opts, cleanup)
}

// MultipartETag returns the ETag of an object completed from parts.
func MultipartETag(parts []CompletePart) string { return cmd.ComputeCompleteMultipartMD5(parts) }
