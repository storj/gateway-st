// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

// Package minioobjectlayer adapts an api/objectlayer.ObjectLayer to minio's cmd.ObjectLayer, so
// that minio's handlers can serve requests with it.
package minioobjectlayer

import (
	"context"
	"net/http"

	"storj.io/gateway/api/objectlayer"
	minio "storj.io/minio/cmd"
	"storj.io/minio/pkg/tags"
)

// ObjectLayer is an objectlayer.ObjectLayer with the methods that only minio's handlers call.
type ObjectLayer interface {
	objectlayer.ObjectLayer

	Shutdown(ctx context.Context) error
	StorageInfo(ctx context.Context) (minio.StorageInfo, []error)
	IsNotificationSupported() bool
	IsTaggingSupported() bool
	GetObjectTags(ctx context.Context, bucket, object string, opts objectlayer.ObjectOptions) (*tags.Tags, error)
}

// New returns a minio cmd.ObjectLayer that serves requests with layer. What minio's types can't
// carry is dropped.
func New(layer ObjectLayer) minio.ObjectLayer {
	return &adapter{layer: layer}
}

type adapter struct {
	minio.GatewayUnsupported

	layer ObjectLayer
}

func (a *adapter) Shutdown(ctx context.Context) error { return a.layer.Shutdown(ctx) }

func (a *adapter) StorageInfo(ctx context.Context) (minio.StorageInfo, []error) {
	return a.layer.StorageInfo(ctx)
}

func (a *adapter) IsNotificationSupported() bool { return a.layer.IsNotificationSupported() }

func (a *adapter) IsTaggingSupported() bool { return a.layer.IsTaggingSupported() }

func (a *adapter) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	return a.layer.MakeBucketWithLocation(ctx, bucket, opts)
}

func (a *adapter) GetBucketInfo(ctx context.Context, bucket string) (minio.BucketInfo, error) {
	return a.layer.GetBucketInfo(ctx, bucket)
}

func (a *adapter) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	return a.layer.ListBuckets(ctx)
}

func (a *adapter) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	return a.layer.DeleteBucket(ctx, bucket, forceDelete)
}

func (a *adapter) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (minio.ListObjectsInfo, error) {
	info, err := a.layer.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	return minio.ListObjectsInfo{
		IsTruncated: info.IsTruncated,
		NextMarker:  info.NextMarker,
		Objects:     toMinioObjectInfos(info.Objects),
		Prefixes:    info.Prefixes,
	}, err
}

func (a *adapter) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (minio.ListObjectsV2Info, error) {
	info, err := a.layer.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
	return minio.ListObjectsV2Info{
		IsTruncated:           info.IsTruncated,
		ContinuationToken:     info.ContinuationToken,
		NextContinuationToken: info.NextContinuationToken,
		Objects:               toMinioObjectInfos(info.Objects),
		Prefixes:              info.Prefixes,
	}, err
}

func (a *adapter) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (minio.ListObjectVersionsInfo, error) {
	info, err := a.layer.ListObjectVersions(ctx, bucket, prefix, marker, versionMarker, delimiter, maxKeys)
	return minio.ListObjectVersionsInfo{
		IsTruncated:         info.IsTruncated,
		NextMarker:          info.NextMarker,
		NextVersionIDMarker: info.NextVersionIDMarker,
		Objects:             toMinioObjectInfos(info.Objects),
		Prefixes:            info.Prefixes,
	}, err
}

func (a *adapter) SetBucketVersioning(ctx context.Context, bucket string, v *objectlayer.Versioning) error {
	return a.layer.SetBucketVersioning(ctx, bucket, v)
}

func (a *adapter) GetBucketVersioning(ctx context.Context, bucket string) (*objectlayer.Versioning, error) {
	return a.layer.GetBucketVersioning(ctx, bucket)
}

func (a *adapter) GetBucketTagging(ctx context.Context, bucket string) (*tags.Tags, error) {
	return a.layer.GetBucketTagging(ctx, bucket)
}

func (a *adapter) SetBucketTagging(ctx context.Context, bucket string, t *tags.Tags) error {
	return a.layer.SetBucketTagging(ctx, bucket, t)
}

func (a *adapter) GetObjectLockConfig(ctx context.Context, bucket string) (*objectlayer.ObjectLockConfig, error) {
	return a.layer.GetObjectLockConfig(ctx, bucket)
}

func (a *adapter) SetObjectLockConfig(ctx context.Context, bucket string, config *objectlayer.ObjectLockConfig) error {
	return a.layer.SetObjectLockConfig(ctx, bucket, config)
}

func (a *adapter) GetBucketNotificationConfig(ctx context.Context, bucket string) (*objectlayer.EventConfig, error) {
	return a.layer.GetBucketNotificationConfig(ctx, bucket)
}

func (a *adapter) SetBucketNotificationConfig(ctx context.Context, bucket string, config *objectlayer.EventConfig) error {
	return a.layer.SetBucketNotificationConfig(ctx, bucket, config)
}

func (a *adapter) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (*minio.GetObjectReader, error) {
	reader, err := a.layer.GetObjectNInfo(ctx, bucket, object, rs, h, lockType, fromMinioObjectOptions(opts))
	if err != nil {
		return nil, err
	}
	// The layer has already checked the preconditions.
	return minio.NewGetObjectReaderFromReader(reader, toMinioObjectInfo(reader.ObjInfo), minio.ObjectOptions{}, func() { _ = reader.Close() })
}

func (a *adapter) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	info, err := a.layer.GetObjectInfo(ctx, bucket, object, fromMinioObjectOptions(opts))
	return toMinioObjectInfo(info), err
}

func (a *adapter) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	info, err := a.layer.PutObject(ctx, bucket, object, data, fromMinioObjectOptions(opts))
	return toMinioObjectInfo(info), err
}

func (a *adapter) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.ObjectInfo, error) {
	info, err := a.layer.CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, fromMinioObjectInfo(srcInfo), fromMinioObjectOptions(srcOpts), fromMinioObjectOptions(dstOpts))
	return toMinioObjectInfo(info), err
}

func (a *adapter) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	info, err := a.layer.DeleteObject(ctx, bucket, object, fromMinioObjectOptions(opts))
	return toMinioObjectInfo(info), err
}

func (a *adapter) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) ([]minio.DeletedObject, []minio.DeleteObjectsError, error) {
	return a.layer.DeleteObjects(ctx, bucket, objects, fromMinioObjectOptions(opts))
}

func (a *adapter) SetObjectRetention(ctx context.Context, bucket, object, versionID string, opts minio.ObjectOptions) error {
	return a.layer.SetObjectRetention(ctx, bucket, object, versionID, fromMinioObjectOptions(opts))
}

func (a *adapter) GetObjectRetention(ctx context.Context, bucket, object, versionID string) (*objectlayer.ObjectRetention, error) {
	return a.layer.GetObjectRetention(ctx, bucket, object, versionID)
}

func (a *adapter) SetObjectLegalHold(ctx context.Context, bucket, object, versionID string, legalHold *objectlayer.ObjectLegalHold) error {
	return a.layer.SetObjectLegalHold(ctx, bucket, object, versionID, legalHold)
}

func (a *adapter) GetObjectLegalHold(ctx context.Context, bucket, object, versionID string) (*objectlayer.ObjectLegalHold, error) {
	return a.layer.GetObjectLegalHold(ctx, bucket, object, versionID)
}

func (a *adapter) PutObjectTags(ctx context.Context, bucket, object, t string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	info, err := a.layer.PutObjectTags(ctx, bucket, object, t, fromMinioObjectOptions(opts))
	return toMinioObjectInfo(info), err
}

func (a *adapter) GetObjectTags(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (*tags.Tags, error) {
	return a.layer.GetObjectTags(ctx, bucket, object, fromMinioObjectOptions(opts))
}

func (a *adapter) DeleteObjectTags(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	info, err := a.layer.DeleteObjectTags(ctx, bucket, object, fromMinioObjectOptions(opts))
	return toMinioObjectInfo(info), err
}

func (a *adapter) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (minio.ListMultipartsInfo, error) {
	info, err := a.layer.ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	var uploads []minio.MultipartInfo
	if info.Uploads != nil {
		uploads = make([]minio.MultipartInfo, len(info.Uploads))
	}
	for i, upload := range info.Uploads {
		uploads[i] = minio.MultipartInfo{
			Bucket:      upload.Bucket,
			Object:      upload.Object,
			UploadID:    upload.UploadID,
			Initiated:   upload.Initiated,
			UserDefined: upload.UserDefined,
		}
	}
	return minio.ListMultipartsInfo{
		KeyMarker:          info.KeyMarker,
		UploadIDMarker:     info.UploadIDMarker,
		NextKeyMarker:      info.NextKeyMarker,
		NextUploadIDMarker: info.NextUploadIDMarker,
		MaxUploads:         info.MaxUploads,
		IsTruncated:        info.IsTruncated,
		Uploads:            uploads,
		Prefix:             info.Prefix,
		Delimiter:          info.Delimiter,
		CommonPrefixes:     info.CommonPrefixes,
	}, err
}

func (a *adapter) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (string, error) {
	result, err := a.layer.NewMultipartUpload(ctx, bucket, object, fromMinioObjectOptions(opts))
	return result.UploadID, err
}

func (a *adapter) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int, startOffset, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.PartInfo, error) {
	info, err := a.layer.CopyObjectPart(ctx, srcBucket, srcObject, dstBucket, dstObject, uploadID, partID, startOffset, length, fromMinioObjectInfo(srcInfo), fromMinioObjectOptions(srcOpts), fromMinioObjectOptions(dstOpts))
	return toMinioPartInfo(info), err
}

func (a *adapter) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (minio.PartInfo, error) {
	info, err := a.layer.PutObjectPart(ctx, bucket, object, uploadID, partID, data, fromMinioObjectOptions(opts))
	return toMinioPartInfo(info), err
}

func (a *adapter) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, opts minio.ObjectOptions) (minio.ListPartsInfo, error) {
	info, err := a.layer.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, fromMinioObjectOptions(opts))
	var parts []minio.PartInfo
	if info.Parts != nil {
		parts = make([]minio.PartInfo, len(info.Parts))
	}
	for i, part := range info.Parts {
		parts[i] = toMinioPartInfo(part)
	}
	return minio.ListPartsInfo{
		Bucket:               info.Bucket,
		Object:               info.Object,
		UploadID:             info.UploadID,
		StorageClass:         info.StorageClass,
		PartNumberMarker:     info.PartNumberMarker,
		NextPartNumberMarker: info.NextPartNumberMarker,
		MaxParts:             info.MaxParts,
		IsTruncated:          info.IsTruncated,
		Parts:                parts,
		UserDefined:          info.UserDefined,
	}, err
}

func (a *adapter) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) error {
	return a.layer.AbortMultipartUpload(ctx, bucket, object, uploadID, fromMinioObjectOptions(opts))
}

func (a *adapter) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []minio.CompletePart, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	var completeParts []objectlayer.CompletePart
	if parts != nil {
		completeParts = make([]objectlayer.CompletePart, len(parts))
	}
	for i, part := range parts {
		completeParts[i] = objectlayer.CompletePart{PartNumber: part.PartNumber, ETag: part.ETag}
	}
	info, err := a.layer.CompleteMultipartUpload(ctx, bucket, object, uploadID, completeParts, fromMinioObjectOptions(opts))
	return toMinioObjectInfo(info), err
}

func toMinioObjectInfo(info objectlayer.ObjectInfo) minio.ObjectInfo {
	return minio.ObjectInfo{
		Bucket:          info.Bucket,
		Name:            info.Name,
		ModTime:         info.ModTime,
		Size:            info.Size,
		ETag:            info.ETag,
		VersionID:       info.VersionID,
		IsLatest:        info.IsLatest,
		DeleteMarker:    info.DeleteMarker,
		ContentType:     info.ContentType,
		ContentEncoding: info.ContentEncoding,
		Expires:         info.Expires,
		StorageClass:    info.StorageClass,
		UserDefined:     info.UserDefined,
		UserTags:        info.UserTags,
	}
}

func toMinioObjectInfos(infos []objectlayer.ObjectInfo) []minio.ObjectInfo {
	if infos == nil {
		return nil
	}
	minioInfos := make([]minio.ObjectInfo, len(infos))
	for i, info := range infos {
		minioInfos[i] = toMinioObjectInfo(info)
	}
	return minioInfos
}

func fromMinioObjectInfo(info minio.ObjectInfo) objectlayer.ObjectInfo {
	return objectlayer.ObjectInfo{
		Bucket:          info.Bucket,
		Name:            info.Name,
		ModTime:         info.ModTime,
		Size:            info.Size,
		ETag:            info.ETag,
		VersionID:       info.VersionID,
		IsLatest:        info.IsLatest,
		DeleteMarker:    info.DeleteMarker,
		ContentType:     info.ContentType,
		ContentEncoding: info.ContentEncoding,
		Expires:         info.Expires,
		StorageClass:    info.StorageClass,
		UserDefined:     info.UserDefined,
		UserTags:        info.UserTags,
	}
}

// fromMinioObjectOptions returns opts as objectlayer.ObjectOptions. The part number is dropped,
// because minio's handlers read parts as ranges of the entire object, which they take objects
// from the layer to be.
func fromMinioObjectOptions(opts minio.ObjectOptions) objectlayer.ObjectOptions {
	var conditions []objectlayer.PostPolicyCondition
	if opts.PostPolicy.Conditions.Items != nil {
		conditions = make([]objectlayer.PostPolicyCondition, len(opts.PostPolicy.Conditions.Items))
	}
	for i, condition := range opts.PostPolicy.Conditions.Items {
		conditions[i] = objectlayer.PostPolicyCondition(condition)
	}

	var checkPrecondFn objectlayer.CheckPreconditionFn
	if fn := opts.CheckPrecondFn; fn != nil {
		checkPrecondFn = func(info objectlayer.ObjectInfo) bool { return fn(toMinioObjectInfo(info)) }
	}

	return objectlayer.ObjectOptions{
		VersionID: opts.VersionID,
		PostPolicy: objectlayer.PostPolicyForm{
			Expiration: opts.PostPolicy.Expiration,
			Conditions: objectlayer.PostPolicyConditions{
				Items:              conditions,
				ContentLengthRange: objectlayer.ContentLengthRange(opts.PostPolicy.Conditions.ContentLengthRange),
			},
		},
		Retention:                 opts.Retention,
		BypassGovernanceRetention: opts.BypassGovernanceRetention,
		LegalHold:                 opts.LegalHold,
		IfNoneMatch:               opts.IfNoneMatch,
		UserDefined:               opts.UserDefined,
		CheckPrecondFn:            checkPrecondFn,
		Quiet:                     opts.Quiet,
	}
}

func toMinioPartInfo(info objectlayer.PartInfo) minio.PartInfo {
	return minio.PartInfo{
		PartNumber:   info.PartNumber,
		LastModified: info.LastModified,
		ETag:         info.ETag,
		Size:         info.Size,
		ActualSize:   info.ActualSize,
	}
}
