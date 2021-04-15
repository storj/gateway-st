// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"context"
	"net/http"
	"reflect"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/madmin"
	"go.uber.org/zap"

	"storj.io/common/errs2"
)

type gatewayLogging struct {
	gateway minio.Gateway
	log     *zap.Logger
}

// Logging returns a wrapper of minio.Gateway that logs errors before returning them.
func Logging(gateway minio.Gateway, log *zap.Logger) minio.Gateway {
	return &gatewayLogging{gateway, log}
}

func (lg *gatewayLogging) Name() string     { return lg.gateway.Name() }
func (lg *gatewayLogging) Production() bool { return lg.gateway.Production() }
func (lg *gatewayLogging) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	layer, err := lg.gateway.NewGatewayLayer(creds)
	return &layerLogging{layer: layer, logger: lg.log}, err
}

type layerLogging struct {
	minio.GatewayUnsupported
	layer  minio.ObjectLayer
	logger *zap.Logger
}

// minioError checks if the given error is a minio error.
func minioError(err error) bool {
	return reflect.TypeOf(err).ConvertibleTo(reflect.TypeOf(minio.GenericError{}))
}

// log unexpected errors, i.e. non-minio errors. It will return the given error
// to allow method chaining.
func (log *layerLogging) log(err error) error {
	// most of the time context canceled is intentionally caused by the client
	// to keep log message clean, we will only log it on debug level
	if errs2.IsCanceled(err) {
		log.logger.Debug("gateway error:", zap.Error(err))
		return err
	}

	if err != nil && !minioError(err) {
		log.logger.Error("gateway error:", zap.Error(err))
	}
	return err
}

func (log *layerLogging) NewNSLock(bucket string, objects ...string) minio.RWLocker {
	return log.layer.NewNSLock(bucket, objects...)
}

func (log *layerLogging) Shutdown(ctx context.Context) error {
	return log.log(log.layer.Shutdown(ctx))
}

func (log *layerLogging) StorageInfo(ctx context.Context) (minio.StorageInfo, []error) {
	return log.layer.StorageInfo(ctx)
}

func (log *layerLogging) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	return log.log(log.layer.MakeBucketWithLocation(ctx, bucket, opts))
}

func (log *layerLogging) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo minio.BucketInfo, err error) {
	bucketInfo, err = log.layer.GetBucketInfo(ctx, bucket)
	return bucketInfo, log.log(err)
}

func (log *layerLogging) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	buckets, err = log.layer.ListBuckets(ctx)
	return buckets, log.log(err)
}

func (log *layerLogging) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	return log.log(log.layer.DeleteBucket(ctx, bucket, forceDelete))
}

func (log *layerLogging) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	result, err = log.layer.ListObjects(ctx, bucket, prefix, marker, delimiter,
		maxKeys)
	return result, log.log(err)
}

func (log *layerLogging) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	result, err = log.layer.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
	return result, log.log(err)
}

func (log *layerLogging) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (reader *minio.GetObjectReader, err error) {
	reader, err = log.layer.GetObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
	return reader, log.log(err)
}

func (log *layerLogging) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = log.layer.GetObjectInfo(ctx, bucket, object, opts)
	return objInfo, log.log(err)
}

func (log *layerLogging) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = log.layer.PutObject(ctx, bucket, object, data, opts)
	return objInfo, log.log(err)
}

func (log *layerLogging) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, destOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = log.layer.CopyObject(ctx, srcBucket, srcObject, destBucket, destObject, srcInfo, srcOpts, destOpts)
	return objInfo, log.log(err)
}

func (log *layerLogging) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = log.layer.DeleteObject(ctx, bucket, object, opts)
	return objInfo, log.log(err)
}

func (log *layerLogging) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) (deleted []minio.DeletedObject, errors []error) {
	deleted, errors = log.layer.DeleteObjects(ctx, bucket, objects, opts)
	for _, err := range errors {
		_ = log.log(err)
	}
	return deleted, errors
}

func (log *layerLogging) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result minio.ListMultipartsInfo, err error) {
	result, err = log.layer.ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	return result, log.log(err)
}

func (log *layerLogging) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	uploadID, err = log.layer.NewMultipartUpload(ctx, bucket, object, opts)
	return uploadID, log.log(err)
}

func (log *layerLogging) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64, srcInfo minio.ObjectInfo, srcOpts, destOpts minio.ObjectOptions) (info minio.PartInfo, err error) {
	info, err = log.layer.CopyObjectPart(ctx, srcBucket, srcObject, destBucket, destObject, uploadID, partID, startOffset, length, srcInfo, srcOpts, destOpts)
	return info, log.log(err)
}

func (log *layerLogging) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	info, err = log.layer.PutObjectPart(ctx, bucket, object, uploadID, partID, data, opts)
	return info, log.log(err)
}

func (log *layerLogging) GetMultipartInfo(ctx context.Context, bucket string, object string, uploadID string, opts minio.ObjectOptions) (info minio.MultipartInfo, err error) {
	info, err = log.layer.GetMultipartInfo(ctx, bucket, object, uploadID, opts)
	return info, log.log(err)
}

func (log *layerLogging) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	result, err = log.layer.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, opts)
	return result, log.log(err)
}

func (log *layerLogging) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) error {
	return log.log(log.layer.AbortMultipartUpload(ctx, bucket, object, uploadID, opts))
}

func (log *layerLogging) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = log.layer.CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, opts)
	return objInfo, log.log(err)
}

func (log *layerLogging) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	rv, err := log.layer.HealFormat(ctx, dryRun)
	return rv, log.log(err)
}

func (log *layerLogging) HealBucket(ctx context.Context, bucket string, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	rv, err := log.layer.HealBucket(ctx, bucket, opts)
	return rv, log.log(err)
}

func (log *layerLogging) HealObject(ctx context.Context, bucket, object, versionID string, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	rv, err := log.layer.HealObject(ctx, bucket, object, versionID, opts)
	return rv, log.log(err)
}

func (log *layerLogging) SetBucketPolicy(ctx context.Context, n string, p *policy.Policy) error {
	return log.log(log.layer.SetBucketPolicy(ctx, n, p))
}

func (log *layerLogging) GetBucketPolicy(ctx context.Context, n string) (*policy.Policy, error) {
	p, err := log.layer.GetBucketPolicy(ctx, n)
	return p, log.log(err)
}

func (log *layerLogging) DeleteBucketPolicy(ctx context.Context, n string) error {
	return log.log(log.layer.DeleteBucketPolicy(ctx, n))
}

func (log *layerLogging) IsNotificationSupported() bool {
	return log.layer.IsNotificationSupported()
}

func (log *layerLogging) IsEncryptionSupported() bool {
	return log.layer.IsEncryptionSupported()
}

func (log *layerLogging) IsCompressionSupported() bool {
	return log.layer.IsCompressionSupported()
}

func (log *layerLogging) GetMetrics(ctx context.Context) (*minio.BackendMetrics, error) {
	metrics, err := log.layer.GetMetrics(ctx)
	return metrics, log.log(err)
}
