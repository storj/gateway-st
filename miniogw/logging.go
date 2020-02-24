// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"context"
	"io"
	"net/http"
	"reflect"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/auth"
	bucketsse "github.com/minio/minio/pkg/bucket/encryption"
	"github.com/minio/minio/pkg/bucket/lifecycle"
	"github.com/minio/minio/pkg/bucket/object/tagging"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/madmin"
	"go.uber.org/zap"
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
	if err != nil && !minioError(err) {
		log.logger.Error("gateway error:", zap.Error(err))
	}
	return err
}

func (log *layerLogging) NewNSLock(ctx context.Context, bucket string, objects ...string) minio.RWLocker {
	return log.layer.NewNSLock(ctx, bucket, objects...)
}

func (log *layerLogging) Shutdown(ctx context.Context) error {
	return log.log(log.layer.Shutdown(ctx))
}

func (log *layerLogging) CrawlAndGetDataUsage(ctx context.Context, endCh <-chan struct{}) minio.DataUsageInfo {
	return log.layer.CrawlAndGetDataUsage(ctx, endCh)
}

func (log *layerLogging) StorageInfo(ctx context.Context, local bool) minio.StorageInfo {
	return log.layer.StorageInfo(ctx, local)
}

func (log *layerLogging) MakeBucketWithLocation(ctx context.Context, bucket string, location string) error {
	return log.log(log.layer.MakeBucketWithLocation(ctx, bucket, location))
}

func (log *layerLogging) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo minio.BucketInfo, err error) {
	bucketInfo, err = log.layer.GetBucketInfo(ctx, bucket)
	return bucketInfo, log.log(err)
}

func (log *layerLogging) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	buckets, err = log.layer.ListBuckets(ctx)
	return buckets, log.log(err)
}

func (log *layerLogging) DeleteBucket(ctx context.Context, bucket string) error {
	return log.log(log.layer.DeleteBucket(ctx, bucket))
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

func (log *layerLogging) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) (err error) {
	return log.log(log.layer.GetObject(ctx, bucket, object, startOffset, length, writer, etag, opts))
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

func (log *layerLogging) DeleteObject(ctx context.Context, bucket, object string) (err error) {
	return log.log(log.layer.DeleteObject(ctx, bucket, object))
}

func (log *layerLogging) DeleteObjects(ctx context.Context, bucket string, objects []string) (errors []error, err error) {
	errors, err = log.layer.DeleteObjects(ctx, bucket, objects)
	return errors, log.log(err)
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

func (log *layerLogging) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	result, err = log.layer.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, opts)
	return result, log.log(err)
}

func (log *layerLogging) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	return log.log(log.layer.AbortMultipartUpload(ctx, bucket, object, uploadID))
}

func (log *layerLogging) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = log.layer.CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, opts)
	return objInfo, log.log(err)
}

func (log *layerLogging) ReloadFormat(ctx context.Context, dryRun bool) error {
	return log.log(log.layer.ReloadFormat(ctx, dryRun))
}

func (log *layerLogging) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	rv, err := log.layer.HealFormat(ctx, dryRun)
	return rv, log.log(err)
}

func (log *layerLogging) HealBucket(ctx context.Context, bucket string, dryRun, remove bool) (madmin.HealResultItem, error) {
	rv, err := log.layer.HealBucket(ctx, bucket, dryRun, remove)
	return rv, log.log(err)
}

func (log *layerLogging) HealObject(ctx context.Context, bucket, object string, dryRun, remove bool, scanMode madmin.HealScanMode) (madmin.HealResultItem, error) {
	rv, err := log.layer.HealObject(ctx, bucket, object, dryRun, remove, scanMode)
	return rv, log.log(err)
}

func (log *layerLogging) ListBucketsHeal(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	buckets, err = log.layer.ListBucketsHeal(ctx)
	return buckets, log.log(err)
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

func (log *layerLogging) IsListenBucketSupported() bool {
	return log.layer.IsListenBucketSupported()
}

func (log *layerLogging) IsEncryptionSupported() bool {
	return log.layer.IsEncryptionSupported()
}

func (log *layerLogging) IsCompressionSupported() bool {
	return log.layer.IsCompressionSupported()
}

func (log *layerLogging) SetBucketLifecycle(ctx context.Context, bucket string, lifecycle *lifecycle.Lifecycle) error {
	return log.log(log.layer.SetBucketLifecycle(ctx, bucket, lifecycle))
}

func (log *layerLogging) GetBucketLifecycle(ctx context.Context, bucket string) (*lifecycle.Lifecycle, error) {
	lifecycle, err := log.layer.GetBucketLifecycle(ctx, bucket)
	return lifecycle, log.log(err)
}

func (log *layerLogging) DeleteBucketLifecycle(ctx context.Context, bucket string) error {
	return log.log(log.layer.DeleteBucketLifecycle(ctx, bucket))
}

func (log *layerLogging) SetBucketSSEConfig(ctx context.Context, bucket string, config *bucketsse.BucketSSEConfig) error {
	return log.log(log.layer.SetBucketSSEConfig(ctx, bucket, config))
}

func (log *layerLogging) GetBucketSSEConfig(ctx context.Context, bucket string) (*bucketsse.BucketSSEConfig, error) {
	config, err := log.layer.GetBucketSSEConfig(ctx, bucket)
	return config, log.log(err)
}

func (log *layerLogging) DeleteBucketSSEConfig(ctx context.Context, bucket string) error {
	return log.log(log.layer.DeleteBucketSSEConfig(ctx, bucket))
}

func (log *layerLogging) GetMetrics(ctx context.Context) (*minio.Metrics, error) {
	metrics, err := log.layer.GetMetrics(ctx)
	return metrics, log.log(err)
}

func (log *layerLogging) IsReady(ctx context.Context) bool {
	return log.layer.IsReady(ctx)
}

func (log *layerLogging) PutObjectTag(ctx context.Context, bucket, object, tags string) error {
	return log.log(log.layer.PutObjectTag(ctx, bucket, object, tags))
}

func (log *layerLogging) GetObjectTag(ctx context.Context, bucket, object string) (tagging.Tagging, error) {
	tags, err := log.layer.GetObjectTag(ctx, bucket, object)
	return tags, log.log(err)
}

func (log *layerLogging) DeleteObjectTag(ctx context.Context, bucket, object string) error {
	return log.log(log.layer.DeleteObjectTag(ctx, bucket, object))
}
