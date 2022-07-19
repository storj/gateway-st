// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"context"
	"errors"
	"net/http"
	"reflect"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/zeebo/errs"
	"go.uber.org/zap"

	"storj.io/common/errs2"
	minio "storj.io/minio/cmd"
	"storj.io/minio/pkg/auth"
	"storj.io/minio/pkg/bucket/policy"
	"storj.io/uplink"
)

type singleTenantGateway struct {
	log     *zap.Logger
	access  *uplink.Access
	config  uplink.Config
	gateway minio.Gateway
	website bool
}

// NewSingleTenantGateway returns a wrapper of minio.Gateway that logs responses
// and makes gateway single-tenant.
func NewSingleTenantGateway(log *zap.Logger, access *uplink.Access, config uplink.Config, gateway minio.Gateway, website bool) minio.Gateway {
	return &singleTenantGateway{
		log:     log,
		access:  access,
		config:  config,
		gateway: gateway,
		website: website,
	}
}

func (g *singleTenantGateway) Name() string { return g.gateway.Name() }

func (g *singleTenantGateway) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	project, err := g.config.OpenProject(minio.GlobalContext, g.access)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	layer, err := g.gateway.NewGatewayLayer(creds)

	return &singleTenancyLayer{
		logger:  g.log,
		project: project,
		layer:   layer,
		website: g.website,
	}, err
}

func (g *singleTenantGateway) Production() bool { return g.gateway.Production() }

type singleTenancyLayer struct {
	minio.GatewayUnsupported

	logger  *zap.Logger
	project *uplink.Project
	layer   minio.ObjectLayer

	website bool
}

// minioError checks if the given error is a minio error.
func minioError(err error) bool {
	// some minio errors are not minio.GenericError, so we need to check for
	// these specifically.
	switch {
	case errors.As(err, &miniogo.ErrorResponse{}):
		return true
	default:
		return reflect.TypeOf(err).ConvertibleTo(reflect.TypeOf(minio.GenericError{}))
	}
}

// log unexpected errors, i.e. non-minio errors. It will return the given error
// to allow method chaining.
func (l *singleTenancyLayer) log(err error) error {
	// most of the time context canceled is intentionally caused by the client
	// to keep log message clean, we will only log it on debug level
	if errs2.IsCanceled(err) {
		l.logger.Debug("error:", zap.Error(err))
		return err
	}

	if err != nil && !minioError(err) {
		l.logger.Error("error:", zap.Error(err))
	}

	return err
}

func (l *singleTenancyLayer) Shutdown(ctx context.Context) error {
	return l.log(l.project.Close())
}

func (l *singleTenancyLayer) StorageInfo(ctx context.Context) (minio.StorageInfo, []error) {
	info, errors := l.layer.StorageInfo(WithUplinkProject(ctx, l.project))

	for _, err := range errors {
		_ = l.log(err)
	}

	return info, errors
}

func (l *singleTenancyLayer) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	return l.log(l.layer.MakeBucketWithLocation(WithUplinkProject(ctx, l.project), bucket, opts))
}

func (l *singleTenancyLayer) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo minio.BucketInfo, err error) {
	bucketInfo, err = l.layer.GetBucketInfo(WithUplinkProject(ctx, l.project), bucket)
	return bucketInfo, l.log(err)
}

func (l *singleTenancyLayer) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	buckets, err = l.layer.ListBuckets(WithUplinkProject(ctx, l.project))
	return buckets, l.log(err)
}

func (l *singleTenancyLayer) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	return l.log(l.layer.DeleteBucket(WithUplinkProject(ctx, l.project), bucket, forceDelete))
}

func (l *singleTenancyLayer) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	result, err = l.layer.ListObjects(WithUplinkProject(ctx, l.project), bucket, prefix, marker, delimiter, maxKeys)
	return result, l.log(err)
}

func (l *singleTenancyLayer) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	result, err = l.layer.ListObjectsV2(WithUplinkProject(ctx, l.project), bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
	return result, l.log(err)
}

func (l *singleTenancyLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (reader *minio.GetObjectReader, err error) {
	reader, err = l.layer.GetObjectNInfo(WithUplinkProject(ctx, l.project), bucket, object, rs, h, lockType, opts)
	return reader, l.log(err)
}

func (l *singleTenancyLayer) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = l.layer.GetObjectInfo(WithUplinkProject(ctx, l.project), bucket, object, opts)
	return objInfo, l.log(err)
}

func (l *singleTenancyLayer) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = l.layer.PutObject(WithUplinkProject(ctx, l.project), bucket, object, data, opts)
	return objInfo, l.log(err)
}

func (l *singleTenancyLayer) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, destOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = l.layer.CopyObject(WithUplinkProject(ctx, l.project), srcBucket, srcObject, destBucket, destObject, srcInfo, srcOpts, destOpts)
	return objInfo, l.log(err)
}

func (l *singleTenancyLayer) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = l.layer.DeleteObject(WithUplinkProject(ctx, l.project), bucket, object, opts)
	return objInfo, l.log(err)
}

func (l *singleTenancyLayer) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) (deleted []minio.DeletedObject, errors []error) {
	deleted, errors = l.layer.DeleteObjects(WithUplinkProject(ctx, l.project), bucket, objects, opts)

	for _, err := range errors {
		_ = l.log(err)
	}

	return deleted, errors
}

func (l *singleTenancyLayer) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result minio.ListMultipartsInfo, err error) {
	result, err = l.layer.ListMultipartUploads(WithUplinkProject(ctx, l.project), bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	return result, l.log(err)
}

func (l *singleTenancyLayer) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	uploadID, err = l.layer.NewMultipartUpload(WithUplinkProject(ctx, l.project), bucket, object, opts)
	return uploadID, l.log(err)
}

func (l *singleTenancyLayer) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	info, err = l.layer.PutObjectPart(WithUplinkProject(ctx, l.project), bucket, object, uploadID, partID, data, opts)
	return info, l.log(err)
}

func (l *singleTenancyLayer) GetMultipartInfo(ctx context.Context, bucket string, object string, uploadID string, opts minio.ObjectOptions) (info minio.MultipartInfo, err error) {
	info, err = l.layer.GetMultipartInfo(WithUplinkProject(ctx, l.project), bucket, object, uploadID, opts)
	return info, l.log(err)
}

func (l *singleTenancyLayer) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	result, err = l.layer.ListObjectParts(WithUplinkProject(ctx, l.project), bucket, object, uploadID, partNumberMarker, maxParts, opts)
	return result, l.log(err)
}

func (l *singleTenancyLayer) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) error {
	return l.log(l.layer.AbortMultipartUpload(WithUplinkProject(ctx, l.project), bucket, object, uploadID, opts))
}

func (l *singleTenancyLayer) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = l.layer.CompleteMultipartUpload(WithUplinkProject(ctx, l.project), bucket, object, uploadID, uploadedParts, opts)
	return objInfo, l.log(err)
}

func (l *singleTenancyLayer) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	if !l.website {
		return nil, minio.NotImplemented{}
	}

	// Allow reading any file from any bucket
	return &policy.Policy{
		Version: "2012-10-17",
		Statements: []policy.Statement{
			{
				Effect:    policy.Allow,
				Principal: policy.NewPrincipal("*"),
				Actions: policy.NewActionSet(
					policy.GetBucketLocationAction,
					policy.ListBucketAction,
				),
				Resources: policy.NewResourceSet(
					policy.NewResource(bucket, ""),
				),
			},
			{
				Effect:    policy.Allow,
				Principal: policy.NewPrincipal("*"),
				Actions: policy.NewActionSet(
					policy.GetObjectAction,
				),
				Resources: policy.NewResourceSet(
					policy.NewResource(bucket, "*"),
				),
			},
		},
	}, nil
}

func (l *singleTenancyLayer) IsTaggingSupported() bool {
	return l.layer.IsTaggingSupported()
}

func (l *singleTenancyLayer) PutObjectTags(ctx context.Context, bucketName, objectPath string, tags string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	objInfo, err := l.layer.PutObjectTags(WithUplinkProject(ctx, l.project), bucketName, objectPath, tags, opts)
	return objInfo, l.log(err)
}

func (l *singleTenancyLayer) GetObjectTags(ctx context.Context, bucketName, objectPath string, opts minio.ObjectOptions) (t *tags.Tags, err error) {
	t, err = l.layer.GetObjectTags(WithUplinkProject(ctx, l.project), bucketName, objectPath, opts)
	return t, l.log(err)
}

func (l *singleTenancyLayer) DeleteObjectTags(ctx context.Context, bucketName, objectPath string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	objInfo, err := l.layer.DeleteObjectTags(WithUplinkProject(ctx, l.project), bucketName, objectPath, opts)
	return objInfo, l.log(err)
}
