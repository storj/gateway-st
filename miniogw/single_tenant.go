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
	objectlock "storj.io/minio/pkg/bucket/object/lock"
	"storj.io/minio/pkg/bucket/policy"
	"storj.io/minio/pkg/bucket/versioning"
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

func (g *singleTenantGateway) NewGatewayLayer(authCredentials auth.Credentials) (minio.ObjectLayer, error) {
	project, err := g.config.OpenProject(minio.GlobalContext, g.access)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	layer, err := g.gateway.NewGatewayLayer(authCredentials)

	return &singleTenancyLayer{
		logger:  g.log,
		project: project,
		credentialsInfo: CredentialsInfo{
			Access: g.access,
			// TODO(artur): fill the PublicProjectID field as well
		},
		layer:   layer,
		website: g.website,
	}, err
}

func (g *singleTenantGateway) Production() bool { return g.gateway.Production() }

type singleTenancyLayer struct {
	minio.GatewayUnsupported

	logger          *zap.Logger
	project         *uplink.Project
	credentialsInfo CredentialsInfo
	layer           minio.ObjectLayer

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
	var eg errs.Group
	eg.Add(l.log(l.project.Close()))
	eg.Add(l.layer.Shutdown(ctx))
	return eg.Err()
}

func (l *singleTenancyLayer) StorageInfo(ctx context.Context) (minio.StorageInfo, []error) {
	info, errors := l.layer.StorageInfo(WithCredentials(ctx, l.project, l.credentialsInfo))

	for _, err := range errors {
		_ = l.log(err)
	}

	return info, errors
}

func (l *singleTenancyLayer) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	return l.log(l.layer.MakeBucketWithLocation(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, opts))
}

func (l *singleTenancyLayer) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo minio.BucketInfo, err error) {
	bucketInfo, err = l.layer.GetBucketInfo(WithCredentials(ctx, l.project, l.credentialsInfo), bucket)
	return bucketInfo, l.log(err)
}

func (l *singleTenancyLayer) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	buckets, err = l.layer.ListBuckets(WithCredentials(ctx, l.project, l.credentialsInfo))
	return buckets, l.log(err)
}

func (l *singleTenancyLayer) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	return l.log(l.layer.DeleteBucket(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, forceDelete))
}

func (l *singleTenancyLayer) GetObjectLockConfig(ctx context.Context, bucket string) (objectLockConfig *objectlock.Config, err error) {
	objectLockConfig, err = l.layer.GetObjectLockConfig(WithCredentials(ctx, l.project, l.credentialsInfo), bucket)
	return objectLockConfig, l.log(err)
}

func (l *singleTenancyLayer) SetObjectLockConfig(ctx context.Context, bucket string, objectLockConfig *objectlock.Config) (err error) {
	return l.log(l.layer.SetObjectLockConfig(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, objectLockConfig))
}

func (l *singleTenancyLayer) GetObjectLegalHold(ctx context.Context, bucketName, object, version string) (_ *objectlock.ObjectLegalHold, err error) {
	lh, err := l.layer.GetObjectLegalHold(WithCredentials(ctx, l.project, l.credentialsInfo), bucketName, object, version)
	return lh, l.log(err)
}

func (l *singleTenancyLayer) SetObjectLegalHold(ctx context.Context, bucket, object, version string, lh *objectlock.ObjectLegalHold) (err error) {
	err = l.layer.SetObjectLegalHold(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, object, version, lh)
	return l.log(err)
}

func (l *singleTenancyLayer) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	result, err = l.layer.ListObjects(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, prefix, marker, delimiter, maxKeys)
	return result, l.log(err)
}

func (l *singleTenancyLayer) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	result, err = l.layer.ListObjectsV2(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
	return result, l.log(err)
}

func (l *singleTenancyLayer) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (result minio.ListObjectVersionsInfo, err error) {
	result, err = l.layer.ListObjectVersions(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, prefix, marker, versionMarker, delimiter, maxKeys)
	return result, l.log(err)
}

func (l *singleTenancyLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (reader *minio.GetObjectReader, err error) {
	reader, err = l.layer.GetObjectNInfo(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, object, rs, h, lockType, opts)
	return reader, l.log(err)
}

func (l *singleTenancyLayer) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = l.layer.GetObjectInfo(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, object, opts)
	return objInfo, l.log(err)
}

func (l *singleTenancyLayer) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = l.layer.PutObject(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, object, data, opts)
	return objInfo, l.log(err)
}

func (l *singleTenancyLayer) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, destOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = l.layer.CopyObject(WithCredentials(ctx, l.project, l.credentialsInfo), srcBucket, srcObject, destBucket, destObject, srcInfo, srcOpts, destOpts)
	return objInfo, l.log(err)
}

func (l *singleTenancyLayer) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = l.layer.DeleteObject(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, object, opts)
	return objInfo, l.log(err)
}

func (l *singleTenancyLayer) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) (deleted []minio.DeletedObject, deleteErrors []minio.DeleteObjectsError, err error) {
	deleted, deleteErrors, err = l.layer.DeleteObjects(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, objects, opts)

	for _, deleteError := range deleteErrors {
		_ = l.log(deleteError.Error)
	}

	return deleted, deleteErrors, l.log(err)
}

func (l *singleTenancyLayer) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result minio.ListMultipartsInfo, err error) {
	result, err = l.layer.ListMultipartUploads(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	return result, l.log(err)
}

func (l *singleTenancyLayer) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	uploadID, err = l.layer.NewMultipartUpload(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, object, opts)
	return uploadID, l.log(err)
}

func (l *singleTenancyLayer) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	info, err = l.layer.PutObjectPart(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, object, uploadID, partID, data, opts)
	return info, l.log(err)
}

func (l *singleTenancyLayer) GetMultipartInfo(ctx context.Context, bucket string, object string, uploadID string, opts minio.ObjectOptions) (info minio.MultipartInfo, err error) {
	info, err = l.layer.GetMultipartInfo(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, object, uploadID, opts)
	return info, l.log(err)
}

func (l *singleTenancyLayer) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	result, err = l.layer.ListObjectParts(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, object, uploadID, partNumberMarker, maxParts, opts)
	return result, l.log(err)
}

func (l *singleTenancyLayer) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) error {
	return l.log(l.layer.AbortMultipartUpload(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, object, uploadID, opts))
}

func (l *singleTenancyLayer) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = l.layer.CompleteMultipartUpload(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, object, uploadID, uploadedParts, opts)
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
	objInfo, err := l.layer.PutObjectTags(WithCredentials(ctx, l.project, l.credentialsInfo), bucketName, objectPath, tags, opts)
	return objInfo, l.log(err)
}

func (l *singleTenancyLayer) GetObjectTags(ctx context.Context, bucketName, objectPath string, opts minio.ObjectOptions) (t *tags.Tags, err error) {
	t, err = l.layer.GetObjectTags(WithCredentials(ctx, l.project, l.credentialsInfo), bucketName, objectPath, opts)
	return t, l.log(err)
}

func (l *singleTenancyLayer) DeleteObjectTags(ctx context.Context, bucketName, objectPath string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	objInfo, err := l.layer.DeleteObjectTags(WithCredentials(ctx, l.project, l.credentialsInfo), bucketName, objectPath, opts)
	return objInfo, l.log(err)
}

func (l *singleTenancyLayer) GetBucketVersioning(ctx context.Context, bucket string) (_ *versioning.Versioning, err error) {
	versioning, err := l.layer.GetBucketVersioning(WithCredentials(ctx, l.project, l.credentialsInfo), bucket)
	return versioning, l.log(err)
}

func (l *singleTenancyLayer) SetBucketVersioning(ctx context.Context, bucket string, v *versioning.Versioning) (err error) {
	err = l.layer.SetBucketVersioning(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, v)
	return l.log(err)
}

func (l *singleTenancyLayer) GetObjectRetention(ctx context.Context, bucket, object, versionID string) (_ *objectlock.ObjectRetention, err error) {
	retention, err := l.layer.GetObjectRetention(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, object, versionID)
	return retention, l.log(err)
}

func (l *singleTenancyLayer) SetObjectRetention(ctx context.Context, bucket, object, versionID string, opts minio.ObjectOptions) (err error) {
	err = l.layer.SetObjectRetention(WithCredentials(ctx, l.project, l.credentialsInfo), bucket, object, versionID, opts)
	return l.log(err)
}
