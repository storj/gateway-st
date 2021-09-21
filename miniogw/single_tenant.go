// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/tags"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/zeebo/errs"
	"go.uber.org/zap"

	"storj.io/common/errs2"
	"storj.io/uplink"
)

type contextKey string

// UplinkProject is a key intended to be passed to context.WithValue (and
// retrieved using Value) under which *uplink.Project should reside.
const UplinkProject contextKey = "UplinkProject"

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
		active:  newActiveUploads(),
	}, err
}

func (g *singleTenantGateway) Production() bool { return g.gateway.Production() }

type singleTenancyLayer struct {
	minio.GatewayUnsupported

	logger  *zap.Logger
	project *uplink.Project
	layer   minio.ObjectLayer

	website bool

	active *activeUploads
}

// minioError checks if the given error is a minio error.
func minioError(err error) bool {
	// some minio errors are not minio.GenericError, so we need to check for these specifically.
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
	return l.log(errs.Combine(l.layer.Shutdown(context.WithValue(ctx, UplinkProject, l.project)), l.project.Close()))
}

func (l *singleTenancyLayer) StorageInfo(ctx context.Context, local bool) (minio.StorageInfo, []error) {
	info, errors := l.layer.StorageInfo(context.WithValue(ctx, UplinkProject, l.project), false)

	for _, err := range errors {
		_ = l.log(err)
	}

	return info, errors
}

func (l *singleTenancyLayer) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	return l.log(l.layer.MakeBucketWithLocation(context.WithValue(ctx, UplinkProject, l.project), bucket, opts))
}

func (l *singleTenancyLayer) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo minio.BucketInfo, err error) {
	bucketInfo, err = l.layer.GetBucketInfo(context.WithValue(ctx, UplinkProject, l.project), bucket)
	return bucketInfo, l.log(err)
}

func (l *singleTenancyLayer) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	buckets, err = l.layer.ListBuckets(context.WithValue(ctx, UplinkProject, l.project))
	return buckets, l.log(err)
}

func (l *singleTenancyLayer) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	return l.log(l.layer.DeleteBucket(context.WithValue(ctx, UplinkProject, l.project), bucket, forceDelete))
}

func (l *singleTenancyLayer) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	result, err = l.layer.ListObjects(context.WithValue(ctx, UplinkProject, l.project), bucket, prefix, marker, delimiter, maxKeys)
	return result, l.log(err)
}

func (l *singleTenancyLayer) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	result, err = l.layer.ListObjectsV2(context.WithValue(ctx, UplinkProject, l.project), bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
	return result, l.log(err)
}

func (l *singleTenancyLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (reader *minio.GetObjectReader, err error) {
	reader, err = l.layer.GetObjectNInfo(context.WithValue(ctx, UplinkProject, l.project), bucket, object, rs, h, lockType, opts)
	return reader, l.log(err)
}

func (l *singleTenancyLayer) GetObject(ctx context.Context, bucket, object string, startOffset, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	return l.log(l.layer.GetObject(context.WithValue(ctx, UplinkProject, l.project), bucket, object, startOffset, length, writer, etag, opts))
}

func (l *singleTenancyLayer) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = l.layer.GetObjectInfo(context.WithValue(ctx, UplinkProject, l.project), bucket, object, opts)
	return objInfo, l.log(err)
}

func (l *singleTenancyLayer) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	if ok := l.active.tryAdd(bucket, object); !ok {
		return minio.ObjectInfo{}, minio.ObjectAlreadyExists{
			Bucket:    bucket,
			Object:    object,
			VersionID: "",
			Err:       errs.New("concurrent upload"),
		}
	}
	defer l.active.remove(bucket, object)

	objInfo, err = l.layer.PutObject(context.WithValue(ctx, UplinkProject, l.project), bucket, object, data, opts)

	return objInfo, l.log(err)
}

func (l *singleTenancyLayer) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, destOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = l.layer.CopyObject(context.WithValue(ctx, UplinkProject, l.project), srcBucket, srcObject, destBucket, destObject, srcInfo, srcOpts, destOpts)
	return objInfo, l.log(err)
}

func (l *singleTenancyLayer) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = l.layer.DeleteObject(context.WithValue(ctx, UplinkProject, l.project), bucket, object, opts)
	return objInfo, l.log(err)
}

func (l *singleTenancyLayer) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) (deleted []minio.DeletedObject, errors []error) {
	deleted, errors = l.layer.DeleteObjects(context.WithValue(ctx, UplinkProject, l.project), bucket, objects, opts)

	for _, err := range errors {
		_ = l.log(err)
	}

	return deleted, errors
}

func (l *singleTenancyLayer) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result minio.ListMultipartsInfo, err error) {
	result, err = l.layer.ListMultipartUploads(context.WithValue(ctx, UplinkProject, l.project), bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	return result, l.log(err)
}

func (l *singleTenancyLayer) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	uploadID, err = l.layer.NewMultipartUpload(context.WithValue(ctx, UplinkProject, l.project), bucket, object, opts)
	return uploadID, l.log(err)
}

func (l *singleTenancyLayer) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	info, err = l.layer.PutObjectPart(context.WithValue(ctx, UplinkProject, l.project), bucket, object, uploadID, partID, data, opts)
	return info, l.log(err)
}

func (l *singleTenancyLayer) GetMultipartInfo(ctx context.Context, bucket string, object string, uploadID string, opts minio.ObjectOptions) (info minio.MultipartInfo, err error) {
	info, err = l.layer.GetMultipartInfo(context.WithValue(ctx, UplinkProject, l.project), bucket, object, uploadID, opts)
	return info, l.log(err)
}

func (l *singleTenancyLayer) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	result, err = l.layer.ListObjectParts(context.WithValue(ctx, UplinkProject, l.project), bucket, object, uploadID, partNumberMarker, maxParts, opts)
	return result, l.log(err)
}

func (l *singleTenancyLayer) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) error {
	return l.log(l.layer.AbortMultipartUpload(context.WithValue(ctx, UplinkProject, l.project), bucket, object, uploadID, opts))
}

func (l *singleTenancyLayer) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = l.layer.CompleteMultipartUpload(context.WithValue(ctx, UplinkProject, l.project), bucket, object, uploadID, uploadedParts, opts)
	return objInfo, l.log(err)
}

func (l *singleTenancyLayer) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	if !l.website {
		return &policy.Policy{}, nil
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

func (l *singleTenancyLayer) PutObjectTags(ctx context.Context, bucketName, objectPath string, tags string, opts minio.ObjectOptions) error {
	return l.log(l.layer.PutObjectTags(context.WithValue(ctx, UplinkProject, l.project), bucketName, objectPath, tags, opts))
}

func (l *singleTenancyLayer) GetObjectTags(ctx context.Context, bucketName, objectPath string, opts minio.ObjectOptions) (t *tags.Tags, err error) {
	t, err = l.layer.GetObjectTags(context.WithValue(ctx, UplinkProject, l.project), bucketName, objectPath, opts)
	return t, l.log(err)
}

func (l *singleTenancyLayer) DeleteObjectTags(ctx context.Context, bucketName, objectPath string, opts minio.ObjectOptions) error {
	return l.log(l.layer.DeleteObjectTags(context.WithValue(ctx, UplinkProject, l.project), bucketName, objectPath, opts))
}
