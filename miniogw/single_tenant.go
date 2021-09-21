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
	"github.com/minio/minio/pkg/madmin"
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
func (s *singleTenancyLayer) log(err error) error {
	// most of the time context canceled is intentionally caused by the client
	// to keep log message clean, we will only log it on debug level
	if errs2.IsCanceled(err) {
		s.logger.Debug("error:", zap.Error(err))
		return err
	}

	if err != nil && !minioError(err) {
		s.logger.Error("error:", zap.Error(err))
	}
	return err
}

func (s *singleTenancyLayer) NewNSLock(bucket string, objects ...string) minio.RWLocker {
	return s.layer.NewNSLock(bucket, objects...)
}

func (s *singleTenancyLayer) Shutdown(ctx context.Context) error {
	return s.log(errs.Combine(s.layer.Shutdown(context.WithValue(ctx, UplinkProject, s.project)), s.project.Close()))
}

func (s *singleTenancyLayer) StorageInfo(ctx context.Context, local bool) (minio.StorageInfo, []error) {
	return s.layer.StorageInfo(context.WithValue(ctx, UplinkProject, s.project), false)
}

func (s *singleTenancyLayer) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	return s.log(s.layer.MakeBucketWithLocation(context.WithValue(ctx, UplinkProject, s.project), bucket, opts))
}

func (s *singleTenancyLayer) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo minio.BucketInfo, err error) {
	bucketInfo, err = s.layer.GetBucketInfo(context.WithValue(ctx, UplinkProject, s.project), bucket)
	return bucketInfo, s.log(err)
}

func (s *singleTenancyLayer) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	buckets, err = s.layer.ListBuckets(context.WithValue(ctx, UplinkProject, s.project))
	return buckets, s.log(err)
}

func (s *singleTenancyLayer) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	return s.log(s.layer.DeleteBucket(context.WithValue(ctx, UplinkProject, s.project), bucket, forceDelete))
}

func (s *singleTenancyLayer) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	result, err = s.layer.ListObjects(context.WithValue(ctx, UplinkProject, s.project), bucket, prefix, marker, delimiter, maxKeys)
	return result, s.log(err)
}

func (s *singleTenancyLayer) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	result, err = s.layer.ListObjectsV2(context.WithValue(ctx, UplinkProject, s.project), bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
	return result, s.log(err)
}

func (s *singleTenancyLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (reader *minio.GetObjectReader, err error) {
	reader, err = s.layer.GetObjectNInfo(context.WithValue(ctx, UplinkProject, s.project), bucket, object, rs, h, lockType, opts)
	return reader, s.log(err)
}

func (s *singleTenancyLayer) GetObject(ctx context.Context, bucket, object string, startOffset, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	return s.log(s.layer.GetObject(context.WithValue(ctx, UplinkProject, s.project), bucket, object, startOffset, length, writer, etag, opts))
}

func (s *singleTenancyLayer) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = s.layer.GetObjectInfo(context.WithValue(ctx, UplinkProject, s.project), bucket, object, opts)
	return objInfo, s.log(err)
}

func (s *singleTenancyLayer) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	if ok := s.active.tryAdd(bucket, object); !ok {
		return minio.ObjectInfo{}, minio.ObjectAlreadyExists{
			Bucket:    bucket,
			Object:    object,
			VersionID: "",
			Err:       errs.New("concurrent upload"),
		}
	}
	defer s.active.remove(bucket, object)
	objInfo, err = s.layer.PutObject(context.WithValue(ctx, UplinkProject, s.project), bucket, object, data, opts)
	return objInfo, s.log(err)
}

func (s *singleTenancyLayer) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, destOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = s.layer.CopyObject(context.WithValue(ctx, UplinkProject, s.project), srcBucket, srcObject, destBucket, destObject, srcInfo, srcOpts, destOpts)
	return objInfo, s.log(err)
}

func (s *singleTenancyLayer) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = s.layer.DeleteObject(context.WithValue(ctx, UplinkProject, s.project), bucket, object, opts)
	return objInfo, s.log(err)
}

func (s *singleTenancyLayer) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) (deleted []minio.DeletedObject, errors []error) {
	deleted, errors = s.layer.DeleteObjects(context.WithValue(ctx, UplinkProject, s.project), bucket, objects, opts)
	for _, err := range errors {
		_ = s.log(err)
	}
	return deleted, errors
}

func (s *singleTenancyLayer) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result minio.ListMultipartsInfo, err error) {
	result, err = s.layer.ListMultipartUploads(context.WithValue(ctx, UplinkProject, s.project), bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	return result, s.log(err)
}

func (s *singleTenancyLayer) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	uploadID, err = s.layer.NewMultipartUpload(context.WithValue(ctx, UplinkProject, s.project), bucket, object, opts)
	return uploadID, s.log(err)
}

func (s *singleTenancyLayer) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64, srcInfo minio.ObjectInfo, srcOpts, destOpts minio.ObjectOptions) (info minio.PartInfo, err error) {
	info, err = s.layer.CopyObjectPart(context.WithValue(ctx, UplinkProject, s.project), srcBucket, srcObject, destBucket, destObject, uploadID, partID, startOffset, length, srcInfo, srcOpts, destOpts)
	return info, s.log(err)
}

func (s *singleTenancyLayer) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	info, err = s.layer.PutObjectPart(context.WithValue(ctx, UplinkProject, s.project), bucket, object, uploadID, partID, data, opts)
	return info, s.log(err)
}

func (s *singleTenancyLayer) GetMultipartInfo(ctx context.Context, bucket string, object string, uploadID string, opts minio.ObjectOptions) (info minio.MultipartInfo, err error) {
	info, err = s.layer.GetMultipartInfo(context.WithValue(ctx, UplinkProject, s.project), bucket, object, uploadID, opts)
	return info, s.log(err)
}

func (s *singleTenancyLayer) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	result, err = s.layer.ListObjectParts(context.WithValue(ctx, UplinkProject, s.project), bucket, object, uploadID, partNumberMarker, maxParts, opts)
	return result, s.log(err)
}

func (s *singleTenancyLayer) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) error {
	return s.log(s.layer.AbortMultipartUpload(context.WithValue(ctx, UplinkProject, s.project), bucket, object, uploadID, opts))
}

func (s *singleTenancyLayer) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objInfo, err = s.layer.CompleteMultipartUpload(context.WithValue(ctx, UplinkProject, s.project), bucket, object, uploadID, uploadedParts, opts)
	return objInfo, s.log(err)
}

func (s *singleTenancyLayer) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	rv, err := s.layer.HealFormat(context.WithValue(ctx, UplinkProject, s.project), dryRun)
	return rv, s.log(err)
}

func (s *singleTenancyLayer) HealBucket(ctx context.Context, bucket string, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	rv, err := s.layer.HealBucket(context.WithValue(ctx, UplinkProject, s.project), bucket, opts)
	return rv, s.log(err)
}

func (s *singleTenancyLayer) HealObject(ctx context.Context, bucket, object, versionID string, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	rv, err := s.layer.HealObject(context.WithValue(ctx, UplinkProject, s.project), bucket, object, versionID, opts)
	return rv, s.log(err)
}

func (s *singleTenancyLayer) SetBucketPolicy(ctx context.Context, n string, p *policy.Policy) error {
	return s.log(s.layer.SetBucketPolicy(context.WithValue(ctx, UplinkProject, s.project), n, p))
}

func (s *singleTenancyLayer) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	if !s.website {
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

func (s *singleTenancyLayer) DeleteBucketPolicy(ctx context.Context, n string) error {
	return s.log(s.layer.DeleteBucketPolicy(context.WithValue(ctx, UplinkProject, s.project), n))
}

func (s *singleTenancyLayer) IsNotificationSupported() bool {
	return s.layer.IsNotificationSupported()
}

func (s *singleTenancyLayer) IsEncryptionSupported() bool {
	return s.layer.IsEncryptionSupported()
}

func (s *singleTenancyLayer) IsTaggingSupported() bool {
	return s.layer.IsTaggingSupported()
}

func (s *singleTenancyLayer) IsCompressionSupported() bool {
	return s.layer.IsCompressionSupported()
}

func (s *singleTenancyLayer) GetMetrics(ctx context.Context) (*minio.Metrics, error) {
	metrics, err := s.layer.GetMetrics(context.WithValue(ctx, UplinkProject, s.project))
	return metrics, s.log(err)
}

func (s *singleTenancyLayer) PutObjectTags(ctx context.Context, bucketName, objectPath string, tags string, opts minio.ObjectOptions) error {
	return s.log(s.layer.PutObjectTags(context.WithValue(ctx, UplinkProject, s.project), bucketName, objectPath, tags, opts))
}

func (s *singleTenancyLayer) GetObjectTags(ctx context.Context, bucketName, objectPath string, opts minio.ObjectOptions) (t *tags.Tags, err error) {
	t, err = s.layer.GetObjectTags(context.WithValue(ctx, UplinkProject, s.project), bucketName, objectPath, opts)
	return t, s.log(err)
}

func (s *singleTenancyLayer) DeleteObjectTags(ctx context.Context, bucketName, objectPath string, opts minio.ObjectOptions) error {
	return s.log(s.layer.DeleteObjectTags(context.WithValue(ctx, UplinkProject, s.project), bucketName, objectPath, opts))
}
