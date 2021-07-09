// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/minio/minio-go/v7/pkg/tags"
	minio "github.com/minio/minio/cmd"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/hash"
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/private/version"
	"storj.io/uplink"
)

var (
	mon = monkit.Package()

	// Error is the errs class of standard End User Client errors.
	Error = errs.Class("Storj Gateway error")
)

// Config allows configuration of some Gateway options.
type Config struct {
	Uplink  uplink.Config
	Website bool
}

// NewStorjGateway creates a new Storj S3 gateway.
func NewStorjGateway(access *uplink.Access, config Config) *Gateway {
	return &Gateway{
		access: access,
		config: config,
	}
}

// Gateway is the implementation of a minio cmd.Gateway.
type Gateway struct {
	access *uplink.Access
	config Config
}

// Name implements cmd.Gateway.
func (gateway *Gateway) Name() string {
	return "storj"
}

// NewGatewayLayer implements cmd.Gateway.
func (gateway *Gateway) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	ctx := minio.GlobalContext

	project, err := gateway.config.Uplink.OpenProject(ctx, gateway.access)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	return &gatewayLayer{
		gateway: gateway,
		active:  newActiveUploads(),
		project: project,
	}, nil
}

// Production implements cmd.Gateway.
func (gateway *Gateway) Production() bool {
	return version.Build.Release
}

type gatewayLayer struct {
	minio.GatewayUnsupported
	gateway *Gateway
	active  *activeUploads
	project *uplink.Project
}

func (layer *gatewayLayer) IsTaggingSupported() bool {
	return true
}

func (layer *gatewayLayer) DeleteBucket(ctx context.Context, bucketName string, forceDelete bool) (err error) {
	defer mon.Task()(&ctx)(&err)

	if forceDelete {
		_, err = layer.project.DeleteBucketWithObjects(ctx, bucketName)
		return convertError(err, bucketName, "")
	}

	_, err = layer.project.DeleteBucket(ctx, bucketName)

	return convertError(err, bucketName, "")
}

func (layer *gatewayLayer) DeleteObject(ctx context.Context, bucketName, objectPath string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	// TODO this should be removed and implemented on satellite side.
	// This call needs to occur prior to the DeleteObject call below, because
	// project.DeleteObject will return a nil error for a missing bucket. To
	// maintain consistency, we need to manually check if the bucket exists.
	_, err = layer.project.StatBucket(ctx, bucketName)
	if err != nil {
		return minio.ObjectInfo{}, convertError(err, bucketName, objectPath)
	}

	object, err := layer.project.DeleteObject(ctx, bucketName, objectPath)
	if err != nil {
		return minio.ObjectInfo{}, convertError(err, bucketName, objectPath)
	}

	return minioObjectInfo(bucketName, "", object), nil
}

func (layer *gatewayLayer) DeleteObjects(ctx context.Context, bucketName string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) (deleted []minio.DeletedObject, errs []error) {
	// TODO: implement multiple object deletion in libuplink API
	errs = make([]error, len(objects))
	deleted = make([]minio.DeletedObject, len(objects))
	for i, object := range objects {
		_, deleteErr := layer.DeleteObject(ctx, bucketName, object.ObjectName, opts)
		if deleteErr != nil && !errors.As(deleteErr, &minio.ObjectNotFound{}) {
			errs[i] = convertError(deleteErr, bucketName, object.ObjectName)
			continue
		}
		deleted[i].ObjectName = object.ObjectName
	}
	return deleted, errs
}

func (layer *gatewayLayer) GetBucketInfo(ctx context.Context, bucketName string) (bucketInfo minio.BucketInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	bucket, err := layer.project.StatBucket(ctx, bucketName)

	if err != nil {
		return minio.BucketInfo{}, convertError(err, bucketName, "")
	}

	return minio.BucketInfo{
		Name:    bucket.Name,
		Created: bucket.Created,
	}, nil
}

func (layer *gatewayLayer) GetObjectNInfo(ctx context.Context, bucketName, objectPath string, rangeSpec *minio.HTTPRangeSpec, header http.Header, lockType minio.LockType, opts minio.ObjectOptions) (reader *minio.GetObjectReader, err error) {
	defer mon.Task()(&ctx)(&err)

	// TODO this should be removed and implemented on satellite side
	defer func() {
		err = checkBucketError(ctx, layer.project, bucketName, objectPath, err)
	}()

	downloadOpts, err := rangeSpecToDownloadOptions(rangeSpec)
	if err != nil {
		return nil, err
	}

	download, err := layer.project.DownloadObject(ctx, bucketName, objectPath, downloadOpts)
	if err != nil {
		return nil, convertError(err, bucketName, objectPath)
	}

	objectInfo := minioObjectInfo(bucketName, "", download.Info())
	downloadCloser := func() { _ = download.Close() }

	return minio.NewGetObjectReaderFromReader(download, objectInfo, opts, downloadCloser)
}

func rangeSpecToDownloadOptions(spec *minio.HTTPRangeSpec) (opts *uplink.DownloadOptions, err error) {
	switch {
	// Case 1: Not present -> represented by a nil RangeSpec
	case spec == nil:
		return nil, nil

	// Case 2: bytes=1-10 (absolute start and end offsets) -> RangeSpec{false, 1, 10}
	case spec.Start >= 0 && spec.End >= 0 && !spec.IsSuffixLength:
		return &uplink.DownloadOptions{
			Offset: spec.Start,
			Length: spec.End - spec.Start + 1,
		}, nil

	// Case 3: bytes=10- (absolute start offset with end offset unspecified) -> RangeSpec{false, 10, -1}
	case spec.Start >= 0 && spec.End == -1 && !spec.IsSuffixLength:
		return &uplink.DownloadOptions{
			Offset: spec.Start,
			Length: -1,
		}, nil

	// Case 4: bytes=-30 (suffix length specification) -> RangeSpec{true, -30, -1}
	case spec.Start <= 0 && spec.End == -1 && spec.IsSuffixLength:
		if spec.Start == 0 {
			return &uplink.DownloadOptions{Offset: 0, Length: 0}, nil
		}
		return &uplink.DownloadOptions{
			Offset: spec.Start,
			Length: -1,
		}, nil

	default:
		return nil, errs.New("Unexpected range specification case: %#v", spec)
	}
}

func (layer *gatewayLayer) GetObjectInfo(ctx context.Context, bucketName, objectPath string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	object, err := layer.project.StatObject(ctx, bucketName, objectPath)
	if err != nil {
		// TODO this should be removed and implemented on satellite side
		err = checkBucketError(ctx, layer.project, bucketName, objectPath, err)
		return minio.ObjectInfo{}, convertError(err, bucketName, objectPath)
	}

	return minioObjectInfo(bucketName, "", object), nil
}

func (layer *gatewayLayer) PutObjectTags(ctx context.Context, bucketName, objectPath string, tags string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if err != nil {
		return objInfo, err
	}
	defer func() {
		err = errs.Combine(err, layer.project.Close())
	}()

	object, err := layer.project.StatObject(ctx, bucketName, objectPath)
	if err != nil {
		// TODO this should be removed and implemented on satellite side
		err = checkBucketError(ctx, layer.project, bucketName, objectPath, err)
		return objInfo, convertError(err, bucketName, objectPath)
	}

	if _, ok := object.Custom["s3:tags"]; !ok && tags == "" {
		return objInfo, nil
	}

	newMetadata := object.Custom.Clone()
	if tags == "" {
		delete(newMetadata, "s3:tags")
	} else {
		newMetadata["s3:tags"] = tags
	}

	err = layer.project.UpdateObjectMetadata(ctx, bucketName, objectPath, newMetadata, nil)
	if err != nil {
		return objInfo, convertError(err, bucketName, objectPath)
	}

	return minioObjectInfo(bucketName, "", object), nil
}

func (layer *gatewayLayer) GetObjectTags(ctx context.Context, bucketName, objectPath string, opts minio.ObjectOptions) (t *tags.Tags, err error) {
	defer mon.Task()(&ctx)(&err)

	if err != nil {
		return nil, err
	}
	defer func() {
		err = errs.Combine(err, layer.project.Close())
	}()

	object, err := layer.project.StatObject(ctx, bucketName, objectPath)
	if err != nil {
		// TODO this should be removed and implemented on satellite side
		err = checkBucketError(ctx, layer.project, bucketName, objectPath, err)
		return nil, convertError(err, bucketName, objectPath)
	}

	t, err = tags.ParseObjectTags(object.Custom["s3:tags"])
	if err != nil {
		return nil, convertError(err, bucketName, objectPath)
	}

	return t, nil
}
func (layer *gatewayLayer) DeleteObjectTags(ctx context.Context, bucketName, objectPath string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if err != nil {
		return objInfo, err
	}
	defer func() {
		err = errs.Combine(err, layer.project.Close())
	}()

	object, err := layer.project.StatObject(ctx, bucketName, objectPath)
	if err != nil {
		// TODO this should be removed and implemented on satellite side
		err = checkBucketError(ctx, layer.project, bucketName, objectPath, err)
		return objInfo, convertError(err, bucketName, objectPath)
	}

	if _, ok := object.Custom["s3:tags"]; !ok {
		return objInfo, nil
	}

	newMetadata := object.Custom.Clone()
	delete(newMetadata, "s3:tags")

	err = layer.project.UpdateObjectMetadata(ctx, bucketName, objectPath, newMetadata, nil)
	if err != nil {
		return objInfo, convertError(err, bucketName, objectPath)
	}

	return minioObjectInfo(bucketName, "", object), nil
}

func (layer *gatewayLayer) ListBuckets(ctx context.Context) (items []minio.BucketInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	buckets := layer.project.ListBuckets(ctx, nil)
	for buckets.Next() {
		info := buckets.Item()
		items = append(items, minio.BucketInfo{
			Name:    info.Name,
			Created: info.Created,
		})
	}
	if buckets.Err() != nil {
		return nil, buckets.Err()
	}
	return items, nil
}

func (layer *gatewayLayer) ListObjects(ctx context.Context, bucketName, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	// TODO maybe this should be checked by project.ListObjects
	if bucketName == "" {
		return minio.ListObjectsInfo{}, minio.BucketNameInvalid{}
	}

	if delimiter != "" && delimiter != "/" {
		return minio.ListObjectsInfo{}, minio.UnsupportedDelimiter{Delimiter: delimiter}
	}

	// TODO this should be removed and implemented on satellite side
	defer func() {
		err = checkBucketError(ctx, layer.project, bucketName, "", err)
	}()

	recursive := delimiter == ""

	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		// N.B.: in this case, the most S3-compatible thing we could do
		// is ask the satellite to list all siblings of this prefix that
		// share the same parent encryption key, decrypt all of them,
		// then only return the ones that have this same unencrypted
		// prefix.
		// this is terrible from a performance perspective, and it turns
		// out, many of the usages of listing without a /-suffix are
		// simply to provide a sort of StatObject like feature. in fact,
		// for example, duplicity never calls list without a /-suffix
		// in a case where it expects to get back more than one result.
		// so, we could either
		// 1) return an error here, guaranteeing nothing works
		// 2) do the full S3 compatible thing, which has terrible
		//    performance for a really common case (StatObject-like
		//		functionality)
		// 3) handle strictly more of the use cases than #1 without
		//    loss of performance by turning this into a StatObject.
		// so we do #3 here. it's great!

		return layer.listSingleObject(ctx, bucketName, prefix, marker, recursive, maxKeys)
	}

	list := layer.project.ListObjects(ctx, bucketName, &uplink.ListObjectsOptions{
		Prefix:    prefix,
		Cursor:    strings.TrimPrefix(marker, prefix),
		Recursive: recursive,

		System: true,
		Custom: true,
	})

	startAfter := marker
	var objects []minio.ObjectInfo
	var prefixes []string

	limit := maxKeys
	for (limit > 0 || maxKeys == 0) && list.Next() {
		object := list.Item()

		limit--

		if object.IsPrefix {
			prefixes = append(prefixes, object.Key)
		} else {
			objects = append(objects, minioObjectInfo(bucketName, "", object))
		}

		startAfter = object.Key
	}
	if list.Err() != nil {
		return result, convertError(list.Err(), bucketName, "")
	}

	more := list.Next()
	if list.Err() != nil {
		return result, convertError(list.Err(), bucketName, "")
	}

	result = minio.ListObjectsInfo{
		IsTruncated: more,
		Objects:     objects,
		Prefixes:    prefixes,
	}
	if more {
		result.NextMarker = startAfter
	}

	return result, nil
}

func (layer *gatewayLayer) listSingleObject(ctx context.Context, bucketName, key, marker string, recursive bool, maxKeys int) (result minio.ListObjectsInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	isTruncated, nextMarker, objects, prefixes, err := layer.listSingle(ctx, bucketName, key, marker, recursive, maxKeys)

	return minio.ListObjectsInfo{
		IsTruncated: isTruncated,
		NextMarker:  nextMarker,
		Objects:     objects,
		Prefixes:    prefixes,
	}, err // already converted/wrapped
}

func (layer *gatewayLayer) ListObjectsV2(ctx context.Context, bucketName, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	defer mon.Task()(&ctx)(&err)

	if delimiter != "" && delimiter != "/" {
		return minio.ListObjectsV2Info{}, minio.UnsupportedDelimiter{Delimiter: delimiter}
	}

	// TODO this should be removed and implemented on satellite side
	defer func() {
		err = checkBucketError(ctx, layer.project, bucketName, "", err)
	}()

	recursive := delimiter == ""

	var startAfterPath string

	if startAfter != "" {
		startAfterPath = startAfter
	}
	if continuationToken != "" {
		startAfterPath = continuationToken
	}

	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		// N.B.: in this case, the most S3-compatible thing we could do
		// is ask the satellite to list all siblings of this prefix that
		// share the same parent encryption key, decrypt all of them,
		// then only return the ones that have this same unencrypted
		// prefix.
		// this is terrible from a performance perspective, and it turns
		// out, many of the usages of listing without a /-suffix are
		// simply to provide a sort of StatObject like feature. in fact,
		// for example, duplicity never calls list without a /-suffix
		// in a case where it expects to get back more than one result.
		// so, we could either
		// 1) return an error here, guaranteeing nothing works
		// 2) do the full S3 compatible thing, which has terrible
		//    performance for a really common case (StatObject-like
		//		functionality)
		// 3) handle strictly more of the use cases than #1 without
		//    loss of performance by turning this into a StatObject.
		// so we do #3 here. it's great!

		return layer.listSingleObjectV2(ctx, bucketName, prefix, continuationToken, startAfterPath, recursive, maxKeys)
	}

	var objects []minio.ObjectInfo
	var prefixes []string

	list := layer.project.ListObjects(ctx, bucketName, &uplink.ListObjectsOptions{
		Prefix:    prefix,
		Cursor:    strings.TrimPrefix(startAfterPath, prefix),
		Recursive: recursive,

		System: true,
		Custom: true,
	})

	limit := maxKeys
	for (limit > 0 || maxKeys == 0) && list.Next() {
		object := list.Item()

		limit--

		if object.IsPrefix {
			prefixes = append(prefixes, object.Key)
		} else {
			objects = append(objects, minioObjectInfo(bucketName, "", object))
		}

		startAfter = object.Key
	}
	if list.Err() != nil {
		return result, convertError(list.Err(), bucketName, "")
	}

	more := list.Next()
	if list.Err() != nil {
		return result, convertError(list.Err(), bucketName, "")
	}

	result = minio.ListObjectsV2Info{
		IsTruncated:       more,
		ContinuationToken: continuationToken,
		Objects:           objects,
		Prefixes:          prefixes,
	}
	if more {
		result.NextContinuationToken = startAfter
	}

	return result, nil
}

func (layer *gatewayLayer) listSingleObjectV2(ctx context.Context, bucketName, key, continuationToken, startAfterPath string, recursive bool, maxKeys int) (result minio.ListObjectsV2Info, err error) {
	defer mon.Task()(&ctx)(&err)

	isTruncated, nextMarker, objects, prefixes, err := layer.listSingle(ctx, bucketName, key, startAfterPath, recursive, maxKeys)

	return minio.ListObjectsV2Info{
		IsTruncated:           isTruncated,
		ContinuationToken:     continuationToken,
		NextContinuationToken: nextMarker,
		Objects:               objects,
		Prefixes:              prefixes,
	}, err // already converted/wrapped
}

func (layer *gatewayLayer) listSingle(ctx context.Context, bucketName, key, marker string, recursive bool, maxKeys int) (isTruncated bool, nextMarker string, objects []minio.ObjectInfo, prefixes []string, err error) {
	defer mon.Task()(&ctx)(&err)

	if marker == "" {
		object, err := layer.project.StatObject(ctx, bucketName, key)
		if err != nil {
			if !errors.Is(err, uplink.ErrObjectNotFound) {
				return false, "", nil, nil, convertError(err, bucketName, key)
			}
		} else {
			objects = append(objects, minioObjectInfo(bucketName, "", object))

			if maxKeys == 1 {
				return true, key, objects, nil, nil
			}
		}
	}

	if !recursive && (marker == "" || marker == key) {
		list := layer.project.ListObjects(ctx, bucketName, &uplink.ListObjectsOptions{
			Prefix:    key + "/",
			Recursive: true,
			// Limit: 1, would be nice to set here
		})
		if list.Next() {
			prefixes = append(prefixes, key+"/")
		}
		if err := list.Err(); err != nil {
			return false, "", nil, nil, convertError(err, bucketName, key)
		}
	}

	return false, "", objects, prefixes, nil
}

func (layer *gatewayLayer) MakeBucketWithLocation(ctx context.Context, bucketName string, opts minio.BucketOptions) (err error) {
	defer mon.Task()(&ctx)(&err)

	// TODO: maybe this should return an error since we don't support locations

	_, err = layer.project.CreateBucket(ctx, bucketName)

	return convertError(err, bucketName, "")
}

func (layer *gatewayLayer) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, destOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if srcObject == "" {
		return minio.ObjectInfo{}, minio.ObjectNameInvalid{Bucket: srcBucket}
	}
	if destObject == "" {
		return minio.ObjectInfo{}, minio.ObjectNameInvalid{Bucket: destBucket}
	}

	// TODO this should be removed and implemented on satellite side
	_, err = layer.project.StatBucket(ctx, srcBucket)
	if err != nil {
		return minio.ObjectInfo{}, convertError(err, srcBucket, "")
	}

	// TODO this should be removed and implemented on satellite side
	if srcBucket != destBucket {
		_, err = layer.project.StatBucket(ctx, destBucket)
		if err != nil {
			return minio.ObjectInfo{}, convertError(err, destBucket, "")
		}
	}

	if srcBucket == destBucket && srcObject == destObject {
		// Source and destination are the same. Do nothing, otherwise copying
		// the same object over itself may destroy it, especially if it is a
		// larger one.
		return srcInfo, nil
	}

	download, err := layer.project.DownloadObject(ctx, srcBucket, srcObject, nil)
	if err != nil {
		return minio.ObjectInfo{}, convertError(err, srcBucket, srcObject)
	}
	defer func() {
		// TODO: this hides minio error
		err = errs.Combine(err, download.Close())
	}()

	upload, err := layer.project.UploadObject(ctx, destBucket, destObject, nil)
	if err != nil {
		return minio.ObjectInfo{}, convertError(err, destBucket, destObject)
	}

	info := download.Info()
	err = upload.SetCustomMetadata(ctx, info.Custom)
	if err != nil {
		abortErr := upload.Abort()
		err = errs.Combine(err, abortErr)
		return minio.ObjectInfo{}, convertError(err, destBucket, destObject)
	}

	reader, err := hash.NewReader(download, info.System.ContentLength, "", "", info.System.ContentLength)
	if err != nil {
		abortErr := upload.Abort()
		err = errs.Combine(err, abortErr)
		return minio.ObjectInfo{}, convertError(err, destBucket, destObject)
	}

	_, err = io.Copy(upload, reader)
	if err != nil {
		abortErr := upload.Abort()
		err = errs.Combine(err, abortErr)
		return minio.ObjectInfo{}, convertError(err, destBucket, destObject)
	}

	err = upload.Commit()
	if err != nil {
		return minio.ObjectInfo{}, convertError(err, destBucket, destObject)
	}

	return minioObjectInfo(destBucket, hex.EncodeToString(reader.MD5Current()), upload.Info()), nil
}

func (layer *gatewayLayer) PutObject(ctx context.Context, bucketName, objectPath string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	// TODO this should be removed and implemented on satellite side
	defer func() {
		err = checkBucketError(ctx, layer.project, bucketName, objectPath, err)
	}()

	if ok := layer.active.tryAdd(bucketName, objectPath); !ok {
		return minio.ObjectInfo{}, minio.ObjectAlreadyExists{
			Bucket:    bucketName,
			Object:    objectPath,
			VersionID: "",
			Err:       errors.New("concurrent upload"),
		}
	}
	defer layer.active.remove(bucketName, objectPath)

	if data == nil {
		hashReader, err := hash.NewReader(bytes.NewReader([]byte{}), 0, "", "", 0)
		if err != nil {
			return minio.ObjectInfo{}, convertError(err, bucketName, objectPath)
		}
		data = minio.NewPutObjReader(hashReader)
	}

	upload, err := layer.project.UploadObject(ctx, bucketName, objectPath, nil)
	if err != nil {
		return minio.ObjectInfo{}, convertError(err, bucketName, objectPath)
	}

	_, err = io.Copy(upload, data)
	if err != nil {
		abortErr := upload.Abort()
		err = errs.Combine(err, abortErr)
		return minio.ObjectInfo{}, convertError(err, bucketName, objectPath)
	}

	if opts.UserDefined == nil {
		opts.UserDefined = map[string]string{}
	}

	if tagsStr, ok := opts.UserDefined[xhttp.AmzObjectTagging]; ok {
		opts.UserDefined["s3:tags"] = tagsStr
		delete(opts.UserDefined, xhttp.AmzObjectTagging)
	}

	opts.UserDefined["s3:etag"] = hex.EncodeToString(data.MD5Current())
	err = upload.SetCustomMetadata(ctx, opts.UserDefined)
	if err != nil {
		abortErr := upload.Abort()
		err = errs.Combine(err, abortErr)
		return minio.ObjectInfo{}, convertError(err, bucketName, objectPath)
	}

	err = upload.Commit()
	if err != nil {
		return minio.ObjectInfo{}, convertError(err, bucketName, objectPath)
	}

	return minioObjectInfo(bucketName, opts.UserDefined["s3:etag"], upload.Info()), nil
}

func (layer *gatewayLayer) Shutdown(ctx context.Context) (err error) {
	defer mon.Task()(&ctx)(&err)
	return layer.project.Close()
}

func (layer *gatewayLayer) StorageInfo(ctx context.Context) (minio.StorageInfo, []error) {
	info := minio.StorageInfo{}
	info.Backend.Type = minio.BackendGateway
	info.Backend.GatewayOnline = layer.isSatelliteOnline(ctx)
	return info, nil
}

func (layer *gatewayLayer) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	if !layer.gateway.config.Website {
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

func (layer *gatewayLayer) isSatelliteOnline(ctx context.Context) bool {
	project, err := layer.gateway.config.Uplink.OpenProject(ctx, layer.gateway.access)
	if err != nil {
		return false
	}

	err = project.Close()
	return err == nil
}

// checkBucketError will stat the bucket if the provided error is not nil, in
// order to check if the proper error to return is really a bucket not found
// error. If the satellite has already returned this error, do not make an
// additional check.
func checkBucketError(ctx context.Context, project *uplink.Project, bucketName, object string, err error) error {
	if err != nil && !errors.Is(err, uplink.ErrBucketNotFound) {
		_, statErr := project.StatBucket(ctx, bucketName)
		if statErr != nil {
			return convertError(statErr, bucketName, object)
		}
	}
	return err
}

func convertError(err error, bucket, object string) error {
	if errors.Is(err, uplink.ErrBucketNameInvalid) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}

	if errors.Is(err, uplink.ErrBucketAlreadyExists) {
		return minio.BucketAlreadyExists{Bucket: bucket}
	}

	if errors.Is(err, uplink.ErrBucketNotFound) {
		return minio.BucketNotFound{Bucket: bucket}
	}

	if errors.Is(err, uplink.ErrBucketNotEmpty) {
		return minio.BucketNotEmpty{Bucket: bucket}
	}

	if errors.Is(err, uplink.ErrObjectKeyInvalid) {
		return minio.ObjectNameInvalid{Bucket: bucket, Object: object}
	}

	if errors.Is(err, uplink.ErrObjectNotFound) {
		return minio.ObjectNotFound{Bucket: bucket, Object: object}
	}

	if errors.Is(err, uplink.ErrBandwidthLimitExceeded) {
		return minio.BucketQuotaExceeded{Bucket: bucket}
	}

	return err
}

func minioObjectInfo(bucket, etag string, object *uplink.Object) minio.ObjectInfo {
	if object == nil {
		object = &uplink.Object{}
	}

	contentType := ""
	for k, v := range object.Custom {
		if strings.ToLower(k) == "content-type" {
			contentType = v
			break
		}
	}
	if etag == "" {
		etag = object.Custom["s3:etag"]
	}
	return minio.ObjectInfo{
		Bucket:      bucket,
		Name:        object.Key,
		Size:        object.System.ContentLength,
		ETag:        etag,
		ModTime:     object.System.Created,
		ContentType: contentType,
		UserDefined: object.Custom,
	}
}
