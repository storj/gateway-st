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

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	minio "storj.io/minio/cmd"
	"storj.io/minio/cmd/config/storageclass"
	xhttp "storj.io/minio/cmd/http"
	"storj.io/minio/pkg/auth"
	"storj.io/minio/pkg/hash"
	"storj.io/private/version"
	"storj.io/uplink"
)

var (
	mon = monkit.Package()

	// ErrProjectUsageLimit is a custom error for when a user has reached their
	// Satellite project upload limit.
	//
	// Note: we went with 403 Forbidden over 507 Insufficient Storage, as 507
	// doesn't fit this case exactly. 507 is for when the server cannot
	// physically store any further data, e.g. the disk has filled up. In this
	// case though the user can solve this themselves by upgrading their plan,
	// so it should be treated as a 4xx level error, not 5xx.
	ErrProjectUsageLimit = miniogo.ErrorResponse{
		Code:       "XStorjProjectLimits",
		StatusCode: http.StatusForbidden,
		Message:    "You have reached your Storj project upload limit on the Satellite.",
	}

	// ErrSlowDown is a custom error for when a user is exceeding Satellite
	// request rate limits. We don't use the built-in `minio.SlowDown{}` error
	// because minio doesn't expect this error would be returned by the object
	// API. e.g. it would map errors like `minio.InsufficientWriteQuorum` to
	// `minio.SlowDown`, but not SlowDown to itself.
	ErrSlowDown = miniogo.ErrorResponse{
		Code:       "SlowDown",
		StatusCode: http.StatusTooManyRequests,
		Message:    "Please reduce your request rate.",
	}

	// ErrNoUplinkProject is a custom error that indicates there was no
	// `*uplink.Project` in the context for the gateway to pick up. This error
	// may signal that passing credentials down to the object layer is working
	// incorrectly.
	ErrNoUplinkProject = errs.Class("uplink project")
)

// Gateway is the implementation of cmd.Gateway.
type Gateway struct {
	compatibilityConfig S3CompatibilityConfig
}

// NewStorjGateway creates a new Storj S3 gateway.
func NewStorjGateway(compatibilityConfig S3CompatibilityConfig) *Gateway {
	return &Gateway{
		compatibilityConfig: compatibilityConfig,
	}
}

// Name implements cmd.Gateway.
func (gateway *Gateway) Name() string {
	return "storj"
}

// NewGatewayLayer implements cmd.Gateway.
func (gateway *Gateway) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	return &gatewayLayer{
		compatibilityConfig: gateway.compatibilityConfig,
	}, nil
}

// Production implements cmd.Gateway.
func (gateway *Gateway) Production() bool {
	return version.Build.Release
}

type gatewayLayer struct {
	minio.GatewayUnsupported
	compatibilityConfig S3CompatibilityConfig
}

// Shutdown is a no-op. It's never called from the layer above.
func (layer *gatewayLayer) Shutdown(ctx context.Context) (err error) {
	return nil
}

func (layer *gatewayLayer) StorageInfo(ctx context.Context, local bool) (minio.StorageInfo, []error) {
	info := minio.StorageInfo{}
	info.Backend.Type = minio.BackendGateway
	info.Backend.GatewayOnline = true
	return info, nil
}

func (layer *gatewayLayer) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) (err error) {
	defer mon.Task()(&ctx)(&err)

	project, err := projectFromContext(ctx, bucket, "")
	if err != nil {
		return err
	}

	_, err = project.CreateBucket(ctx, bucket)

	return convertError(err, bucket, "")
}

func (layer *gatewayLayer) GetBucketInfo(ctx context.Context, bucketName string) (bucketInfo minio.BucketInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	project, err := projectFromContext(ctx, bucketName, "")
	if err != nil {
		return minio.BucketInfo{}, err
	}

	bucket, err := project.StatBucket(ctx, bucketName)
	if err != nil {
		return minio.BucketInfo{}, convertError(err, bucketName, "")
	}

	return minio.BucketInfo{
		Name:    bucket.Name,
		Created: bucket.Created,
	}, nil
}

func (layer *gatewayLayer) ListBuckets(ctx context.Context) (items []minio.BucketInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	project, err := projectFromContext(ctx, "", "")
	if err != nil {
		return nil, err
	}

	buckets := project.ListBuckets(ctx, nil)
	for buckets.Next() {
		info := buckets.Item()
		items = append(items, minio.BucketInfo{
			Name:    info.Name,
			Created: info.Created,
		})
	}
	if buckets.Err() != nil {
		return nil, convertError(buckets.Err(), "", "")
	}
	return items, nil
}

func (layer *gatewayLayer) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) (err error) {
	defer mon.Task()(&ctx)(&err)

	project, err := projectFromContext(ctx, bucket, "")
	if err != nil {
		return err
	}

	if forceDelete {
		_, err = project.DeleteBucketWithObjects(ctx, bucket)
		return convertError(err, bucket, "")
	}

	_, err = project.DeleteBucket(ctx, bucket)
	if errs.Is(err, uplink.ErrBucketNotEmpty) {
		// Check if the bucket contains any non-pending objects. If it doesn't,
		// this would mean there were initiated non-committed and non-aborted
		// multipart uploads. Other S3 implementations allow deletion of such
		// buckets, but with uplink, we need to explicitly force bucket
		// deletion.
		it := project.ListObjects(ctx, bucket, nil)
		if !it.Next() && it.Err() == nil {
			_, err = project.DeleteBucketWithObjects(ctx, bucket)
			return convertError(err, bucket, "")
		}
	}

	return convertError(err, bucket, "")
}

// limitMaxKeys returns maxKeys limited to what gateway is configured to limit
// maxKeys to, aligned with paging limitations on the satellite side. It will
// also return the highest limit possible if maxKeys is not positive.
func (layer *gatewayLayer) limitMaxKeys(maxKeys int) int {
	if maxKeys <= 0 || maxKeys >= layer.compatibilityConfig.MaxKeysLimit {
		// Return max keys with a buffer to gather the continuation token to
		// avoid paging problems until we have a method in libuplink to get more
		// info about page boundaries.
		return layer.compatibilityConfig.MaxKeysLimit - 1
	}
	return maxKeys
}

func (layer *gatewayLayer) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if delimiter != "" && delimiter != "/" {
		return minio.ListObjectsInfo{}, minio.UnsupportedDelimiter{Delimiter: delimiter}
	}

	project, err := projectFromContext(ctx, bucket, "")
	if err != nil {
		return minio.ListObjectsInfo{}, err
	}

	// TODO this should be removed and implemented on satellite side
	defer func() {
		err = checkBucketError(ctx, project, bucket, "", err)
	}()

	recursive := delimiter == ""

	startAfter := marker

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
		//    functionality)
		// 3) handle strictly more of the use cases than #1 without
		//    loss of performance by turning this into a StatObject.
		// so we do #3 here. it's great!

		return listSingleObject(ctx, project, bucket, prefix, startAfter, recursive, maxKeys)
	}

	list := project.ListObjects(ctx, bucket, &uplink.ListObjectsOptions{
		Prefix:    prefix,
		Cursor:    strings.TrimPrefix(startAfter, prefix),
		Recursive: recursive,

		System: true,
		Custom: layer.compatibilityConfig.IncludeCustomMetadataListing,
	})

	var objects []minio.ObjectInfo
	var prefixes []string

	limit := layer.limitMaxKeys(maxKeys)
	for limit > 0 && list.Next() {
		object := list.Item()

		limit--

		if object.IsPrefix {
			prefixes = append(prefixes, object.Key)
		} else {
			objects = append(objects, minioObjectInfo(bucket, "", object))
		}

		startAfter = object.Key
	}
	if list.Err() != nil {
		return result, convertError(list.Err(), bucket, "")
	}

	more := list.Next()
	if list.Err() != nil {
		return result, convertError(list.Err(), bucket, "")
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

func (layer *gatewayLayer) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	defer mon.Task()(&ctx)(&err)

	if delimiter != "" && delimiter != "/" {
		return minio.ListObjectsV2Info{}, minio.UnsupportedDelimiter{Delimiter: delimiter}
	}

	project, err := projectFromContext(ctx, bucket, "")
	if err != nil {
		return minio.ListObjectsV2Info{}, err
	}

	// TODO this should be removed and implemented on satellite side
	defer func() {
		err = checkBucketError(ctx, project, bucket, "", err)
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
		//    functionality)
		// 3) handle strictly more of the use cases than #1 without
		//    loss of performance by turning this into a StatObject.
		// so we do #3 here. it's great!

		return listSingleObjectV2(ctx, project, bucket, prefix, continuationToken, startAfterPath, recursive, maxKeys)
	}

	list := project.ListObjects(ctx, bucket, &uplink.ListObjectsOptions{
		Prefix:    prefix,
		Cursor:    strings.TrimPrefix(startAfterPath, prefix),
		Recursive: recursive,

		System: true,
		Custom: layer.compatibilityConfig.IncludeCustomMetadataListing,
	})

	var objects []minio.ObjectInfo
	var prefixes []string

	limit := layer.limitMaxKeys(maxKeys)
	for limit > 0 && list.Next() {
		object := list.Item()

		limit--

		if object.IsPrefix {
			prefixes = append(prefixes, object.Key)
		} else {
			objects = append(objects, minioObjectInfo(bucket, "", object))
		}

		startAfter = object.Key
	}
	if list.Err() != nil {
		return result, convertError(list.Err(), bucket, "")
	}

	more := list.Next()
	if list.Err() != nil {
		return result, convertError(list.Err(), bucket, "")
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

func listSingleObject(ctx context.Context, project *uplink.Project, bucket, key, marker string, recursive bool, maxKeys int) (result minio.ListObjectsInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	isTruncated, nextMarker, objects, prefixes, err := listSingle(ctx, project, bucket, key, marker, recursive, maxKeys)

	return minio.ListObjectsInfo{
		IsTruncated: isTruncated,
		NextMarker:  nextMarker,
		Objects:     objects,
		Prefixes:    prefixes,
	}, err // already converted/wrapped
}

func listSingleObjectV2(ctx context.Context, project *uplink.Project, bucket, key, continuationToken, startAfterPath string, recursive bool, maxKeys int) (result minio.ListObjectsV2Info, err error) {
	defer mon.Task()(&ctx)(&err)

	isTruncated, nextMarker, objects, prefixes, err := listSingle(ctx, project, bucket, key, startAfterPath, recursive, maxKeys)

	return minio.ListObjectsV2Info{
		IsTruncated:           isTruncated,
		ContinuationToken:     continuationToken,
		NextContinuationToken: nextMarker,
		Objects:               objects,
		Prefixes:              prefixes,
	}, err // already converted/wrapped
}

func listSingle(ctx context.Context, project *uplink.Project, bucket, key, marker string, recursive bool, maxKeys int) (isTruncated bool, nextMarker string, objects []minio.ObjectInfo, prefixes []string, err error) {
	defer mon.Task()(&ctx)(&err)

	if marker == "" {
		object, err := project.StatObject(ctx, bucket, key)
		if err != nil {
			if !errors.Is(err, uplink.ErrObjectNotFound) {
				return false, "", nil, nil, convertError(err, bucket, key)
			}
		} else {
			objects = append(objects, minioObjectInfo(bucket, "", object))

			if maxKeys == 1 {
				return true, key, objects, nil, nil
			}
		}
	}

	if !recursive && (marker == "" || marker == key) {
		list := project.ListObjects(ctx, bucket, &uplink.ListObjectsOptions{
			Prefix:    key + "/",
			Recursive: true,
			// Limit: 1, would be nice to set here
		})
		if list.Next() {
			prefixes = append(prefixes, key+"/")
		}
		if err := list.Err(); err != nil {
			return false, "", nil, nil, convertError(err, bucket, key)
		}
	}

	return false, "", objects, prefixes, nil
}

func (layer *gatewayLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (reader *minio.GetObjectReader, err error) {
	defer mon.Task()(&ctx)(&err)

	project, err := projectFromContext(ctx, bucket, object)
	if err != nil {
		return nil, err
	}

	// TODO this should be removed and implemented on satellite side
	defer func() {
		err = checkBucketError(ctx, project, bucket, object, err)
	}()

	downloadOpts, err := rangeSpecToDownloadOptions(rs)
	if err != nil {
		return nil, err
	}

	download, err := project.DownloadObject(ctx, bucket, object, downloadOpts)
	if err != nil {
		return nil, convertError(err, bucket, object)
	}

	objectInfo := minioObjectInfo(bucket, "", download.Info())
	downloadCloser := func() { _ = download.Close() }

	return minio.NewGetObjectReaderFromReader(download, objectInfo, opts, downloadCloser)
}

func rangeSpecToDownloadOptions(rs *minio.HTTPRangeSpec) (opts *uplink.DownloadOptions, err error) {
	switch {
	// Case 1: Not present -> represented by a nil RangeSpec
	case rs == nil:
		return nil, nil
	// Case 2: bytes=1-10 (absolute start and end offsets) -> RangeSpec{false, 1, 10}
	case rs.Start >= 0 && rs.End >= 0 && !rs.IsSuffixLength:
		return &uplink.DownloadOptions{
			Offset: rs.Start,
			Length: rs.End - rs.Start + 1,
		}, nil
	// Case 3: bytes=10- (absolute start offset with end offset unspecified) -> RangeSpec{false, 10, -1}
	case rs.Start >= 0 && rs.End == -1 && !rs.IsSuffixLength:
		return &uplink.DownloadOptions{
			Offset: rs.Start,
			Length: -1,
		}, nil
	// Case 4: bytes=-30 (suffix length specification) -> RangeSpec{true, -30, -1}
	case rs.Start <= 0 && rs.End == -1 && rs.IsSuffixLength:
		if rs.Start == 0 {
			return &uplink.DownloadOptions{Offset: 0, Length: 0}, nil
		}
		return &uplink.DownloadOptions{
			Offset: rs.Start,
			Length: -1,
		}, nil
	default:
		return nil, errs.New("Unexpected range specification case: %#v", rs)
	}
}

func (layer *gatewayLayer) GetObject(ctx context.Context, bucket, objectPath string, startOffset, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) (err error) {
	defer mon.Task()(&ctx)(&err)

	project, err := projectFromContext(ctx, bucket, objectPath)
	if err != nil {
		return err
	}

	download, err := project.DownloadObject(ctx, bucket, objectPath, &uplink.DownloadOptions{
		Offset: startOffset,
		Length: length,
	})
	if err != nil {
		// TODO this should be removed and implemented on satellite side
		err = checkBucketError(ctx, project, bucket, objectPath, err)
		return convertError(err, bucket, objectPath)
	}
	defer func() { err = errs.Combine(err, download.Close()) }()

	object := download.Info()
	if startOffset < 0 || length < -1 {
		return minio.InvalidRange{
			OffsetBegin:  startOffset,
			OffsetEnd:    startOffset + length,
			ResourceSize: object.System.ContentLength,
		}
	}

	_, err = io.Copy(writer, download)

	return convertError(err, bucket, objectPath)
}

func (layer *gatewayLayer) GetObjectInfo(ctx context.Context, bucket, objectPath string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	project, err := projectFromContext(ctx, bucket, objectPath)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	object, err := project.StatObject(ctx, bucket, objectPath)
	if err != nil {
		// TODO this should be removed and implemented on satellite side
		err = checkBucketError(ctx, project, bucket, objectPath, err)
		return minio.ObjectInfo{}, convertError(err, bucket, objectPath)
	}

	return minioObjectInfo(bucket, "", object), nil
}

func (layer *gatewayLayer) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if storageClass, ok := opts.UserDefined[xhttp.AmzStorageClass]; ok && storageClass != storageclass.STANDARD {
		return minio.ObjectInfo{}, minio.NotImplemented{API: "PutObject (storage class)"}
	}

	project, err := projectFromContext(ctx, bucket, object)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	// TODO this should be removed and implemented on satellite side
	defer func() {
		err = checkBucketError(ctx, project, bucket, object, err)
	}()

	if data == nil {
		hashReader, err := hash.NewReader(bytes.NewReader([]byte{}), 0, "", "", 0, true)
		if err != nil {
			return minio.ObjectInfo{}, convertError(err, bucket, object)
		}
		data = minio.NewPutObjReader(hashReader, nil, nil)
	}

	upload, err := project.UploadObject(ctx, bucket, object, nil)
	if err != nil {
		return minio.ObjectInfo{}, convertError(err, bucket, object)
	}

	_, err = io.Copy(upload, data)
	if err != nil {
		abortErr := upload.Abort()
		err = errs.Combine(err, abortErr)
		return minio.ObjectInfo{}, convertError(err, bucket, object)
	}

	if tagsStr, ok := opts.UserDefined[xhttp.AmzObjectTagging]; ok {
		opts.UserDefined["s3:tags"] = tagsStr
		delete(opts.UserDefined, xhttp.AmzObjectTagging)
	}

	etag := data.MD5CurrentHexString()
	opts.UserDefined["s3:etag"] = etag

	err = upload.SetCustomMetadata(ctx, opts.UserDefined)
	if err != nil {
		abortErr := upload.Abort()
		err = errs.Combine(err, abortErr)
		return minio.ObjectInfo{}, convertError(err, bucket, object)
	}

	err = upload.Commit()
	if err != nil {
		return minio.ObjectInfo{}, convertError(err, bucket, object)
	}

	return minioObjectInfo(bucket, etag, upload.Info()), nil
}

func (layer *gatewayLayer) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, destOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if layer.compatibilityConfig.DisableCopyObject {
		// Note: In production Gateway-MT, we want to return Not Implemented until we implement server-side copy
		return minio.ObjectInfo{}, minio.NotImplemented{API: "CopyObject"}
	}

	if storageClass, ok := srcInfo.UserDefined[xhttp.AmzStorageClass]; ok && storageClass != storageclass.STANDARD {
		return minio.ObjectInfo{}, minio.NotImplemented{API: "CopyObject (storage class)"}
	}

	if srcObject == "" {
		return minio.ObjectInfo{}, minio.ObjectNameInvalid{Bucket: srcBucket}
	}
	if destObject == "" {
		return minio.ObjectInfo{}, minio.ObjectNameInvalid{Bucket: destBucket}
	}

	project, err := projectFromContext(ctx, srcBucket, srcObject)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	// TODO this should be removed and implemented on satellite side
	_, err = project.StatBucket(ctx, srcBucket)
	if err != nil {
		return minio.ObjectInfo{}, convertError(err, srcBucket, "")
	}

	// TODO this should be removed and implemented on satellite side
	if srcBucket != destBucket {
		_, err = project.StatBucket(ctx, destBucket)
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

	download, err := project.DownloadObject(ctx, srcBucket, srcObject, nil)
	if err != nil {
		return minio.ObjectInfo{}, convertError(err, srcBucket, srcObject)
	}
	defer func() {
		// TODO: this hides minio error
		err = errs.Combine(err, download.Close())
	}()

	upload, err := project.UploadObject(ctx, destBucket, destObject, nil)
	if err != nil {
		return minio.ObjectInfo{}, convertError(err, destBucket, destObject)
	}

	info := download.Info()

	// if X-Amz-Metadata-Directive header is provided and is set to "REPLACE",
	// then srcInfo.UserDefined will contain new metadata from the copy request.
	// If the directive is "COPY", or undefined, the source object's metadata
	// will be contained in srcInfo.UserDefined instead. This is done by the
	// caller handler in minio.

	// if X-Amz-Tagging-Directive is set to "REPLACE", we need to set the copied
	// object's tags to the tags provided in the copy request. If the directive
	// is "COPY", or undefined, copy over any existing tags from the source
	// object.
	if td, ok := srcInfo.UserDefined[xhttp.AmzTagDirective]; ok && td == "REPLACE" {
		srcInfo.UserDefined["s3:tags"] = srcInfo.UserDefined[xhttp.AmzObjectTagging]
	} else if tags, ok := info.Custom["s3:tags"]; ok {
		srcInfo.UserDefined["s3:tags"] = tags
	}

	delete(srcInfo.UserDefined, xhttp.AmzObjectTagging)
	delete(srcInfo.UserDefined, xhttp.AmzTagDirective)

	// if X-Amz-Metadata-Directive header is set to "REPLACE", then
	// srcInfo.UserDefined will be missing s3:etag, so make sure it's copied.
	srcInfo.UserDefined["s3:etag"] = info.Custom["s3:etag"]

	err = upload.SetCustomMetadata(ctx, srcInfo.UserDefined)
	if err != nil {
		abortErr := upload.Abort()
		err = errs.Combine(err, abortErr)
		return minio.ObjectInfo{}, convertError(err, destBucket, destObject)
	}

	reader, err := hash.NewReader(download, info.System.ContentLength, "", "", info.System.ContentLength, true)
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

func (layer *gatewayLayer) DeleteObject(ctx context.Context, bucket, objectPath string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	project, err := projectFromContext(ctx, bucket, objectPath)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	// TODO this should be removed and implemented on satellite side.
	// This call needs to occur prior to the DeleteObject call below, because
	// project.DeleteObject will return a nil error for a missing bucket. To
	// maintain consistency, we need to manually check if the bucket exists.
	_, err = project.StatBucket(ctx, bucket)
	if err != nil {
		return minio.ObjectInfo{}, convertError(err, bucket, objectPath)
	}

	object, err := project.DeleteObject(ctx, bucket, objectPath)
	if err != nil {
		return minio.ObjectInfo{}, convertError(err, bucket, objectPath)
	}

	return minioObjectInfo(bucket, "", object), nil
}

func (layer *gatewayLayer) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) ([]minio.DeletedObject, []error) {
	// TODO: implement multiple object deletion in libuplink API
	deleted, errs := make([]minio.DeletedObject, len(objects)), make([]error, len(objects))
	for i, object := range objects {
		_, deleteErr := layer.DeleteObject(ctx, bucket, object.ObjectName, opts)
		if deleteErr != nil && !errors.As(deleteErr, &minio.ObjectNotFound{}) {
			errs[i] = convertError(deleteErr, bucket, object.ObjectName)
			continue
		}
		deleted[i].ObjectName = object.ObjectName
	}
	return deleted, errs
}

func (layer *gatewayLayer) IsTaggingSupported() bool {
	return true
}

func (layer *gatewayLayer) PutObjectTags(ctx context.Context, bucket, objectPath string, tags string, opts minio.ObjectOptions) (err error) {
	defer mon.Task()(&ctx)(&err)

	project, err := projectFromContext(ctx, bucket, objectPath)
	if err != nil {
		return err
	}

	object, err := project.StatObject(ctx, bucket, objectPath)
	if err != nil {
		// TODO this should be removed and implemented on satellite side
		err = checkBucketError(ctx, project, bucket, objectPath, err)
		return convertError(err, bucket, objectPath)
	}

	if _, ok := object.Custom["s3:tags"]; !ok && tags == "" {
		return nil
	}

	newMetadata := object.Custom.Clone()
	if tags == "" {
		delete(newMetadata, "s3:tags")
	} else {
		newMetadata["s3:tags"] = tags
	}

	err = project.UpdateObjectMetadata(ctx, bucket, objectPath, newMetadata, nil)
	if err != nil {
		return convertError(err, bucket, objectPath)
	}

	return nil
}

func (layer *gatewayLayer) GetObjectTags(ctx context.Context, bucket, objectPath string, opts minio.ObjectOptions) (t *tags.Tags, err error) {
	defer mon.Task()(&ctx)(&err)

	project, err := projectFromContext(ctx, bucket, objectPath)
	if err != nil {
		return nil, err
	}

	object, err := project.StatObject(ctx, bucket, objectPath)
	if err != nil {
		// TODO this should be removed and implemented on satellite side
		err = checkBucketError(ctx, project, bucket, objectPath, err)
		return nil, convertError(err, bucket, objectPath)
	}

	t, err = tags.ParseObjectTags(object.Custom["s3:tags"])
	if err != nil {
		return nil, convertError(err, bucket, objectPath)
	}

	return t, nil
}

func (layer *gatewayLayer) DeleteObjectTags(ctx context.Context, bucket, objectPath string, opts minio.ObjectOptions) (err error) {
	defer mon.Task()(&ctx)(&err)

	project, err := projectFromContext(ctx, bucket, objectPath)
	if err != nil {
		return err
	}

	object, err := project.StatObject(ctx, bucket, objectPath)
	if err != nil {
		// TODO this should be removed and implemented on satellite side
		err = checkBucketError(ctx, project, bucket, objectPath, err)
		return convertError(err, bucket, objectPath)
	}

	if _, ok := object.Custom["s3:tags"]; !ok {
		return nil
	}

	newMetadata := object.Custom.Clone()
	delete(newMetadata, "s3:tags")

	err = project.UpdateObjectMetadata(ctx, bucket, objectPath, newMetadata, nil)
	if err != nil {
		return convertError(err, bucket, objectPath)
	}

	return nil
}

func projectFromContext(ctx context.Context, bucket, object string) (*uplink.Project, error) {
	pr, ok := GetUplinkProject(ctx)
	if !ok {
		return nil, convertError(ErrNoUplinkProject.New("not found"), bucket, object)
	}
	return pr, nil
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
	switch {
	case err == nil:
		return nil
	case errors.Is(err, uplink.ErrBucketNameInvalid):
		return minio.BucketNameInvalid{Bucket: bucket}
	case errors.Is(err, uplink.ErrBucketAlreadyExists):
		return minio.BucketAlreadyExists{Bucket: bucket}
	case errors.Is(err, uplink.ErrBucketNotFound):
		return minio.BucketNotFound{Bucket: bucket}
	case errors.Is(err, uplink.ErrBucketNotEmpty):
		return minio.BucketNotEmpty{Bucket: bucket}
	case errors.Is(err, uplink.ErrObjectKeyInvalid):
		return minio.ObjectNameInvalid{Bucket: bucket, Object: object}
	case errors.Is(err, uplink.ErrObjectNotFound):
		return minio.ObjectNotFound{Bucket: bucket, Object: object}
	case errors.Is(err, uplink.ErrBandwidthLimitExceeded):
		return ErrProjectUsageLimit
	case errors.Is(err, uplink.ErrPermissionDenied):
		return minio.PrefixAccessDenied{Bucket: bucket, Object: object}
	case errors.Is(err, uplink.ErrTooManyRequests):
		return ErrSlowDown
	case errors.Is(err, io.ErrUnexpectedEOF):
		return minio.IncompleteBody{Bucket: bucket, Object: object}
	default:
		return err
	}
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
