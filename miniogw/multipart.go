// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/zeebo/errs"

	"storj.io/common/memory"
	"storj.io/common/sync2"
	minio "storj.io/minio/cmd"
	"storj.io/minio/cmd/config/storageclass"
	xhttp "storj.io/minio/cmd/http"
	"storj.io/uplink"
	"storj.io/uplink/private/multipart"
)

// ListMultipartUploads lists all multipart uploads.
func (layer *gatewayLayer) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result minio.ListMultipartsInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := validateBucket(ctx, bucket); err != nil {
		return minio.ListMultipartsInfo{}, minio.BucketNameInvalid{Bucket: bucket}
	}

	project, err := projectFromContext(ctx, bucket, "")
	if err != nil {
		return minio.ListMultipartsInfo{}, err
	}

	if delimiter != "" && delimiter != "/" {
		return minio.ListMultipartsInfo{}, minio.NotImplemented{Message: fmt.Sprintf("Unsupported delimiter: %q", delimiter)}
	}

	// TODO this should be removed and implemented on satellite side
	defer func() {
		err = checkBucketError(ctx, project, bucket, "", err)
	}()
	recursive := delimiter == ""

	list := project.ListUploads(ctx, bucket, &uplink.ListUploadsOptions{
		Prefix:    prefix,
		Cursor:    strings.TrimPrefix(keyMarker, prefix),
		Recursive: recursive,
		System:    true,
		Custom:    layer.compatibilityConfig.IncludeCustomMetadataListing,
	})

	var (
		nextKeyMarker string
		uploads       []minio.MultipartInfo
		prefixes      []string
	)

	limit := limitResults(maxUploads, layer.compatibilityConfig.MaxUploadsLimit)

	for limit > 0 && list.Next() {
		limit--
		object := list.Item()

		if object.IsPrefix {
			prefixes = append(prefixes, object.Key)
			continue
		}

		uploads = append(uploads, minioMultipartInfo(bucket, object))

		nextKeyMarker = object.Key
	}
	if list.Err() != nil {
		return result, convertMultipartError(list.Err(), bucket, "", "")
	}

	more := list.Next()
	if list.Err() != nil {
		return result, convertMultipartError(list.Err(), bucket, "", "")
	}

	if !more {
		nextKeyMarker = ""
	}

	// TODO: support NextUploadID (https://github.com/storj/gateway-mt/issues/213)
	return minio.ListMultipartsInfo{
		KeyMarker:      keyMarker,
		NextKeyMarker:  nextKeyMarker,
		UploadIDMarker: uploadIDMarker,
		MaxUploads:     maxUploads,
		IsTruncated:    nextKeyMarker != "",
		Uploads:        uploads,
		Prefix:         prefix,
		Delimiter:      delimiter,
		CommonPrefixes: prefixes,
	}, nil
}

func (layer *gatewayLayer) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := validateBucket(ctx, bucket); err != nil {
		return "", minio.BucketNameInvalid{Bucket: bucket}
	}

	if len(object) > memory.KiB.Int() { // https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
		return "", minio.ObjectNameTooLong{Bucket: bucket, Object: object}
	}

	if storageClass, ok := opts.UserDefined[xhttp.AmzStorageClass]; ok && storageClass != storageclass.STANDARD {
		return "", minio.NotImplemented{Message: "NewMultipartUpload (storage class)"}
	}

	project, err := projectFromContext(ctx, bucket, object)
	if err != nil {
		return "", err
	}

	if tagsStr, ok := opts.UserDefined[xhttp.AmzObjectTagging]; ok {
		opts.UserDefined["s3:tags"] = tagsStr
		delete(opts.UserDefined, xhttp.AmzObjectTagging)
	}

	e, err := parseTTL(opts.UserDefined)
	if err != nil {
		return "", ErrInvalidTTL
	}
	info, err := multipart.BeginUpload(ctx, project, bucket, object, &multipart.UploadOptions{
		// TODO: Truncate works around https://github.com/storj/storj-private/issues/84 until fixed on the satellite.
		Expires:        e.Truncate(time.Microsecond),
		CustomMetadata: uplink.CustomMetadata(opts.UserDefined).Clone(),
	})
	if err != nil {
		return "", convertMultipartError(err, bucket, object, "")
	}
	return info.UploadID, nil
}

func (layer *gatewayLayer) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := validateBucket(ctx, bucket); err != nil {
		return minio.PartInfo{}, minio.BucketNameInvalid{Bucket: bucket}
	}

	if partID < 1 || int64(partID) > math.MaxUint32 {
		return minio.PartInfo{}, minio.InvalidArgument{
			Bucket: bucket,
			Object: object,
			Err:    errs.New("partID is out of range."),
		}
	}

	project, err := projectFromContext(ctx, bucket, object)
	if err != nil {
		return minio.PartInfo{}, err
	}

	partUpload, err := project.UploadPart(ctx, bucket, object, uploadID, uint32(partID))
	if err != nil {
		return minio.PartInfo{}, convertMultipartError(err, bucket, object, uploadID)
	}

	_, err = sync2.Copy(ctx, partUpload, data)
	if err != nil {
		abortErr := partUpload.Abort()
		err = errs.Combine(err, abortErr)
		return minio.PartInfo{}, convertMultipartError(err, bucket, object, uploadID)
	}

	err = partUpload.SetETag([]byte(data.MD5CurrentHexString()))
	if err != nil {
		abortErr := partUpload.Abort()
		err = errs.Combine(err, abortErr)
		return minio.PartInfo{}, convertMultipartError(err, bucket, object, uploadID)
	}

	err = partUpload.Commit()
	if err != nil {
		return minio.PartInfo{}, convertMultipartError(err, bucket, object, uploadID)
	}

	part := partUpload.Info()
	return minio.PartInfo{
		PartNumber:   int(part.PartNumber),
		Size:         part.Size,
		ActualSize:   part.Size,
		ETag:         string(part.ETag),
		LastModified: part.Modified,
	}, nil
}

func (layer *gatewayLayer) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) (info minio.MultipartInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := validateBucket(ctx, bucket); err != nil {
		return minio.MultipartInfo{}, minio.BucketNameInvalid{Bucket: bucket}
	}

	if object == "" {
		return minio.MultipartInfo{}, minio.ObjectNameInvalid{}
	}

	if uploadID == "" {
		return minio.MultipartInfo{}, minio.InvalidUploadID{}
	}

	project, err := projectFromContext(ctx, bucket, object)
	if err != nil {
		return minio.MultipartInfo{}, err
	}

	info.Bucket = bucket
	info.Object = object
	info.UploadID = uploadID

	list := project.ListUploads(ctx, bucket, &uplink.ListUploadsOptions{
		Prefix: object,
		System: true,
		Custom: layer.compatibilityConfig.IncludeCustomMetadataListing,
	})

	for list.Next() {
		obj := list.Item()
		if obj.UploadID == uploadID {
			return minioMultipartInfo(bucket, obj), nil
		}
	}
	if list.Err() != nil {
		return minio.MultipartInfo{}, ConvertError(list.Err(), bucket, object)
	}
	return minio.MultipartInfo{}, minio.ObjectNotFound{Bucket: bucket, Object: object}
}

func (layer *gatewayLayer) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := validateBucket(ctx, bucket); err != nil {
		return minio.ListPartsInfo{}, minio.BucketNameInvalid{Bucket: bucket}
	}

	project, err := projectFromContext(ctx, bucket, object)
	if err != nil {
		return minio.ListPartsInfo{}, err
	}

	list := project.ListUploadParts(ctx, bucket, object, uploadID, &uplink.ListUploadPartsOptions{
		Cursor: uint32(partNumberMarker),
	})

	parts := make([]minio.PartInfo, 0, maxParts)

	limit := maxParts
	for (limit > 0 || maxParts == 0) && list.Next() {
		limit--
		part := list.Item()
		parts = append(parts, minio.PartInfo{
			PartNumber:   int(part.PartNumber),
			LastModified: part.Modified,
			ETag:         string(part.ETag), // Entity tag returned when the part was initially uploaded.
			Size:         part.Size,         // Size in bytes of the part.
			ActualSize:   part.Size,         // Decompressed Size.
		})
	}
	if list.Err() != nil {
		return result, convertMultipartError(list.Err(), bucket, object, uploadID)
	}

	more := list.Next()
	if list.Err() != nil {
		return result, convertMultipartError(list.Err(), bucket, object, uploadID)
	}
	nextPartNumberMarker := partNumberMarker
	if len(parts) > 0 {
		nextPartNumberMarker = parts[len(parts)-1].PartNumber
	}
	return minio.ListPartsInfo{
		Bucket:               bucket,
		Object:               object,
		UploadID:             uploadID,
		StorageClass:         storageclass.STANDARD,
		PartNumberMarker:     partNumberMarker,     // Part number after which listing begins.
		NextPartNumberMarker: nextPartNumberMarker, // NextPartNum is really more like last part num.
		MaxParts:             maxParts,
		IsTruncated:          more,
		Parts:                parts,
		// also available: UserDefined map[string]string
	}, nil
}

func (layer *gatewayLayer) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, _ minio.ObjectOptions) (err error) {
	defer mon.Task()(&ctx)(&err)

	if err := validateBucket(ctx, bucket); err != nil {
		return minio.BucketNameInvalid{Bucket: bucket}
	}

	project, err := projectFromContext(ctx, bucket, object)
	if err != nil {
		return err
	}

	err = project.AbortUpload(ctx, bucket, object, uploadID)
	if err != nil {
		// NOTE: It's not clear whether AbortMultipartUpload should return a 404
		// for objects not found. MinIO tests only cover "bucket not found" and
		// "invalid id".
		if errors.Is(err, uplink.ErrObjectNotFound) {
			return nil
		}
		return convertMultipartError(err, bucket, object, uploadID)
	}
	return nil
}

func (layer *gatewayLayer) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := validateBucket(ctx, bucket); err != nil {
		return minio.ObjectInfo{}, minio.BucketNameInvalid{Bucket: bucket}
	}

	project, err := projectFromContext(ctx, bucket, object)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	var idx int
	list := project.ListUploadParts(ctx, bucket, object, uploadID, nil)
	for ; list.Next(); idx++ {
		part := list.Item()
		// Are we listing past what we received?
		if idx >= len(uploadedParts) {
			return minio.ObjectInfo{}, minio.InvalidPart{
				PartNumber: int(part.PartNumber),
				ExpETag:    string(part.ETag),
				GotETag:    "",
			}
		}
		// Is size okay for everything except the last part?
		if idx != len(uploadedParts)-1 {
			if part.Size < layer.compatibilityConfig.MinPartSize {
				return minio.ObjectInfo{}, minio.PartTooSmall{
					PartSize:   part.Size,
					PartNumber: int(part.PartNumber),
					PartETag:   string(part.ETag),
				}
			}
		}
		// Do we agree on the part number?
		if uploadedParts[idx].PartNumber != int(part.PartNumber) {
			return minio.ObjectInfo{}, minio.InvalidPart{
				PartNumber: uploadedParts[idx].PartNumber,
				ExpETag:    "",
				GotETag:    uploadedParts[idx].ETag,
			}
		}
		// Do we agree on ETag?
		if uploadedParts[idx].ETag != string(part.ETag) {
			return minio.ObjectInfo{}, minio.InvalidPart{
				PartNumber: int(part.PartNumber),
				ExpETag:    string(part.ETag),
				GotETag:    uploadedParts[idx].ETag,
			}
		}
	}
	if list.Err() != nil {
		return minio.ObjectInfo{}, convertMultipartError(list.Err(), bucket, object, uploadID)
	}

	if len(uploadedParts) > idx { // We didn't list enough
		return minio.ObjectInfo{}, minio.InvalidPart{
			PartNumber: uploadedParts[idx].PartNumber, // Condition guarantees safe access
			ExpETag:    "",                            // We expected nothing
			GotETag:    uploadedParts[idx].ETag,
		}
	}

	etag := minio.ComputeCompleteMultipartMD5(uploadedParts)

	if tagsStr, ok := opts.UserDefined[xhttp.AmzObjectTagging]; ok {
		opts.UserDefined["s3:tags"] = tagsStr
		delete(opts.UserDefined, xhttp.AmzObjectTagging)
	}

	metadata := uplink.CustomMetadata{}
	// TODO we can think about batching this request with ListUploadParts
	uploads := project.ListUploads(ctx, bucket, &uplink.ListUploadsOptions{
		Prefix: object,
		Custom: true,
	})
	for uploads.Next() {
		upload := uploads.Item()
		// TODO should we error if we didn't find corresponding upload
		if upload.UploadID == uploadID {
			metadata = upload.Custom
		}
	}
	if err := uploads.Err(); err != nil {
		return minio.ObjectInfo{}, convertMultipartError(err, bucket, object, uploadID)
	}

	metadata = metadata.Clone()
	metadata["s3:etag"] = etag

	obj, err := project.CommitUpload(ctx, bucket, object, uploadID, &uplink.CommitUploadOptions{
		CustomMetadata: metadata,
	})
	if err != nil {
		return minio.ObjectInfo{}, convertMultipartError(err, bucket, object, uploadID)
	}

	return minioObjectInfo(bucket, etag, obj), nil
}

func minioMultipartInfo(bucket string, object *uplink.UploadInfo) minio.MultipartInfo {
	if object == nil {
		object = &uplink.UploadInfo{}
	}

	return minio.MultipartInfo{
		Bucket:      bucket,
		Object:      object.Key,
		Initiated:   object.System.Created,
		UploadID:    object.UploadID,
		UserDefined: object.Custom,
	}
}

func convertMultipartError(err error, bucket, object, uploadID string) error {
	if errors.Is(err, uplink.ErrUploadIDInvalid) {
		return minio.InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
	}

	return ConvertError(err, bucket, object)
}
