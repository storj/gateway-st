// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"context"
	"crypto/md5" /* #nosec G501 */ // Is only used for calculating a hash of the ETags of the all the parts of a multipart upload.
	"encoding/hex"
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"

	minio "github.com/minio/minio/cmd"
	"github.com/zeebo/errs"

	"storj.io/common/sync2"
	"storj.io/uplink"
)

func (layer *gatewayLayer) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	defer mon.Task()(&ctx)(&err)

	info, err := layer.project.BeginUpload(ctx, bucket, object, nil)
	if err != nil {
		return "", convertMultipartError(err, bucket, object, "")
	}
	return info.UploadID, nil
}

func (layer *gatewayLayer) GetMultipartInfo(ctx context.Context, bucket string, object string, uploadID string, opts minio.ObjectOptions) (info minio.MultipartInfo, err error) {
	if bucket == "" {
		return minio.MultipartInfo{}, minio.BucketNameInvalid{}
	}

	if object == "" {
		return minio.MultipartInfo{}, minio.ObjectNameInvalid{}
	}

	if uploadID == "" {
		return minio.MultipartInfo{}, minio.InvalidUploadID{}
	}

	info.Bucket = bucket
	info.Object = object
	info.UploadID = uploadID

	list := layer.project.ListUploads(ctx, bucket, &uplink.ListUploadsOptions{
		Prefix: object,
		System: true,
		Custom: true,
	})

	for list.Next() {
		obj := list.Item()
		if obj.UploadID == uploadID {
			return minioMultipartInfo(bucket, obj), nil
		}
	}
	if list.Err() != nil {
		return minio.MultipartInfo{}, convertError(list.Err(), bucket, object)
	}
	return minio.MultipartInfo{}, minio.ObjectNotFound{Bucket: bucket, Object: object}
}

func (layer *gatewayLayer) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if partID < 1 || int64(partID) > math.MaxUint32 {
		return minio.PartInfo{}, errs.New("partID is out of range.")
	}

	partUpload, err := layer.project.UploadPart(ctx, bucket, object, uploadID, uint32(partID-1))
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
		PartNumber:   int(part.PartNumber + 1),
		Size:         part.Size,
		ActualSize:   part.Size,
		ETag:         string(part.ETag),
		LastModified: part.Modified,
	}, nil
}

func (layer *gatewayLayer) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, _ minio.ObjectOptions) (err error) {
	defer mon.Task()(&ctx)(&err)
	err = layer.project.AbortUpload(ctx, bucket, object, uploadID)
	if err != nil {
		return convertMultipartError(err, bucket, object, uploadID)
	}
	return nil
}

func (layer *gatewayLayer) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	// TODO: Check that ETag of uploadedParts match the ETags stored in metabase.

	etag, err := multipartUploadETag(uploadedParts)
	if err != nil {
		return minio.ObjectInfo{}, convertMultipartError(err, bucket, object, uploadID)
	}

	metadata := uplink.CustomMetadata(opts.UserDefined).Clone()
	metadata["s3:etag"] = etag

	obj, err := layer.project.CommitUpload(ctx, bucket, object, uploadID, &uplink.CommitUploadOptions{
		CustomMetadata: metadata,
	})
	if err != nil {
		return minio.ObjectInfo{}, convertMultipartError(err, bucket, object, uploadID)
	}

	return minioObjectInfo(bucket, etag, obj), nil
}

func (layer *gatewayLayer) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	list := layer.project.ListUploadParts(ctx, bucket, object, uploadID, &uplink.ListUploadPartsOptions{
		Cursor: uint32(partNumberMarker - 1),
	})

	parts := make([]minio.PartInfo, 0, maxParts)

	limit := maxParts
	for (limit > 0 || maxParts == 0) && list.Next() {
		limit--
		part := list.Item()
		parts = append(parts, minio.PartInfo{
			PartNumber:   int(part.PartNumber + 1),
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

	sort.Slice(parts, func(i, k int) bool {
		return parts[i].PartNumber < parts[k].PartNumber
	})
	return minio.ListPartsInfo{
		Bucket:               bucket,
		Object:               object,
		UploadID:             uploadID,
		StorageClass:         "",               // TODO
		PartNumberMarker:     partNumberMarker, // Part number after which listing begins.
		NextPartNumberMarker: partNumberMarker, // TODO Next part number marker to be used if list is truncated
		MaxParts:             maxParts,
		IsTruncated:          more,
		Parts:                parts,
		// also available: UserDefined map[string]string
	}, nil
}

// ListMultipartUploads lists all multipart uploads.
func (layer *gatewayLayer) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (result minio.ListMultipartsInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	// TODO maybe this should be checked by project.ListMultipartUploads
	if bucket == "" {
		return minio.ListMultipartsInfo{}, minio.BucketNameInvalid{}
	}

	if delimiter != "" && delimiter != "/" {
		return minio.ListMultipartsInfo{}, minio.UnsupportedDelimiter{Delimiter: delimiter}
	}

	// TODO this should be removed and implemented on satellite side
	defer func() {
		err = checkBucketError(ctx, layer.project, bucket, "", err)
	}()

	recursive := delimiter == ""

	list := layer.project.ListUploads(ctx, bucket, &uplink.ListUploadsOptions{
		Prefix:    prefix,
		Cursor:    keyMarker,
		Recursive: recursive,

		System: true,
		Custom: true,
	})
	startAfter := keyMarker
	var uploads []minio.MultipartInfo
	var prefixes []string

	limit := maxUploads
	for (limit > 0 || maxUploads == 0) && list.Next() {
		limit--
		upload := list.Item()
		if upload.IsPrefix {
			prefixes = append(prefixes, upload.Key)
			continue
		}

		uploads = append(uploads, minioMultipartInfo(bucket, upload))

		startAfter = upload.Key

	}
	if list.Err() != nil {
		return result, convertMultipartError(list.Err(), bucket, "", "")
	}

	more := list.Next()
	if list.Err() != nil {
		return result, convertMultipartError(list.Err(), bucket, "", "")
	}

	result = minio.ListMultipartsInfo{
		KeyMarker:      keyMarker,
		UploadIDMarker: uploadIDMarker,
		MaxUploads:     maxUploads,
		IsTruncated:    more,
		Uploads:        uploads,
		Prefix:         prefix,
		Delimiter:      delimiter,
		CommonPrefixes: prefixes,
	}
	if more {
		result.NextKeyMarker = startAfter
		// TODO: NextUploadID
	}

	return result, nil
}

func minioMultipartInfo(bucket string, upload *uplink.UploadInfo) minio.MultipartInfo {
	if upload == nil {
		return minio.MultipartInfo{}
	}

	return minio.MultipartInfo{
		Bucket:    bucket,
		Object:    upload.Key,
		Initiated: upload.System.Created,
		UploadID:  upload.UploadID,
	}
}

func multipartUploadETag(parts []minio.CompletePart) (string, error) {
	var hashes []byte
	for _, part := range parts {
		md5, err := hex.DecodeString(canonicalEtag(part.ETag))
		if err != nil {
			hashes = append(hashes, []byte(part.ETag)...)
		} else {
			hashes = append(hashes, md5...)
		}
	}

	/* #nosec G401 */ // ETags aren't security sensitive
	sum := md5.Sum(hashes)
	return hex.EncodeToString(sum[:]) + "-" + strconv.Itoa(len(parts)), nil
}

func canonicalEtag(etag string) string {
	etag = strings.Trim(etag, `"`)
	p := strings.IndexByte(etag, '-')
	if p >= 0 {
		return etag[:p]
	}
	return etag
}

func convertMultipartError(err error, bucket, object, uploadID string) error {
	if errors.Is(err, uplink.ErrUploadIDInvalid) {
		return minio.InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
	}

	return convertError(err, bucket, object)
}
