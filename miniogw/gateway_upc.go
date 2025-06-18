// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"

	"github.com/zeebo/errs"

	"storj.io/common/memory"
	"storj.io/common/sync2"
	minio "storj.io/minio/cmd"
	"storj.io/uplink"
	versioned "storj.io/uplink/private/object"
)

func (layer *gatewayLayer) CopyObjectPart(
	ctx context.Context,
	srcBucket, srcObject, destBucket, destObject, uploadID string,
	partID int,
	startOffset, length int64,
	srcInfo minio.ObjectInfo,
	srcOpts, dstOpts minio.ObjectOptions,
) (info minio.PartInfo, err error) {
	// input validation

	if layer.compatibilityConfig.DisableUploadPartCopy {
		return minio.PartInfo{}, minio.NotImplemented{Message: "UploadPartCopy"}
	}

	if err = ValidateBucket(ctx, srcBucket); err != nil {
		return minio.PartInfo{}, minio.BucketNameInvalid{
			Bucket:    srcBucket,
			Object:    srcObject,
			VersionID: srcOpts.VersionID,
		}
	}
	if err = ValidateBucket(ctx, destBucket); err != nil {
		return minio.PartInfo{}, minio.BucketNameInvalid{
			Bucket:    destBucket,
			Object:    destObject,
			VersionID: dstOpts.VersionID,
		}
	}

	if l := len(srcObject); l == 0 {
		return minio.PartInfo{}, minio.ObjectNameInvalid{
			Bucket:    srcBucket,
			Object:    srcObject,
			VersionID: srcOpts.VersionID,
		}
	} else if l > memory.KiB.Int() {
		return minio.PartInfo{}, minio.ObjectNameTooLong{
			Bucket:    srcBucket,
			Object:    srcObject,
			VersionID: srcOpts.VersionID,
		}
	}
	if l := len(destObject); l == 0 {
		return minio.PartInfo{}, minio.ObjectNameInvalid{
			Bucket:    destBucket,
			Object:    destObject,
			VersionID: dstOpts.VersionID,
		}
	} else if l > memory.KiB.Int() {
		return minio.PartInfo{}, minio.ObjectNameTooLong{
			Bucket:    destBucket,
			Object:    destObject,
			VersionID: dstOpts.VersionID,
		}
	}

	srcVersion, err := decodeVersionID(srcInfo.VersionID)
	if err != nil {
		return minio.PartInfo{}, ConvertError(err, srcBucket, srcObject)
	}

	if partID < 1 || partID > 10000 {
		return minio.PartInfo{}, minio.InvalidArgument{
			Bucket:    destBucket,
			Object:    destObject,
			VersionID: dstOpts.VersionID,
			Err:       errs.New("part number must be an integer between 1 and 10000, inclusive"),
		}
	}

	// work

	project, err := projectFromContext(ctx, srcBucket, srcObject)
	if err != nil {
		return minio.PartInfo{}, ConvertError(err, srcBucket, srcObject)
	}

	download, err := versioned.DownloadObject(ctx, project, srcBucket, srcObject, srcVersion, &uplink.DownloadOptions{
		Offset: startOffset,
		Length: length,
	})
	if err != nil {
		return minio.PartInfo{}, ConvertError(err, srcBucket, srcObject)
	}

	defer func() {
		err = errs.Combine(err, ConvertError(download.Close(), srcBucket, srcObject))
	}()

	sum := md5.New()
	src := io.TeeReader(download, sum)

	dst, err := project.UploadPart(ctx, destBucket, destObject, uploadID, uint32(partID))
	if err != nil {
		return minio.PartInfo{}, convertMultipartError(err, destBucket, destObject, uploadID)
	}

	if _, err = sync2.Copy(ctx, dst, src); err != nil {
		err = errs.Combine(err, dst.Abort())
		return minio.PartInfo{}, convertMultipartError(err, destBucket, destObject, uploadID)
	}

	etag := make([]byte, hex.EncodedLen(sum.Size()))
	hex.Encode(etag, sum.Sum(nil))

	if err = dst.SetETag(etag); err != nil {
		err = errs.Combine(err, dst.Abort())
		return minio.PartInfo{}, convertMultipartError(err, destBucket, destObject, uploadID)
	}

	if err = dst.Commit(); err != nil {
		err = errs.Combine(err, dst.Abort())
		return minio.PartInfo{}, convertMultipartError(err, destBucket, destObject, uploadID)
	}

	partInfo := dst.Info()
	return minio.PartInfo{
		PartNumber:   int(partInfo.PartNumber),
		LastModified: partInfo.Modified,
		ETag:         string(partInfo.ETag),
		Size:         partInfo.Size,
		ActualSize:   partInfo.Size,
	}, nil
}
