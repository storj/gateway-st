// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/zeebo/errs"

	"storj.io/common/memory"
	"storj.io/common/sync2"
	"storj.io/common/uuid"
	"storj.io/eventkit"
	minio "storj.io/minio/cmd"
	"storj.io/uplink"
	"storj.io/uplink/private/bucket"
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

	if !layer.compatibilityConfig.UploadPartCopy.Enable {
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
			Bucket: destBucket,
			Object: destObject,
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
			Bucket: destBucket,
			Object: destObject,
		}
	} else if l > memory.KiB.Int() {
		return minio.PartInfo{}, minio.ObjectNameTooLong{
			Bucket: destBucket,
			Object: destObject,
		}
	}

	srcVersion, err := decodeVersionID(srcOpts.VersionID)
	if err != nil {
		return minio.PartInfo{}, ConvertError(err, srcBucket, srcObject)
	}

	if partID < 1 || partID > 10000 {
		return minio.PartInfo{}, minio.InvalidArgument{
			Bucket: destBucket,
			Object: destObject,
			Err:    errs.New("part number must be an integer between 1 and 10000, inclusive"),
		}
	}

	// enablement validation

	credentials, err := credentialsFromContext(ctx)
	if err != nil {
		return minio.PartInfo{}, ConvertError(err, srcBucket, srcObject)
	}

	project := credentials.Project

	// telemetry

	enabled, now := false, time.Now()
	defer func() {
		tags := ctxCredentialsToTags(ctx)
		tags = append(tags, eventkit.Bool("enabled", enabled))
		tags = append(tags, eventkit.Duration("duration", time.Since(now)))
		tags = append(tags, eventkit.String("source bucket", srcBucket))
		tags = append(tags, eventkit.String("source object", hex.EncodeToString([]byte(srcObject))))
		tags = append(tags, eventkit.String("destination bucket", destBucket))
		tags = append(tags, eventkit.String("destination object", hex.EncodeToString([]byte(destObject))))
		tags = append(tags, eventkit.String("upload ID", uploadID))
		tags = append(tags, eventkit.Int64("part number", int64(partID)))
		tags = append(tags, eventkit.Int64("start offset", startOffset))
		tags = append(tags, eventkit.Int64("length", length))
		ek.Event("UploadPartCopy_naive", tags...)
	}()

	enabled, err = layer.copyObjectPartConfig.enabled(ctx, project, credentials.PublicProjectID, srcBucket, destBucket)
	if err != nil {
		return minio.PartInfo{}, ConvertError(err, srcBucket, srcObject)
	} else if !enabled {
		return minio.PartInfo{}, minio.NotImplemented{Message: "UploadPartCopy: not enabled for this project or location"}
	}

	// work

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

type copyObjectPartConfig map[string][]string

func (config copyObjectPartConfig) enabled(
	ctx context.Context,
	project *uplink.Project,
	projectID, srcBucket, destBucket string,
) (bool, error) {
	projectID, err := sanitizeUUID(projectID)
	if err != nil {
		return false, errs.New("invalid project ID: %w", err)
	}

	allowedLocations := slices.Concat(config[projectID], config["*"])

	if len(allowedLocations) == 0 {
		return false, nil
	}

	if slices.Contains(allowedLocations, "*") {
		return true, nil
	}

	srcLocation, err := bucket.GetBucketLocation(ctx, project, srcBucket)
	if err != nil {
		return false, errs.New("failed to get source bucket location: %w", err)
	}
	destLocation, err := bucket.GetBucketLocation(ctx, project, destBucket)
	if err != nil {
		return false, errs.New("failed to get destination bucket location: %w", err)
	}

	return srcLocation == destLocation && slices.Contains(allowedLocations, srcLocation), nil
}

func newCopyObjectPartConfig(rawConfig []string) (copyObjectPartConfig, error) {
	config := make(copyObjectPartConfig)

	for _, raw := range rawConfig {
		before, after, found := strings.Cut(raw, ":")
		if !found {
			return nil, errs.New("%s does not contain a colon", raw)
		}

		if before == "" || after == "" {
			return nil, errs.New("%s contains an empty part", raw)
		}

		var projectID string

		if before == "*" {
			projectID = "*"
		} else {
			p, err := sanitizeUUID(before)
			if err != nil {
				return nil, errs.New("%s does not contain a valid project ID: %w", raw, err)
			}
			projectID = p
		}

		config[projectID] = append(config[projectID], after)
	}

	return config, nil
}

func sanitizeUUID(s string) (string, error) {
	ret, err := uuid.FromString(s)
	if err != nil {
		return "", err
	}
	return ret.String(), nil
}
