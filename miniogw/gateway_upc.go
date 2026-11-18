// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/zeebo/errs"

	"storj.io/common/memory"
	"storj.io/common/sync2"
	"storj.io/common/uuid"
	"storj.io/eventkit"
	"storj.io/gateway/api/objectlayer"
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
	srcInfo objectlayer.ObjectInfo,
	srcOpts, dstOpts objectlayer.ObjectOptions,
) (info objectlayer.PartInfo, err error) {
	// input validation

	if !layer.compatibilityConfig.UploadPartCopy.Enable {
		return objectlayer.PartInfo{}, minio.NotImplemented{Message: "UploadPartCopy"}
	}

	if err = ValidateBucket(ctx, srcBucket); err != nil {
		return objectlayer.PartInfo{}, minio.BucketNameInvalid{
			Bucket:    srcBucket,
			Object:    srcObject,
			VersionID: srcOpts.VersionID,
		}
	}
	if err = ValidateBucket(ctx, destBucket); err != nil {
		return objectlayer.PartInfo{}, minio.BucketNameInvalid{
			Bucket: destBucket,
			Object: destObject,
		}
	}

	if l := len(srcObject); l == 0 {
		return objectlayer.PartInfo{}, minio.ObjectNameInvalid{
			Bucket:    srcBucket,
			Object:    srcObject,
			VersionID: srcOpts.VersionID,
		}
	} else if l > memory.KiB.Int() {
		return objectlayer.PartInfo{}, minio.ObjectNameTooLong{
			Bucket:    srcBucket,
			Object:    srcObject,
			VersionID: srcOpts.VersionID,
		}
	}
	if l := len(destObject); l == 0 {
		return objectlayer.PartInfo{}, minio.ObjectNameInvalid{
			Bucket: destBucket,
			Object: destObject,
		}
	} else if l > memory.KiB.Int() {
		return objectlayer.PartInfo{}, minio.ObjectNameTooLong{
			Bucket: destBucket,
			Object: destObject,
		}
	}

	srcVersion, err := decodeVersionID(srcOpts.VersionID)
	if err != nil {
		return objectlayer.PartInfo{}, ConvertError(err, srcBucket, srcObject)
	}

	if partID < 1 || partID > 10000 {
		return objectlayer.PartInfo{}, minio.InvalidArgument{
			Bucket: destBucket,
			Object: destObject,
			Err:    errs.New("part number must be an integer between 1 and 10000, inclusive"),
		}
	}

	// enablement validation

	credentials, err := credentialsFromContext(ctx)
	if err != nil {
		return objectlayer.PartInfo{}, ConvertError(err, srcBucket, srcObject)
	}

	project := credentials.Project

	// telemetry

	enabled, now := false, time.Now()
	defer func() {
		tags := ctxCredentialsToTags(ctx)
		tags = append(tags, eventkit.Bool("enabled", enabled))
		tags = append(tags, eventkit.Duration("duration", time.Since(now)))
		tags = append(tags, eventkit.String("source_bucket", srcBucket))
		tags = append(tags, eventkit.String("source_path_hash", sha256HexString(srcObject)))
		tags = append(tags, eventkit.String("destination_bucket", destBucket))
		tags = append(tags, eventkit.String("destination_path_hash", sha256HexString(destObject)))
		tags = append(tags, eventkit.String("upload_id", uploadID))
		tags = append(tags, eventkit.Int64("part_number", int64(partID)))
		tags = append(tags, eventkit.Int64("start_offset", startOffset))
		tags = append(tags, eventkit.Int64("length", length))
		ek.Event("UploadPartCopy_naive", tags...)
	}()

	enabled, err = layer.copyObjectPartConfig.enabled(ctx, project, credentials.PublicProjectID, srcBucket, destBucket)
	if err != nil {
		return objectlayer.PartInfo{}, ConvertError(err, srcBucket, srcObject)
	} else if !enabled {
		return objectlayer.PartInfo{}, minio.NotImplemented{Message: "UploadPartCopy: not enabled for this project or location"}
	}

	// work

	id, err := checkUploadID(ctx, destBucket, destObject, uploadID)
	if err != nil {
		return objectlayer.PartInfo{}, err
	}

	download, err := versioned.DownloadObject(ctx, project, srcBucket, srcObject, srcVersion, &versioned.DownloadObjectOptions{
		Offset:         startOffset,
		Length:         length,
		ServerSideCopy: layer.compatibilityConfig.UploadPartCopy.DiscountEgress,
	})
	if err != nil {
		return objectlayer.PartInfo{}, ConvertError(err, srcBucket, srcObject)
	}

	defer func() {
		err = errs.Combine(err, ConvertError(download.Close(), srcBucket, srcObject))
	}()

	if size := download.Info().System.ContentLength; startOffset >= size || startOffset+length > size {
		return objectlayer.PartInfo{}, miniogo.ErrorResponse{
			Code:       "InvalidRange",
			Message:    "The requested range cannot be satisfied.",
			StatusCode: http.StatusRequestedRangeNotSatisfiable,
		}
	}

	upload, err := layer.uploadChecksum(id, destBucket, destObject, uploadID)
	if err != nil {
		return objectlayer.PartInfo{}, err
	}

	sum := md5.New()
	checksum := objectlayer.NewCopyChecksum(io.TeeReader(download, sum), upload.Algorithm)

	dst, err := versioned.UploadPart(ctx, project, destBucket, destObject, uploadID, uint32(partID))
	if err != nil {
		return objectlayer.PartInfo{}, convertMultipartError(err, destBucket, destObject, uploadID)
	}

	if _, err = sync2.Copy(ctx, dst, checksum.Reader()); err != nil {
		err = errs.Combine(err, dst.Abort())
		return objectlayer.PartInfo{}, convertMultipartError(err, destBucket, destObject, uploadID)
	}

	partChecksum, err := checksum.Checksum()
	if err != nil {
		err = errs.Combine(err, dst.Abort())
		return objectlayer.PartInfo{}, convertMultipartError(err, destBucket, destObject, uploadID)
	}
	if partChecksum.Value != "" {
		if err = dst.SetChecksum([]byte(partChecksum.Value)); err != nil {
			err = errs.Combine(err, dst.Abort())
			return objectlayer.PartInfo{}, convertMultipartError(err, destBucket, destObject, uploadID)
		}
	}

	etag := make([]byte, hex.EncodedLen(sum.Size()))
	hex.Encode(etag, sum.Sum(nil))

	if err = dst.SetETag(etag); err != nil {
		err = errs.Combine(err, dst.Abort())
		return objectlayer.PartInfo{}, convertMultipartError(err, destBucket, destObject, uploadID)
	}

	if err = dst.Commit(); err != nil {
		err = errs.Combine(err, dst.Abort())
		return objectlayer.PartInfo{}, convertMultipartError(err, destBucket, destObject, uploadID)
	}

	partInfo := dst.Info()
	partChecksum.Default = upload.Default
	return objectlayer.PartInfo{
		PartNumber: int(partInfo.PartNumber),
		// uplink doesn't report when the satellite committed the part, which was just now.
		//
		// TODO: this can differ from the time ListParts reports by the commit's round trip and
		// any clock skew. Reading the part back would fix that but costs another satellite
		// request, which we don't want. Better would be uplink returning the commit time.
		LastModified: time.Now(),
		ETag:         string(partInfo.ETag),
		Size:         partInfo.Size,
		ActualSize:   partInfo.Size,
		Checksum:     partChecksum,
	}, nil
}

type copyObjectPartConfig map[string][]string

func (config copyObjectPartConfig) enabled(
	ctx context.Context,
	project *uplink.Project,
	projectID, srcBucket, destBucket string,
) (bool, error) {
	allowedLocations := slices.Clone(config["*"])

	if projectID, err := sanitizeUUID(projectID); err == nil {
		allowedLocations = append(allowedLocations, config[projectID]...)
	}

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

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func sha256HexString(s string) string {
	return sha256Hex([]byte(s))
}
