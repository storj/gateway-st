// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"syscall"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/common/memory"
	"storj.io/common/storj"
	"storj.io/common/sync2"
	"storj.io/common/version"
	minio "storj.io/minio/cmd"
	"storj.io/minio/cmd/config/storageclass"
	xhttp "storj.io/minio/cmd/http"
	"storj.io/minio/pkg/auth"
	objectlock "storj.io/minio/pkg/bucket/object/lock"
	"storj.io/minio/pkg/bucket/versioning"
	"storj.io/minio/pkg/hash"
	"storj.io/minio/pkg/madmin"
	"storj.io/uplink"
	"storj.io/uplink/private/bucket"
	"storj.io/uplink/private/metaclient"
	versioned "storj.io/uplink/private/object"
	privateProject "storj.io/uplink/private/project"
)

var (
	mon = monkit.Package()

	// ErrInternalError is a generic error response for internal errors.
	ErrInternalError = miniogo.ErrorResponse{
		Code:       "InternalError",
		StatusCode: http.StatusInternalServerError,
		Message:    "An internal error occurred.",
	}

	// ErrAccessDenied indicates that a user is not allowed to perform the requested operation.
	ErrAccessDenied = miniogo.ErrorResponse{
		Code:       "AccessDenied",
		StatusCode: http.StatusForbidden,
		Message:    "Access Denied.",
	}

	// ErrBandwidthLimitExceeded is a custom error for when a user has reached their
	// Satellite bandwidth limit.
	//
	// Note: we went with 403 Forbidden over 509 Bandwidth Limit Exceeded, as 509
	// is not an official status code and rarely used outside of Apache/cPanel.
	// In this case though the user can solve it themselves by getting a
	// limit increase, so it should be treated as a 4xx level error, not 5xx.
	ErrBandwidthLimitExceeded = miniogo.ErrorResponse{
		Code:       "BandwidthLimitExceeded",
		StatusCode: http.StatusForbidden,
		Message:    "You have reached your project bandwidth limit.",
	}

	// ErrStorageLimitExceeded is a custom error for when a user has reached their
	// Satellite storage limit.
	//
	// Note: we went with 403 Forbidden over 507 Insufficient Storage, as 507
	// doesn't fit this case exactly. 507 is for when the server cannot
	// physically store any further data, e.g. the disk has filled up. In this
	// case though the user can solve this themselves by upgrading their plan,
	// so it should be treated as a 4xx level error, not 5xx.
	ErrStorageLimitExceeded = miniogo.ErrorResponse{
		Code:       "StorageLimitExceeded",
		StatusCode: http.StatusForbidden,
		Message:    "You have reached your project storage limit.",
	}

	// ErrSegmentsLimitExceeded is a custom error for when a user has reached their
	// Satellite segment limit.
	ErrSegmentsLimitExceeded = miniogo.ErrorResponse{
		Code:       "SegmentsLimitExceeded",
		StatusCode: http.StatusForbidden,
		Message:    "You have reached your project segment limit.",
	}

	// ErrInvalidTTL indicates that the value under
	// X-Amz-Meta-Object-Expires/X-Minio-Meta-Object-Expires couldn't be parsed.
	ErrInvalidTTL = miniogo.ErrorResponse{
		Code:       "InvalidTTL",
		Message:    "The TTL you have specified is invalid.",
		StatusCode: http.StatusBadRequest,
	}

	// objectTTLKeyAliases is the list of all supported object TTL keys in custom metadata.
	objectTTLKeyAliases = []string{
		"X-Amz-Meta-Object-Expires",
		"X-Minio-Meta-Object-Expires",
		"X-Amz-Meta-Storj-Expires",
		"X-Minio-Meta-Storj-Expires",
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

	// ErrBucketObjectLockNotEnabled is a custom error for when a user is trying to perform OL operation for a bucket
	// with no OL enabled.
	ErrBucketObjectLockNotEnabled = miniogo.ErrorResponse{
		Code:       "InvalidRequest",
		StatusCode: http.StatusBadRequest,
		Message:    "Bucket is missing Object Lock Configuration",
	}

	// ErrBucketInvalidObjectLockConfig is a custom error for when a user attempts to set
	// an invalid Object Lock configuration on a bucket.
	ErrBucketInvalidObjectLockConfig = miniogo.ErrorResponse{
		Code:       "InvalidArgument",
		StatusCode: http.StatusBadRequest,
		Message:    "Bucket Object Lock configuration is invalid",
	}

	// ErrBucketInvalidStateObjectLock is a custom error for when a user attempts to upload an object with retention
	// configuration but the bucket does not have versioning enabled. It's also for the case of suspending versioning
	// on a bucket when object lock is enabled.
	ErrBucketInvalidStateObjectLock = miniogo.ErrorResponse{
		Code:       "InvalidBucketState",
		StatusCode: http.StatusConflict,
		Message:    "Object lock requires bucket versioning to be enabled",
	}

	// ErrRetentionNotFound is a custom error returned when attempting to get retention config for an object
	// that doesn't have any.
	ErrRetentionNotFound = miniogo.ErrorResponse{
		Code:       "NoSuchObjectLockConfiguration",
		StatusCode: http.StatusNotFound,
		Message:    "Object is missing retention configuration",
	}

	// ErrObjectProtected is a custom error returned when attempting to make any action with an object protected by
	// object lock configuration.
	ErrObjectProtected = miniogo.ErrorResponse{
		Code:       "AccessDenied",
		StatusCode: http.StatusForbidden,
		Message:    "Access Denied because object protected by object lock",
	}

	// ErrNoUplinkProject is a custom error that indicates there was no
	// `*uplink.Project` in the context for the gateway to pick up. This error
	// may signal that passing credentials down to the object layer is working
	// incorrectly.
	ErrNoUplinkProject = errs.Class("uplink project")

	// ErrTooManyItemsToList indicates that ListObjects/ListObjectsV2 failed
	// because of too many items to list for gateway-side filtering using an
	// arbitrary delimiter and/or prefix.
	ErrTooManyItemsToList = minio.NotImplemented{
		Message: "ListObjects(V2): listing too many items for gateway-side filtering using arbitrary delimiter/prefix",
	}

	// ErrVersionIDMarkerWithoutKeyMarker is returned for
	// ListObjectVersions when a version-id marker has been specified
	// without a key marker.
	ErrVersionIDMarkerWithoutKeyMarker = func(bucketName string) miniogo.ErrorResponse {
		return miniogo.ErrorResponse{
			Code:       "InvalidArgument",
			Message:    "A version-id marker cannot be specified without a key marker.",
			BucketName: bucketName,
			StatusCode: http.StatusBadRequest,
		}
	}

	// ErrObjectKeyMissing is returned by DeleteObjects when an object key is missing.
	ErrObjectKeyMissing = miniogo.ErrorResponse{
		Code:       "UserKeyMustBeSpecified",
		Message:    "An object key was not provided.",
		StatusCode: http.StatusBadRequest,
	}

	// ErrObjectKeyTooLong is returned by DeleteObjects when the length of an object key exceeds the limit.
	ErrObjectKeyTooLong = miniogo.ErrorResponse{
		Code:       "KeyTooLongError",
		Message:    "A provided object key is too long.",
		StatusCode: http.StatusBadRequest,
	}

	// ErrObjectVersionInvalid is returned by DeleteObjects when an object version is malformed.
	ErrObjectVersionInvalid = miniogo.ErrorResponse{
		Code:       "InvalidVersion",
		Message:    "A provided object version ID is invalid.",
		StatusCode: http.StatusBadRequest,
	}

	// ErrDeleteObjectsNoItems is returned by DeleteObjects when no objects are specified for deletion.
	ErrDeleteObjectsNoItems = miniogo.ErrorResponse{
		Code:       "MalformedXML",
		Message:    "The list of objects must contain at least one item.",
		StatusCode: http.StatusBadRequest,
	}

	// ErrDeleteObjectsTooManyItems is returned by DeleteObjects when too many objects are specified for deletion.
	ErrDeleteObjectsTooManyItems = miniogo.ErrorResponse{
		Code:       "MalformedXML",
		Message:    "The list of objects contains too many items.",
		StatusCode: http.StatusBadRequest,
	}

	// ErrFailedPrecondition is returned when a precondition is not met, such as If-None-Match for conditional writes.
	ErrFailedPrecondition = miniogo.ErrorResponse{
		Code:       "PreconditionFailed",
		Message:    "At least one of the pre-conditions you specified did not hold",
		StatusCode: http.StatusPreconditionFailed,
	}
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

// Shutdown is a no-op.
func (layer *gatewayLayer) Shutdown(ctx context.Context) (err error) {
	return nil
}

func (layer *gatewayLayer) StorageInfo(ctx context.Context) (minio.StorageInfo, []error) {
	return minio.StorageInfo{
		Backend: madmin.BackendInfo{
			Type:          madmin.Gateway,
			GatewayOnline: true,
		},
	}, nil
}

func (layer *gatewayLayer) MakeBucketWithLocation(ctx context.Context, name string, opts minio.BucketOptions) (err error) {
	defer mon.Task()(&ctx)(&err)

	if err := ValidateBucket(ctx, name); err != nil {
		return minio.BucketNameInvalid{Bucket: name}
	}

	project, err := projectFromContext(ctx, name, "")
	if err != nil {
		return err
	}

	_, err = bucket.CreateBucketWithObjectLock(ctx, project, bucket.CreateBucketWithObjectLockParams{
		Name:              name,
		Placement:         opts.Location,
		ObjectLockEnabled: opts.LockEnabled,
	})

	return ConvertError(err, name, "")
}

func (layer *gatewayLayer) GetBucketInfo(ctx context.Context, bucketName string) (bucketInfo minio.BucketInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := ValidateBucket(ctx, bucketName); err != nil {
		return minio.BucketInfo{}, minio.BucketNameInvalid{Bucket: bucketName}
	}

	project, err := projectFromContext(ctx, bucketName, "")
	if err != nil {
		return minio.BucketInfo{}, err
	}

	bucket, err := project.StatBucket(ctx, bucketName)
	if err != nil {
		return minio.BucketInfo{}, ConvertError(err, bucketName, "")
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
		return nil, ConvertError(buckets.Err(), "", "")
	}
	return items, nil
}

func (layer *gatewayLayer) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) (err error) {
	defer mon.Task()(&ctx)(&err)

	if err := ValidateBucket(ctx, bucket); err != nil {
		return minio.BucketNameInvalid{Bucket: bucket}
	}

	project, err := projectFromContext(ctx, bucket, "")
	if err != nil {
		return err
	}

	if forceDelete {
		_, err = project.DeleteBucketWithObjects(ctx, bucket)
		return ConvertError(err, bucket, "")
	}

	_, err = project.DeleteBucket(ctx, bucket)
	if errs.Is(err, uplink.ErrBucketNotEmpty) {
		// Check if the bucket contains any non-pending objects. If it
		// doesn't, this would mean there were initiated non-committed
		// and non-aborted multipart uploads. Other S3 implementations
		// allow deletion of such buckets, but with uplink, we need to
		// explicitly force bucket deletion.
		list, more, listErr := versioned.ListObjects(ctx, project, bucket, &versioned.ListObjectsOptions{
			Recursive: true,
			Limit:     1,
		})
		if listErr != nil {
			return ConvertError(listErr, bucket, "")
		}
		monLstNext(1, singleListing)
		if len(list) == 0 && !more {
			_, err = project.DeleteBucketWithObjects(ctx, bucket)
			// err is handled by the return call (below) this path falls
			// into.
		}
	}

	return ConvertError(err, bucket, "")
}

func (layer *gatewayLayer) GetObjectLockConfig(ctx context.Context, bucketName string) (objectLockConfig *objectlock.Config, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := ValidateBucket(ctx, bucketName); err != nil {
		return &objectlock.Config{}, minio.BucketNameInvalid{Bucket: bucketName}
	}

	project, err := projectFromContext(ctx, bucketName, "")
	if err != nil {
		return &objectlock.Config{}, err
	}

	config, err := bucket.GetBucketObjectLockConfiguration(ctx, project, bucketName)
	if err != nil {
		if errors.Is(err, bucket.ErrBucketNoLock) {
			return &objectlock.Config{}, minio.BucketObjectLockConfigNotFound{Bucket: bucketName}
		}
		return &objectlock.Config{}, ConvertError(err, bucketName, "")
	}

	if !config.Enabled {
		return &objectlock.Config{}, minio.BucketObjectLockConfigNotFound{Bucket: bucketName}
	}
	minioConfig := toMinioObjectLockConfiguration(config)

	return &minioConfig, nil
}

func (layer *gatewayLayer) SetObjectLockConfig(ctx context.Context, bucketName string, config *objectlock.Config) (err error) {
	defer mon.Task()(&ctx)(&err)

	if err := ValidateBucket(ctx, bucketName); err != nil {
		return minio.BucketNameInvalid{Bucket: bucketName}
	}

	project, err := projectFromContext(ctx, bucketName, "")
	if err != nil {
		return err
	}

	var uplinkCfg *metaclient.BucketObjectLockConfiguration
	if config != nil {
		if config.ObjectLockEnabled != "Enabled" {
			return objectlock.ErrMalformedXML
		}
		uplinkCfg = &metaclient.BucketObjectLockConfiguration{
			Enabled: true,
		}
		if config.Rule != nil {
			mode, err := parseRetentionMode(config.Rule.DefaultRetention.Mode)
			if err != nil {
				return objectlock.ErrMalformedXML
			}

			uplinkCfg.DefaultRetention = &metaclient.DefaultRetention{
				Mode: mode,
			}
			if config.Rule.DefaultRetention.Days != nil {
				uplinkCfg.DefaultRetention.Days = *config.Rule.DefaultRetention.Days
			}
			if config.Rule.DefaultRetention.Years != nil {
				uplinkCfg.DefaultRetention.Years = *config.Rule.DefaultRetention.Years
			}
		}
	}

	err = bucket.SetBucketObjectLockConfiguration(ctx, project, bucketName, uplinkCfg)

	return ConvertError(err, bucketName, "")
}

func (layer *gatewayLayer) listObjectsFast(
	ctx context.Context,
	project *uplink.Project,
	bucket, prefix, continuationToken, delimiter string,
	maxKeys int,
	startAfter string,
) (
	prefixes []string,
	objects []minio.ObjectInfo,
	nextContinuationToken string,
	err error,
) {
	defer mon.Task()(&ctx)(&err)

	after := startAfter

	if continuationToken != "" {
		after = continuationToken
	}

	recursive := delimiter == ""
	limit := limitResults(maxKeys, layer.compatibilityConfig.MaxKeysLimit)

	list, more, err := versioned.ListObjects(ctx, project, bucket, &versioned.ListObjectsOptions{
		Prefix:    prefix,
		Cursor:    strings.TrimPrefix(after, prefix),
		Recursive: recursive,
		System:    true,
		Custom:    layer.compatibilityConfig.IncludeCustomMetadataListing,
		Limit:     limit,
	})
	if err != nil {
		return nil, nil, "", err
	}
	monLstNext(int64(limit), fastListing)

	for _, item := range list {
		key := item.Key
		if prefix != "" {
			key = prefix + key
		}

		if item.IsPrefix {
			prefixes = append(prefixes, key)
		} else {
			object := minioVersionedObjectInfo(bucket, "", item)
			object.Name = key
			objects = append(objects, object)
		}

		if more {
			nextContinuationToken = key
		}
	}

	return prefixes, objects, nextContinuationToken, nil
}

func (layer *gatewayLayer) listObjectsSingle(
	ctx context.Context,
	project *uplink.Project,
	bucket, prefix, continuationToken, delimiter string,
	maxKeys int,
	startAfter string,
) (
	prefixes []string,
	objects []minio.ObjectInfo,
	nextContinuationToken string,
	err error,
) {
	defer mon.Task()(&ctx)(&err)

	after := startAfter

	if continuationToken != "" {
		after = continuationToken
	}

	limit := limitResults(maxKeys, layer.compatibilityConfig.MaxKeysLimit)

	if after == "" {
		object, err := project.StatObject(ctx, bucket, prefix)
		if err != nil {
			if !errors.Is(err, uplink.ErrObjectNotFound) {
				return nil, nil, "", err
			}
		} else {
			objects = append(objects, minioObjectInfo(bucket, "", object))

			if limit == 1 {
				return prefixes, objects, object.Key, nil
			}
		}
	}

	if delimiter == "/" && (after == "" || after == prefix) {
		p := prefix + "/"

		list, more, err := versioned.ListObjects(ctx, project, bucket, &versioned.ListObjectsOptions{
			Prefix:    p,
			Recursive: true,
			Limit:     1,
		})
		if err != nil {
			return nil, nil, "", err
		}
		monLstNext(1, singleListing)

		if len(list) > 0 || more {
			prefixes = append(prefixes, p)
		}
	}

	if len(prefixes) > 0 {
		nextContinuationToken = prefixes[0]
	} else if len(objects) > 0 {
		nextContinuationToken = objects[0].Name
	}

	return prefixes, objects, nextContinuationToken, nil
}

// collapseKey returns a key, if possible, that begins with prefix and ends with
// delimiter, sharing a path with key. Otherwise, it returns false.
func collapseKey(prefix, delimiter, key string) (commonPrefix string, collapsed bool) {
	// Try to find delimiter in key, trimming prefix, as we don't want to search
	// for it in prefix.
	i := strings.Index(key[len(prefix):], delimiter)

	// Proceed to collapse key only if the delimiter is found and not empty.
	// Index returns i=0 for empty substrings, but we don't want to collapse
	// keys when the delimiter is empty.
	if i >= 0 && delimiter != "" {
		// When we collapse the key, we need to cut it where the delimiter
		// begins and add back only the delimiter (alternatively, we could cut
		// it after the delimiter). Since we searched for it in key without
		// prefix, we need to offset index by prefix length.
		return key[:i+len(prefix)] + delimiter, true
	}

	return "", false
}

// itemsToPrefixesAndObjects dispatches items into prefixes and objects. It
// collapses all keys into common prefixes that share a path between prefix and
// delimiter. If there are more items than the limit, nextContinuationToken will
// be the last non-truncated item.
func (layer *gatewayLayer) itemsToPrefixesAndObjects(
	items []*uplink.Object,
	bucket, prefix, delimiter string,
	maxKeys int,
) (
	prefixes []string,
	objects []minio.ObjectInfo,
	nextContinuationToken string,
) {
	sort.Slice(items, func(i, j int) bool {
		return items[i].Key < items[j].Key
	})

	limit := limitResults(maxKeys, layer.compatibilityConfig.MaxKeysLimit)
	prefixesLookup := make(map[string]struct{})

	for _, item := range items {
		// There's a possibility that we managed to get libuplink to get the
		// necessary prefixes for us. If that's true, just add it.
		if item.IsPrefix {
			if limit == 0 {
				return prefixes, objects, nextContinuationToken
			}
			prefixes = append(prefixes, item.Key)
			nextContinuationToken = item.Key
			limit--
			continue
		}

		commonPrefix, ok := collapseKey(prefix, delimiter, item.Key)
		// If we cannot roll up into CommonPrefix, just add the key.
		if !ok {
			if limit == 0 {
				return prefixes, objects, nextContinuationToken
			}
			objects = append(objects, minioObjectInfo(bucket, "", item))
			nextContinuationToken = item.Key
			limit--
			continue
		}

		// Check with prefixesLookup whether we already collapsed the same key
		// as we can't have two same common prefixes in the output.
		if _, ok := prefixesLookup[commonPrefix]; !ok {
			if limit == 0 {
				return prefixes, objects, nextContinuationToken
			}
			prefixesLookup[commonPrefix] = struct{}{}
			prefixes = append(prefixes, commonPrefix)
			nextContinuationToken = commonPrefix
			limit--
		}
	}

	return prefixes, objects, ""
}

// listObjectsExhaustive lists the entire bucket discarding keys that do not
// begin with the necessary prefix and come before continuationToken/startAfter.
// It calls itemsToPrefixesAndObjects to dispatch results into prefixes and
// objects.
func (layer *gatewayLayer) listObjectsExhaustive(
	ctx context.Context,
	project *uplink.Project,
	bucket, prefix, continuationToken, delimiter string,
	maxKeys int,
	startAfter string,
) (
	prefixes []string,
	objects []minio.ObjectInfo,
	nextContinuationToken string,
	err error,
) {
	defer mon.Task()(&ctx)(&err)

	// Filling Prefix and Recursive are a few optimizations that try to make
	// exhaustive listing less resource-intensive if it's possible. We still
	// have to comply with (*uplink.Project).ListObjects' API.
	var listPrefix string

	// There is a good chance we don't need to list the entire bucket if the
	// prefix contains forward slashes! If it does, let's list from the last
	// one. If the satellite doesn't give us anything after that chopped-off
	// prefix, we won't return anything anyway.
	if i := strings.LastIndex(prefix, "/"); i != -1 {
		listPrefix = prefix[:i] + "/"
	}

	list := project.ListObjects(ctx, bucket, &uplink.ListObjectsOptions{
		Prefix:    listPrefix,
		Recursive: delimiter != "/",
		System:    true,
		Custom:    layer.compatibilityConfig.IncludeCustomMetadataListing,
	})

	// Cursor priority: ContinuationToken > StartAfter == Marker
	after := startAfter

	if continuationToken != "" {
		after = continuationToken
	}

	var items []*uplink.Object

	for i := 0; monItrNext(list, exhaustiveListing); i++ {
		if i == layer.compatibilityConfig.MaxKeysExhaustiveLimit {
			return nil, nil, "", ErrTooManyItemsToList
		}

		item := list.Item()

		// Skip keys that do not begin with the required prefix.
		if !strings.HasPrefix(item.Key, prefix) {
			continue
		}

		key := item.Key

		// The reason we try to collapse the key in the filtering step is that
		// continuationToken/startAfter could mean the collapsed key. Example:
		//  p1/a/o
		//  p1/b/o
		//  p1/o
		//  p2/o
		//
		//  Prefix:                              p1/
		//  Delimiter:                           /
		//  ContinuationToken/StartAfter/Marker: p1/b/
		//
		// If we didn't collapse keys (and just filter based on Prefix and
		// StartAfter), we would end up with the exact same list as before:
		//  p1/a/o
		//  p1/b/o
		//  p1/o
		//
		// Instead of:
		//  p1/o
		if commonPrefix, ok := collapseKey(prefix, delimiter, item.Key); ok {
			key = commonPrefix
		}

		// We don't care about keys before ContinuationToken/StartAfter/Marker.
		// Skip them.
		if key <= after {
			continue
		}

		items = append(items, item)
	}
	if list.Err() != nil {
		return nil, nil, "", list.Err()
	}

	prefixes, objects, nextContinuationToken = layer.itemsToPrefixesAndObjects(items, bucket, prefix, delimiter, maxKeys)

	return prefixes, objects, nextContinuationToken, nil
}

// listObjectsGeneral lists bucket trying to best-effort conform to AWS
// S3's listing APIs behavior.
//
// It tries to list the bucket in three, mutually-exclusive ways:
//
//  1. Fast;
//  2. Optimized for non-terminated prefix;
//  3. Exhaustive.
//
// If prefix is empty or terminated with a forward slash and delimiter
// is empty or a forward slash, it will call listObjectsFast to list
// items the fastest way possible. For requests that come with prefix
// non-terminated with a forward slash, it will call listObjectsSingle
// to perform a specific optimization for this type of listing. Finally,
// for all other requests, e.g., these that come with a non-forward
// slash delimiter, it will perform an exhaustive listing calling
// listObjectsExhaustive as it's the only way to make such bucket
// listing using libuplink.
//
// Optimization for non-terminated prefixes relies on the fact that much
// of S3-compatible software uses a call to ListObjects(V2) with a
// non-terminated prefix to check whether a key that is equal to this
// prefix exists. It only cares about the first result. Since we can't
// do such listing with libuplink directly and making an exhaustive
// listing for this kind of query has terrible performance, we only
// check if an object exists. We additionally check if there's a prefix
// object for this prefix because it doesn't cost much more. We return
// these items in a list alone, setting continuation token to one of
// them, signaling that there might be more (S3 only guarantees to
// return not too many items and not as many as possible). If we didn't
// find anything, we fall back to the exhaustive listing.
//
// The only S3-incompatible thing that listObjectsGeneral represents is
// that for fast listing, it will not return items lexicographically
// ordered, but it is a burden we must all bear.
//
// If layer.compatibilityConfig.FullyCompatibleListing is true, it will
// always list exhaustively to achieve full S3 compatibility. Use at
// your own risk.
//
// If maxKeys is 0, it will return an empty minio.ListObjectsV2Info,
// which later translates into a result that is compatible with AWS S3.
func (layer *gatewayLayer) listObjectsGeneral(
	ctx context.Context,
	project *uplink.Project,
	bucket, prefix, continuationToken, delimiter string,
	maxKeys int,
	startAfter string,
) (_ minio.ListObjectsV2Info, err error) {
	defer mon.Task()(&ctx)(&err)

	if maxKeys == 0 {
		return minio.ListObjectsV2Info{}, nil
	}

	var (
		prefixes []string
		objects  []minio.ObjectInfo
		token    string
	)

	supportedPrefix := prefix == "" || strings.HasSuffix(prefix, "/")

	if !layer.compatibilityConfig.FullyCompatibleListing && supportedPrefix && (delimiter == "" || delimiter == "/") {
		prefixes, objects, token, err = layer.listObjectsFast(
			ctx,
			project,
			bucket, prefix, continuationToken, delimiter,
			maxKeys,
			startAfter)
		if err != nil {
			return minio.ListObjectsV2Info{}, err
		}
	} else if !supportedPrefix {
		prefixes, objects, token, err = layer.listObjectsSingle(
			ctx,
			project,
			bucket, prefix, continuationToken, delimiter,
			maxKeys,
			startAfter)
		if err != nil {
			return minio.ListObjectsV2Info{}, err
		}
		// Prefix optimization did not work; we need to fall back to exhaustive.
		if prefixes == nil && objects == nil {
			prefixes, objects, token, err = layer.listObjectsExhaustive(
				ctx,
				project,
				bucket, prefix, continuationToken, delimiter,
				maxKeys,
				startAfter)
			if err != nil {
				return minio.ListObjectsV2Info{}, err
			}
		}
	} else {
		prefixes, objects, token, err = layer.listObjectsExhaustive(
			ctx,
			project,
			bucket, prefix, continuationToken, delimiter,
			maxKeys,
			startAfter)
		if err != nil {
			return minio.ListObjectsV2Info{}, err
		}
	}

	return minio.ListObjectsV2Info{
		IsTruncated:           token != "",
		ContinuationToken:     continuationToken,
		NextContinuationToken: token,
		Objects:               objects,
		Prefixes:              prefixes,
	}, nil
}

// ListObjects calls listObjectsGeneral and translates response from
// ListObjectsV2Info to ListObjectsInfo.
//
// NOTE: For ListObjects (v1), AWS S3 returns NextMarker only if you have
// delimiter request parameter specified; MinIO always returns NextMarker.
// ListObjects does what MinIO does.
func (layer *gatewayLayer) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (_ minio.ListObjectsInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := ValidateBucket(ctx, bucket); err != nil {
		return minio.ListObjectsInfo{}, minio.BucketNameInvalid{Bucket: bucket}
	}

	project, err := projectFromContext(ctx, bucket, "")
	if err != nil {
		return minio.ListObjectsInfo{}, err
	}

	// For V1, marker is V2's startAfter and continuationToken does not exist.
	v2, err := layer.listObjectsGeneral(ctx, project, bucket, prefix, "", delimiter, maxKeys, marker)

	result := minio.ListObjectsInfo{
		IsTruncated: v2.IsTruncated,
		NextMarker:  v2.NextContinuationToken,
		Objects:     v2.Objects,
		Prefixes:    v2.Prefixes,
	}

	return result, ConvertError(err, bucket, "")
}

// ListObjectsV2 calls listObjectsGeneral.
func (layer *gatewayLayer) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (_ minio.ListObjectsV2Info, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := ValidateBucket(ctx, bucket); err != nil {
		return minio.ListObjectsV2Info{}, minio.BucketNameInvalid{Bucket: bucket}
	}

	project, err := projectFromContext(ctx, bucket, "")
	if err != nil {
		return minio.ListObjectsV2Info{}, err
	}

	result, err := layer.listObjectsGeneral(ctx, project, bucket, prefix, continuationToken, delimiter, maxKeys, startAfter)

	return result, ConvertError(err, bucket, "")
}

// ListObjectVersions returns information about all versions of the objects in a bucket.
func (layer *gatewayLayer) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (_ minio.ListObjectVersionsInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := ValidateBucket(ctx, bucket); err != nil {
		return minio.ListObjectVersionsInfo{}, minio.BucketNameInvalid{Bucket: bucket}
	}

	if len(marker) == 0 && len(versionMarker) != 0 {
		return minio.ListObjectVersionsInfo{}, ErrVersionIDMarkerWithoutKeyMarker(bucket)
	}

	if delimiter != "" && delimiter != "/" {
		return minio.ListObjectVersionsInfo{}, minio.NotImplemented{Message: fmt.Sprintf("Unsupported delimiter: %q", delimiter)}
	}

	project, err := projectFromContext(ctx, bucket, "")
	if err != nil {
		return minio.ListObjectVersionsInfo{}, err
	}

	// [1/2] We begin listing with an optimization for prefixes that aren't
	// terminated with a forward slash. For example, for prefixes such as "p/a"
	// we will start listing from "p/".
	originalPrefix, prefix := prefix, prefix[:strings.LastIndex(prefix, "/")+1]

	version, err := decodeVersionID(versionMarker)
	if err != nil {
		return minio.ListObjectVersionsInfo{}, err
	}

	recursive := delimiter == ""

	items, more, err := versioned.ListObjectVersions(ctx, project, bucket, &versioned.ListObjectVersionsOptions{
		Prefix:        prefix,
		Cursor:        strings.TrimPrefix(marker, prefix),
		VersionCursor: version,
		Recursive:     recursive,
		System:        true,
		Custom:        layer.compatibilityConfig.IncludeCustomMetadataListing,
		Limit:         limitResults(maxKeys, layer.compatibilityConfig.MaxKeysLimit),
	})
	if err != nil {
		return minio.ListObjectVersionsInfo{}, ConvertError(err, bucket, "")
	}

	var prefixes []string
	var objects []minio.ObjectInfo
	var nextMarker, nextVersionIDMarker string
	for _, item := range items {
		key := item.Key
		if prefix != "" {
			key = prefix + key
		}

		// [2/2] We filter results based on the originally supplied prefix and
		// not the one we actually list from. This means that in rare cases we
		// might return an empty list with just next markers changed, but
		// because the S3 spec allows it, we can pressure the client to page
		// results instead of the gateway listing exhaustively.
		if !strings.HasPrefix(key, originalPrefix) {
			if more {
				nextMarker = key
				nextVersionIDMarker = encodeVersionID(item.Version)
			}
			continue
		}

		if item.IsPrefix {
			prefixes = append(prefixes, key)
		} else {
			object := minioVersionedObjectInfo(bucket, "", item)
			object.Name = key
			objects = append(objects, object)
		}

		if more {
			nextMarker = key
			nextVersionIDMarker = encodeVersionID(item.Version)
		}
	}

	return minio.ListObjectVersionsInfo{
		IsTruncated:         more,
		NextMarker:          nextMarker,
		NextVersionIDMarker: nextVersionIDMarker,
		Objects:             objects,
		Prefixes:            prefixes,
	}, nil
}

func (layer *gatewayLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (reader *minio.GetObjectReader, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := ValidateBucket(ctx, bucket); err != nil {
		return nil, minio.BucketNameInvalid{Bucket: bucket}
	}

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

	version, err := decodeVersionID(opts.VersionID)
	if err != nil {
		return nil, ConvertError(err, bucket, object)
	}

	download, err := versioned.DownloadObject(ctx, project, bucket, object, version, downloadOpts)
	if err != nil {
		return nil, ConvertError(err, bucket, object)
	}

	objectInfo := minioVersionedObjectInfo(bucket, "", download.Info())
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
		// note: we do not specify ResourceSize here because that would require
		// an additional stat call to get the ContentLength from metadata.
		return nil, minio.InvalidRange{OffsetBegin: rs.Start, OffsetEnd: rs.End}
	}
}

func (layer *gatewayLayer) GetObjectInfo(ctx context.Context, bucket, objectPath string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := ValidateBucket(ctx, bucket); err != nil {
		return minio.ObjectInfo{}, minio.BucketNameInvalid{Bucket: bucket}
	}

	project, err := projectFromContext(ctx, bucket, objectPath)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	version, err := decodeVersionID(opts.VersionID)
	if err != nil {
		return minio.ObjectInfo{}, ConvertError(err, bucket, objectPath)
	}

	object, err := versioned.StatObject(ctx, project, bucket, objectPath, version)
	if err != nil {
		// TODO this should be removed and implemented on satellite side
		err = checkBucketError(ctx, project, bucket, objectPath, err)
		return minio.ObjectInfo{}, ConvertError(err, bucket, objectPath)
	}

	return minioVersionedObjectInfo(bucket, "", object), nil
}

func (layer *gatewayLayer) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := ValidateBucket(ctx, bucket); err != nil {
		return minio.ObjectInfo{}, minio.BucketNameInvalid{Bucket: bucket}
	}

	if len(object) > memory.KiB.Int() { // https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
		return minio.ObjectInfo{}, minio.ObjectNameTooLong{Bucket: bucket, Object: object}
	}

	if storageClass, ok := opts.UserDefined[xhttp.AmzStorageClass]; ok && storageClass != storageclass.STANDARD {
		return minio.ObjectInfo{}, minio.NotImplemented{Message: "PutObject (storage class)"}
	}

	project, err := projectFromContext(ctx, bucket, object)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	if data == nil {
		hashReader, err := hash.NewReader(bytes.NewReader([]byte{}), 0, "", "", 0)
		if err != nil {
			return minio.ObjectInfo{}, ConvertError(err, bucket, object)
		}
		data = minio.NewPutObjReader(hashReader)
	}

	e, err := parseTTL(opts.UserDefined)
	if err != nil {
		return minio.ObjectInfo{}, ErrInvalidTTL
	}

	var retention metaclient.Retention
	if opts.Retention != nil {
		retMode, err := parseRetentionMode(opts.Retention.Mode)
		if err != nil {
			return minio.ObjectInfo{}, err
		}

		retention.Mode = retMode
		retention.RetainUntil = opts.Retention.RetainUntilDate.Time
	}
	legalHold := false
	if opts.LegalHold != nil {
		legalHold, err = parseLegalHoldStatus(*opts.LegalHold)
		if err != nil {
			return minio.ObjectInfo{}, ConvertError(err, bucket, object)
		}
	}

	if err := verifyIfNoneMatch(opts.IfNoneMatch); err != nil {
		return minio.ObjectInfo{}, err
	}

	upload, err := versioned.UploadObject(ctx, project, bucket, object, &metaclient.UploadOptions{
		Expires:     e,
		Retention:   retention,
		LegalHold:   legalHold,
		IfNoneMatch: opts.IfNoneMatch,
	})
	if err != nil {
		return minio.ObjectInfo{}, ConvertError(err, bucket, object)
	}

	size, err := sync2.Copy(ctx, upload, data)
	if err != nil {
		abortErr := upload.Abort()
		err = errs.Combine(err, abortErr)
		return minio.ObjectInfo{}, ConvertError(err, bucket, object)
	}

	lengthRange := opts.PostPolicy.Conditions.ContentLengthRange
	if lengthRange.Valid {
		if size < lengthRange.Min {
			abortErr := upload.Abort()
			return minio.ObjectInfo{}, errs.Combine(minio.ObjectTooSmall{}, abortErr)
		}

		if size > lengthRange.Max {
			abortErr := upload.Abort()
			return minio.ObjectInfo{}, errs.Combine(minio.ObjectTooLarge{}, abortErr)
		}
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
		return minio.ObjectInfo{}, ConvertError(err, bucket, object)
	}

	err = upload.Commit()
	if err != nil {
		return minio.ObjectInfo{}, ConvertError(err, bucket, object)
	}

	return minioVersionedObjectInfo(bucket, etag, upload.Info()), nil
}

func (layer *gatewayLayer) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, destOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	// S3 doesn't support destination VersionID but let's handle this just in case
	if destOpts.VersionID != "" {
		return minio.ObjectInfo{}, minio.NotImplemented{}
	}

	srcAndDestSame := srcBucket == destBucket && srcObject == destObject

	if layer.compatibilityConfig.DisableCopyObject && !srcAndDestSame {
		return minio.ObjectInfo{}, minio.NotImplemented{Message: "CopyObject"}
	}

	if len(destObject) > memory.KiB.Int() { // https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
		return minio.ObjectInfo{}, minio.ObjectNameTooLong{Bucket: destBucket, Object: destObject}
	}

	if storageClass, ok := srcInfo.UserDefined[xhttp.AmzStorageClass]; ok && storageClass != storageclass.STANDARD {
		return minio.ObjectInfo{}, minio.NotImplemented{Message: "CopyObject (storage class)"}
	}

	switch {
	case srcObject == "":
		return minio.ObjectInfo{}, minio.ObjectNameInvalid{Bucket: srcBucket}
	case destObject == "":
		return minio.ObjectInfo{}, minio.ObjectNameInvalid{Bucket: destBucket}
	}

	if err := ValidateBucket(ctx, srcBucket); err != nil {
		return minio.ObjectInfo{}, minio.BucketNameInvalid{Bucket: srcBucket}
	}
	if err := ValidateBucket(ctx, destBucket); err != nil {
		return minio.ObjectInfo{}, minio.BucketNameInvalid{Bucket: destBucket}
	}

	project, err := projectFromContext(ctx, srcBucket, srcObject)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	if srcAndDestSame && srcInfo.VersionID == "" {
		// TODO this should be removed and implemented on satellite side
		_, err = project.StatBucket(ctx, srcBucket)
		if err != nil {
			return minio.ObjectInfo{}, ConvertError(err, srcBucket, "")
		}

		// Source and destination are the same. Do nothing, apart from ensuring
		// metadata is updated. Tools like rclone sync use CopyObject with the
		// same source and destination as a way of updating existing metadata
		// like last modified date/time, as there's no S3 endpoint for
		// manipulating existing object's metadata.
		info, err := project.StatObject(ctx, srcBucket, srcObject)
		if err != nil {
			return minio.ObjectInfo{}, ConvertError(err, srcBucket, srcObject)
		}

		upsertObjectMetadata(srcInfo.UserDefined, info.Custom)

		err = project.UpdateObjectMetadata(ctx, srcBucket, srcObject, srcInfo.UserDefined, nil)
		if err != nil {
			return minio.ObjectInfo{}, ConvertError(err, srcBucket, srcObject)
		}

		return srcInfo, nil
	}

	version, err := decodeVersionID(srcInfo.VersionID)
	if err != nil {
		return minio.ObjectInfo{}, ConvertError(err, srcBucket, srcObject)
	}

	var retention metaclient.Retention
	if destOpts.Retention != nil {
		var mode storj.RetentionMode
		switch destOpts.Retention.Mode {
		case objectlock.RetCompliance:
			mode = storj.ComplianceMode
		case objectlock.RetGovernance:
			mode = storj.GovernanceMode
		default:
			return minio.ObjectInfo{}, objectlock.ErrUnknownWORMModeDirective
		}
		retention = metaclient.Retention{
			Mode:        mode,
			RetainUntil: destOpts.Retention.RetainUntilDate.Time,
		}
	}

	if err := verifyIfNoneMatch(destOpts.IfNoneMatch); err != nil {
		return minio.ObjectInfo{}, err
	}

	object, err := versioned.CopyObject(ctx, project, srcBucket, srcObject, version, destBucket, destObject, versioned.CopyObjectOptions{
		Retention:   retention,
		LegalHold:   destOpts.LegalHold != nil && *destOpts.LegalHold == objectlock.LegalHoldOn,
		IfNoneMatch: destOpts.IfNoneMatch,
	})
	if err != nil {
		// TODO how we can improve it, its ugly
		if errors.Is(err, uplink.ErrBucketNotFound) {
			if strings.Contains(err.Error(), srcBucket) {
				return minio.ObjectInfo{}, minio.BucketNotFound{Bucket: srcBucket}
			} else if strings.Contains(err.Error(), destBucket) {
				return minio.ObjectInfo{}, minio.BucketNotFound{Bucket: destBucket}
			}
		}
		return minio.ObjectInfo{}, ConvertError(err, destBucket, destObject)
	}

	// TODO most probably we need better condition
	if len(srcInfo.UserDefined) > 0 {
		// TODO currently we need to set metadata as a separate step because we
		// don't have a solution to not override ETag stored in custom metadata
		upsertObjectMetadata(srcInfo.UserDefined, object.Custom)

		err = project.UpdateObjectMetadata(ctx, destBucket, destObject, srcInfo.UserDefined, nil)
		if err != nil {
			return minio.ObjectInfo{}, ConvertError(err, destBucket, destObject)
		}
		object.Custom = uplink.CustomMetadata(srcInfo.UserDefined)
	}

	return minioVersionedObjectInfo(destBucket, "", object), nil
}

func (layer *gatewayLayer) DeleteObject(ctx context.Context, bucket, objectPath string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := ValidateBucket(ctx, bucket); err != nil {
		return minio.ObjectInfo{}, minio.BucketNameInvalid{Bucket: bucket}
	}

	project, err := projectFromContext(ctx, bucket, objectPath)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	version, err := decodeVersionID(opts.VersionID)
	if err != nil {
		return minio.ObjectInfo{}, ConvertError(err, bucket, objectPath)
	}

	delOpts := &metaclient.DeleteObjectOptions{BypassGovernanceRetention: opts.BypassGovernanceRetention}
	object, err := versioned.DeleteObject(ctx, project, bucket, objectPath, version, delOpts)
	if err != nil {
		return minio.ObjectInfo{}, ConvertError(err, bucket, objectPath)
	}

	if object == nil {
		// TODO this should be removed and implemented on satellite side.
		//
		// This call needs to occur with DeleteObject call, because project.DeleteObject
		// will return a nil error for a missing bucket. To maintain consistency,
		// we need to manually check if the bucket exists.
		_, err = project.StatBucket(ctx, bucket)
		if err != nil {
			return minio.ObjectInfo{}, ConvertError(err, bucket, objectPath)
		}
	}

	return minioVersionedObjectInfo(bucket, "", object), nil
}

func (layer *gatewayLayer) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) (deletedObjects []minio.DeletedObject, deleteErrors []minio.DeleteObjectsError, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := ValidateBucket(ctx, bucket); err != nil {
		return nil, nil, minio.BucketNameInvalid{Bucket: bucket}
	}

	project, err := projectFromContext(ctx, bucket, "")
	if err != nil {
		return nil, nil, err
	}

	items := make([]versioned.DeleteObjectsItem, 0, len(objects))
	for _, object := range objects {
		version, err := decodeVersionID(object.VersionID)
		if err != nil {
			return nil, nil, ErrObjectVersionInvalid
		}
		items = append(items, metaclient.DeleteObjectsItem{
			ObjectKey: object.ObjectName,
			Version:   version,
		})
	}

	results, err := versioned.DeleteObjects(ctx, project, bucket, items, &metaclient.DeleteObjectsOptions{
		BypassGovernanceRetention: opts.BypassGovernanceRetention,
		Quiet:                     opts.Quiet,
	})
	if err != nil {
		if errors.Is(err, versioned.ErrDeleteObjectsUnimplemented) {
			deletedObjects, deleteErrors = layer.deleteObjectsFallback(ctx, bucket, objects, opts)
			return deletedObjects, deleteErrors, nil
		}
		return nil, nil, ConvertError(err, bucket, "")
	}

	var numErrs int
	for _, result := range results {
		if result.Status != storj.DeleteObjectsStatusNotFound && result.Status != storj.DeleteObjectsStatusOK {
			numErrs++
		}
	}

	deletedObjects = make([]minio.DeletedObject, 0, len(results)-numErrs)
	deleteErrors = make([]minio.DeleteObjectsError, 0, numErrs)
	for _, result := range results {
		switch result.Status {
		case storj.DeleteObjectsStatusOK:
			deleted := minio.DeletedObject{
				ObjectName: result.ObjectKey,
			}
			if result.Removed != nil {
				deleted.VersionID = encodeVersionID(result.Removed.Version)
			}
			if result.Marker != nil {
				deleted.DeleteMarker = true
				deleted.DeleteMarkerVersionID = encodeVersionID(result.Marker.Version)
			}
			deletedObjects = append(deletedObjects, deleted)
		case storj.DeleteObjectsStatusNotFound:
			deletedObjects = append(deletedObjects, minio.DeletedObject{
				ObjectName: result.ObjectKey,
				VersionID:  encodeVersionID(result.RequestedVersion),
			})
		default:
			deleteErrors = append(deleteErrors, minio.DeleteObjectsError{
				ObjectName: result.ObjectKey,
				VersionID:  encodeVersionID(result.RequestedVersion),
				Error:      convertDeleteObjectsStatus(result.Status),
			})
		}
	}

	return deletedObjects, deleteErrors, nil
}

func convertDeleteObjectsStatus(status storj.DeleteObjectsStatus) error {
	switch status {
	case storj.DeleteObjectsStatusLocked:
		return ErrObjectProtected
	case storj.DeleteObjectsStatusUnauthorized:
		return ErrAccessDenied
	default:
		return ErrInternalError
	}
}

func (layer *gatewayLayer) deleteObjectsFallback(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) ([]minio.DeletedObject, []minio.DeleteObjectsError) {
	deletedObjects, deleteErrors := make([]minio.DeletedObject, 0, len(objects)), make([]minio.DeleteObjectsError, 0, len(objects))

	limiter := sync2.NewLimiter(layer.compatibilityConfig.DeleteObjectsConcurrency)

	type deleteResult struct {
		index       int
		deleted     *minio.DeletedObject
		deleteError *minio.DeleteObjectsError
	}

	finished := make(map[int]struct{}, len(objects))
	resultCh := make(chan deleteResult)

	go func() {
		defer close(resultCh)

		for i, object := range objects {
			i, object := i, object
			opts := opts
			opts.VersionID = object.VersionID

			limiter.Go(ctx, func() {
				deleted, deleteError := layer.deleteObjectsFallbackSingle(ctx, bucket, object, opts)
				resultCh <- deleteResult{
					index:       i,
					deleted:     deleted,
					deleteError: deleteError,
				}
			})
		}

		limiter.Wait()
	}()

	for result := range resultCh {
		if result.deleteError != nil {
			deleteErrors = append(deleteErrors, *result.deleteError)
		} else if result.deleted != nil {
			deletedObjects = append(deletedObjects, *result.deleted)
		}
		finished[result.index] = struct{}{}
	}

	if ctx.Err() != nil {
		for i, object := range objects {
			if _, ok := finished[i]; !ok {
				deleteErrors = append(deleteErrors, minio.DeleteObjectsError{
					ObjectName: object.ObjectName,
					VersionID:  object.VersionID,
					Error:      minio.OperationTimedOut{},
				})
			}
		}
	}

	return deletedObjects, deleteErrors
}

func (layer *gatewayLayer) deleteObjectsFallbackSingle(ctx context.Context, bucket string, object minio.ObjectToDelete, opts minio.ObjectOptions) (*minio.DeletedObject, *minio.DeleteObjectsError) {
	info, err := layer.DeleteObject(ctx, bucket, object.ObjectName, opts)
	if err != nil && !errors.As(err, &minio.ObjectNotFound{}) {
		return nil, &minio.DeleteObjectsError{
			ObjectName: object.ObjectName,
			VersionID:  object.VersionID,
			Error:      ConvertError(err, bucket, object.ObjectName),
		}
	}

	deleted := &minio.DeletedObject{
		ObjectName:   object.ObjectName,
		VersionID:    info.VersionID,
		DeleteMarker: info.DeleteMarker,
	}
	if deleted.DeleteMarker {
		deleted.DeleteMarkerVersionID = info.VersionID
	}

	return deleted, nil
}

func (layer *gatewayLayer) IsTaggingSupported() bool {
	return true
}

func (layer *gatewayLayer) PutObjectTags(ctx context.Context, bucket, objectPath string, tags string, opts minio.ObjectOptions) (_ minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	// TODO(ver): not implemented yet
	if opts.VersionID != "" {
		return minio.ObjectInfo{}, minio.NotImplemented{}
	}

	if err := ValidateBucket(ctx, bucket); err != nil {
		return minio.ObjectInfo{}, minio.BucketNameInvalid{Bucket: bucket}
	}

	project, err := projectFromContext(ctx, bucket, objectPath)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	object, err := project.StatObject(ctx, bucket, objectPath)
	if err != nil {
		// TODO this should be removed and implemented on satellite side
		err = checkBucketError(ctx, project, bucket, objectPath, err)
		return minio.ObjectInfo{}, ConvertError(err, bucket, objectPath)
	}

	if _, ok := object.Custom["s3:tags"]; !ok && tags == "" {
		return minioObjectInfo(bucket, "", object), nil
	}

	newMetadata := object.Custom.Clone()
	if tags == "" {
		delete(newMetadata, "s3:tags")
	} else {
		newMetadata["s3:tags"] = tags
	}

	err = project.UpdateObjectMetadata(ctx, bucket, objectPath, newMetadata, nil)
	if err != nil {
		return minio.ObjectInfo{}, ConvertError(err, bucket, objectPath)
	}

	return minioObjectInfo(bucket, "", object), nil
}

func (layer *gatewayLayer) GetObjectTags(ctx context.Context, bucket, objectPath string, opts minio.ObjectOptions) (t *tags.Tags, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := ValidateBucket(ctx, bucket); err != nil {
		return nil, minio.BucketNameInvalid{Bucket: bucket}
	}

	project, err := projectFromContext(ctx, bucket, objectPath)
	if err != nil {
		return nil, err
	}

	version, err := decodeVersionID(opts.VersionID)
	if err != nil {
		return nil, ConvertError(err, bucket, objectPath)
	}

	object, err := versioned.StatObject(ctx, project, bucket, objectPath, version)
	if err != nil {
		// TODO this should be removed and implemented on satellite side
		err = checkBucketError(ctx, project, bucket, objectPath, err)
		return nil, ConvertError(err, bucket, objectPath)
	}

	t, err = tags.ParseObjectTags(object.Custom["s3:tags"])
	if err != nil {
		return nil, ConvertError(err, bucket, objectPath)
	}

	return t, nil
}

func (layer *gatewayLayer) DeleteObjectTags(ctx context.Context, bucket, objectPath string, opts minio.ObjectOptions) (_ minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	// TODO(ver): not implemented yet
	if opts.VersionID != "" {
		return minio.ObjectInfo{}, minio.NotImplemented{}
	}

	if err := ValidateBucket(ctx, bucket); err != nil {
		return minio.ObjectInfo{}, minio.BucketNameInvalid{Bucket: bucket}
	}

	project, err := projectFromContext(ctx, bucket, objectPath)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	object, err := project.StatObject(ctx, bucket, objectPath)
	if err != nil {
		// TODO this should be removed and implemented on satellite side
		err = checkBucketError(ctx, project, bucket, objectPath, err)
		return minio.ObjectInfo{}, ConvertError(err, bucket, objectPath)
	}

	if _, ok := object.Custom["s3:tags"]; !ok {
		return minioObjectInfo(bucket, "", object), nil
	}

	newMetadata := object.Custom.Clone()
	delete(newMetadata, "s3:tags")

	err = project.UpdateObjectMetadata(ctx, bucket, objectPath, newMetadata, nil)
	if err != nil {
		return minio.ObjectInfo{}, ConvertError(err, bucket, objectPath)
	}

	return minioObjectInfo(bucket, "", object), nil
}

// GetBucketVersioning retrieves versioning configuration of a bucket.
func (layer *gatewayLayer) GetBucketVersioning(ctx context.Context, bucketName string) (_ *versioning.Versioning, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := ValidateBucket(ctx, bucketName); err != nil {
		return nil, minio.BucketNameInvalid{Bucket: bucketName}
	}

	project, err := projectFromContext(ctx, bucketName, "")
	if err != nil {
		return nil, err
	}

	state, err := bucket.GetBucketVersioning(ctx, project, bucketName)
	if err != nil {
		return nil, ConvertError(err, bucketName, "")
	}

	// TODO(ver): replace magic numbers with consts on libuplink side
	v := &versioning.Versioning{}
	switch state {
	case 2:
		v.Status = versioning.Enabled
	case 3:
		v.Status = versioning.Suspended
	}

	return v, nil
}

// SetBucketVersioning enables/suspends versioning for a bucket.
func (layer *gatewayLayer) SetBucketVersioning(ctx context.Context, bucketName string, v *versioning.Versioning) (err error) {
	defer mon.Task()(&ctx)(&err)

	if err := ValidateBucket(ctx, bucketName); err != nil {
		return minio.BucketNameInvalid{Bucket: bucketName}
	}

	project, err := projectFromContext(ctx, bucketName, "")
	if err != nil {
		return err
	}

	versioning := true
	if v.Suspended() {
		versioning = false

		// TODO(ver): workaround for not being able to change from unversionded to suspended
		// https://github.com/storj/storj/issues/6591
		state, err := bucket.GetBucketVersioning(ctx, project, bucketName)
		if err != nil {
			return ConvertError(err, bucketName, "")
		}

		if state == 1 {
			err = bucket.SetBucketVersioning(ctx, project, bucketName, true)
			if err != nil {
				return ConvertError(err, bucketName, "")
			}
		}
	}

	err = bucket.SetBucketVersioning(ctx, project, bucketName, versioning)
	if err != nil {
		return ConvertError(err, bucketName, "")
	}

	return nil
}

// GetObjectLegalHold retrieves object lock legal hold configuration of an object.
func (layer *gatewayLayer) GetObjectLegalHold(ctx context.Context, bucketName, object, version string) (_ *objectlock.ObjectLegalHold, err error) {
	defer mon.Task()(&ctx)(&err)

	if err = ValidateBucket(ctx, bucketName); err != nil {
		return nil, minio.BucketNameInvalid{Bucket: bucketName}
	}

	project, err := projectFromContext(ctx, bucketName, object)
	if err != nil {
		return nil, ConvertError(err, bucketName, object)
	}

	versionID, err := decodeVersionID(version)
	if err != nil {
		return nil, ConvertError(err, bucketName, object)
	}

	enabled, err := versioned.GetObjectLegalHold(ctx, project, bucketName, object, versionID)
	if err != nil {
		return nil, ConvertError(err, bucketName, object)
	}

	lh := &objectlock.ObjectLegalHold{
		Status: toMinioLegalHoldStatus(enabled),
	}

	return lh, nil
}

// SetObjectLegalHold sets object lock legal hold configuration for an object.
func (layer *gatewayLayer) SetObjectLegalHold(ctx context.Context, bucketName, object, version string, lh *objectlock.ObjectLegalHold) (err error) {
	defer mon.Task()(&ctx)(&err)

	if err = ValidateBucket(ctx, bucketName); err != nil {
		return minio.BucketNameInvalid{Bucket: bucketName}
	}

	project, err := projectFromContext(ctx, bucketName, object)
	if err != nil {
		return ConvertError(err, bucketName, object)
	}

	versionID, err := decodeVersionID(version)
	if err != nil {
		return ConvertError(err, bucketName, object)
	}

	enabled, err := parseLegalHoldStatus(lh.Status)
	if err != nil {
		return ConvertError(err, bucketName, object)
	}

	err = versioned.SetObjectLegalHold(ctx, project, bucketName, object, versionID, enabled)

	return ConvertError(err, bucketName, object)
}

// GetObjectRetention retrieves object lock configuration of an object.
func (layer *gatewayLayer) GetObjectRetention(ctx context.Context, bucketName, object, version string) (_ *objectlock.ObjectRetention, err error) {
	defer mon.Task()(&ctx)(&err)

	if err = ValidateBucket(ctx, bucketName); err != nil {
		return nil, minio.BucketNameInvalid{Bucket: bucketName}
	}

	project, err := projectFromContext(ctx, bucketName, object)
	if err != nil {
		return nil, ConvertError(err, bucketName, object)
	}

	versionID, err := decodeVersionID(version)
	if err != nil {
		return nil, ConvertError(err, bucketName, object)
	}

	retention, err := versioned.GetObjectRetention(ctx, project, bucketName, object, versionID)
	if err != nil {
		// TODO: Remove this.
		//
		// This check of the error's message is done to appease our build system.
		// Once this package's libuplink dependency version is bumped, libuplink will be
		// unable to properly convert some errors originating from the outdated version
		// of the satellite that our integration tests use.
		//
		// When that happens, those errors will be generic, not wrapped by a specialized
		// libuplink error class. If we don't manually handle them, they will be interpreted
		// as internal server errors (HTTP status 500) by MinIO. This would be an issue
		// because the HTTP status code that should be returned here is 400 Bad Request.
		//
		// Once the satellite version used for integration testing has been updated,
		// this block should be removed.
		if strings.Contains(err.Error(), "Object Lock is not enabled for this bucket") {
			return nil, ErrBucketObjectLockNotEnabled
		}
		return nil, ConvertError(err, bucketName, object)
	}

	r := &objectlock.ObjectRetention{
		Mode: toMinioRetentionMode(retention.Mode),
		RetainUntilDate: objectlock.RetentionDate{
			Time: retention.RetainUntil,
		},
	}

	return r, nil
}

// SetObjectRetention sets object lock configuration for an object.
func (layer *gatewayLayer) SetObjectRetention(ctx context.Context, bucketName, object, version string, opts minio.ObjectOptions) (err error) {
	defer mon.Task()(&ctx)(&err)

	if err = ValidateBucket(ctx, bucketName); err != nil {
		return minio.BucketNameInvalid{Bucket: bucketName}
	}

	project, err := projectFromContext(ctx, bucketName, object)
	if err != nil {
		return ConvertError(err, bucketName, object)
	}

	var retention metaclient.Retention
	if opts.Retention != nil {
		retMode, err := parseRetentionMode(opts.Retention.Mode)
		if err != nil {
			return err
		}

		retention.Mode = retMode
		retention.RetainUntil = opts.Retention.RetainUntilDate.Time
	}

	versionID, err := decodeVersionID(version)
	if err != nil {
		return ConvertError(err, bucketName, object)
	}

	err = versioned.SetObjectRetention(ctx, project, bucketName, object, versionID, retention, &metaclient.SetObjectRetentionOptions{
		BypassGovernanceRetention: opts.BypassGovernanceRetention,
	})

	// TODO: Remove this.
	//
	// This check of the error's message is done to appease our build system.
	// Once this package's libuplink dependency version is bumped, libuplink will be
	// unable to properly convert some errors originating from the outdated version
	// of the satellite that our integration tests use.
	//
	// When that happens, those errors will be generic, not wrapped by a specialized
	// libuplink error class. If we don't manually handle them, they will be interpreted
	// as internal server errors (HTTP status 500) by MinIO. This would be an issue
	// because the HTTP status code that should be returned here is 400 Bad Request.
	//
	// Once the satellite version used for integration testing has been updated,
	// this block should be removed.
	if err != nil && strings.Contains(err.Error(), "invalid retention mode 2, expected 1 (compliance)") {
		return objectlock.ErrUnknownWORMModeDirective
	}

	return ConvertError(err, bucketName, object)
}

// ConvertError translates Storj-specific err associated with object to
// MinIO/S3-specific error. It returns nil if err is nil.
func ConvertError(err error, bucketName, object string) error {
	if convertedErr := asObjectLockError(bucketName, object, err); convertedErr != nil {
		return convertedErr
	}
	if convertedErr := asDeleteObjectsError(err); convertedErr != nil {
		return convertedErr
	}

	switch {
	case err == nil:
		return nil
	case errors.Is(err, uplink.ErrBucketNameInvalid):
		return minio.BucketNameInvalid{Bucket: bucketName}
	case errors.Is(err, uplink.ErrBucketAlreadyExists):
		return minio.BucketAlreadyExists{Bucket: bucketName}
	case errors.Is(err, uplink.ErrBucketNotFound):
		return minio.BucketNotFound{Bucket: bucketName}
	case errors.Is(err, uplink.ErrBucketNotEmpty):
		return minio.BucketNotEmpty{Bucket: bucketName}
	case errors.Is(err, uplink.ErrObjectKeyInvalid):
		return minio.ObjectNameInvalid{Bucket: bucketName, Object: object}
	case errors.Is(err, uplink.ErrObjectNotFound):
		return minio.ObjectNotFound{Bucket: bucketName, Object: object}
	case errors.Is(err, uplink.ErrBandwidthLimitExceeded):
		return ErrBandwidthLimitExceeded
	case errors.Is(err, uplink.ErrStorageLimitExceeded):
		return ErrStorageLimitExceeded
	case errors.Is(err, uplink.ErrSegmentsLimitExceeded):
		return ErrSegmentsLimitExceeded
	case errors.Is(err, uplink.ErrPermissionDenied):
		return minio.PrefixAccessDenied{Bucket: bucketName, Object: object}
	case errors.Is(err, uplink.ErrTooManyRequests):
		return ErrSlowDown
	case errors.Is(err, versioned.ErrMethodNotAllowed):
		return minio.MethodNotAllowed{Bucket: bucketName, Object: object}
	case errors.Is(err, io.ErrUnexpectedEOF):
		return minio.IncompleteBody{Bucket: bucketName, Object: object}
	case errors.Is(err, versioned.ErrFailedPrecondition):
		return ErrFailedPrecondition
	case errors.Is(err, versioned.ErrUnimplemented):
		return minio.NotImplemented{Message: err.Error()}
	case errors.Is(err, syscall.ECONNRESET):
		// This specific error happens when the satellite shuts down or is
		// extremely busy. An event like this might happen during, e.g.
		// production deployment.
		//
		// TODO(artur): what other information we should add to the monkit
		// event?
		mon.Event("connection reset by peer")
		// There's not much we can do at this point but report that we won't
		// continue with this request.
		return minio.OperationTimedOut{}
	default:
		return err
	}
}

func asObjectLockError(bucketName, object string, err error) error {
	switch {
	case noLockEnabledErr(err):
		return ErrBucketObjectLockNotEnabled
	case errors.Is(err, bucket.ErrBucketInvalidStateObjectLock):
		return ErrBucketInvalidStateObjectLock
	case errors.Is(err, bucket.ErrBucketInvalidObjectLockConfig):
		return ErrBucketInvalidObjectLockConfig
	case errors.Is(err, versioned.ErrObjectLockInvalidObjectState):
		return minio.MethodNotAllowed{Bucket: bucketName, Object: object}
	case errors.Is(err, versioned.ErrRetentionNotFound):
		return ErrRetentionNotFound
	case errors.Is(err, versioned.ErrObjectProtected):
		return ErrObjectProtected
	case errors.Is(err, privateProject.ErrLockNotEnabled):
		return minio.NotImplemented{Message: "Object Lock feature is not enabled"}
	default:
		return nil
	}
}

func noLockEnabledErr(err error) bool {
	return errors.Is(err, bucket.ErrBucketNoLock) ||
		errors.Is(err, versioned.ErrNoObjectLockConfiguration)
}

func asDeleteObjectsError(err error) error {
	switch {
	case errors.Is(err, versioned.ErrObjectKeyMissing):
		return ErrObjectKeyMissing
	case errors.Is(err, versioned.ErrObjectKeyTooLong):
		return ErrObjectKeyTooLong
	case errors.Is(err, versioned.ErrObjectVersionInvalid):
		return ErrObjectVersionInvalid
	case errors.Is(err, versioned.ErrDeleteObjectsNoItems):
		return ErrDeleteObjectsNoItems
	case errors.Is(err, versioned.ErrDeleteObjectsTooManyItems):
		return ErrDeleteObjectsTooManyItems
	}
	return nil
}

func projectFromContext(ctx context.Context, bucket, object string) (*uplink.Project, error) {
	pr, ok := GetUplinkProject(ctx)
	if !ok {
		return nil, ConvertError(ErrNoUplinkProject.New("not found"), bucket, object)
	}
	return pr, nil
}

func upsertObjectMetadata(metadata map[string]string, existingMetadata uplink.CustomMetadata) {
	// if X-Amz-Metadata-Directive header is provided and is set to "REPLACE",
	// then srcInfo.UserDefined will contain new metadata from the copy request.
	// If the directive is "COPY", or undefined, the source object's metadata
	// will be contained in srcInfo.UserDefined instead. This is done by the
	// caller handler in minio.

	// if X-Amz-Tagging-Directive is set to "REPLACE", we need to set the copied
	// object's tags to the tags provided in the copy request. If the directive
	// is "COPY", or undefined, copy over any existing tags from the source
	// object.
	if td, ok := metadata[xhttp.AmzTagDirective]; ok && td == "REPLACE" {
		metadata["s3:tags"] = metadata[xhttp.AmzObjectTagging]
	} else if tags, ok := existingMetadata["s3:tags"]; ok {
		metadata["s3:tags"] = tags
	}

	delete(metadata, xhttp.AmzObjectTagging)
	delete(metadata, xhttp.AmzTagDirective)

	// if X-Amz-Metadata-Directive header is set to "REPLACE", then
	// srcInfo.UserDefined will be missing s3:etag, so make sure it's copied.
	metadata["s3:etag"] = existingMetadata["s3:etag"]
}

// checkBucketError will stat the bucket if the provided error is not nil, in
// order to check if the proper error to return is really a bucket not found
// error. If the satellite has already returned this error, do not make an
// additional check.
func checkBucketError(ctx context.Context, project *uplink.Project, bucketName, object string, err error) error {
	if err != nil && !errors.Is(err, uplink.ErrBucketNotFound) {
		_, statErr := project.StatBucket(ctx, bucketName)
		if statErr != nil {
			return ConvertError(statErr, bucketName, object)
		}
	}
	return err
}

func parseRetentionMode(mode objectlock.RetMode) (storj.RetentionMode, error) {
	switch {
	case strings.EqualFold(string(mode), string(objectlock.RetCompliance)):
		return storj.ComplianceMode, nil
	case strings.EqualFold(string(mode), string(objectlock.RetGovernance)):
		return storj.GovernanceMode, nil
	default:
		return storj.NoRetention, objectlock.ErrUnknownWORMModeDirective
	}
}

func toMinioRetentionMode(mode storj.RetentionMode) objectlock.RetMode {
	switch mode {
	case storj.ComplianceMode:
		return objectlock.RetCompliance
	case storj.GovernanceMode:
		return objectlock.RetGovernance
	default:
		return objectlock.RetMode(fmt.Sprintf("Unknown (%d)", mode))
	}
}

func parseLegalHoldStatus(status objectlock.LegalHoldStatus) (bool, error) {
	if status != objectlock.LegalHoldOff && status != objectlock.LegalHoldOn {
		return false, objectlock.ErrMalformedXML
	}

	return status == objectlock.LegalHoldOn, nil
}

func toMinioLegalHoldStatus(enabled bool) objectlock.LegalHoldStatus {
	if enabled {
		return objectlock.LegalHoldOn
	} else {
		return objectlock.LegalHoldOff
	}
}

func toMinioObjectLockConfiguration(configuration *metaclient.BucketObjectLockConfiguration) objectlock.Config {
	config := objectlock.Config{
		ObjectLockEnabled: "Enabled",
	}

	if configuration.DefaultRetention == nil || configuration.DefaultRetention.Mode == storj.NoRetention {
		return config
	}

	defaultRetention := objectlock.DefaultRetention{
		Mode: toMinioRetentionMode(configuration.DefaultRetention.Mode),
	}
	if configuration.DefaultRetention.Days > 0 {
		defaultRetention.Days = &configuration.DefaultRetention.Days
	}
	if configuration.DefaultRetention.Years > 0 {
		defaultRetention.Years = &configuration.DefaultRetention.Years
	}
	config.Rule = &struct {
		DefaultRetention objectlock.DefaultRetention `xml:"DefaultRetention"`
	}{
		DefaultRetention: defaultRetention,
	}

	return config
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

func minioVersionedObjectInfo(bucket, etag string, object *versioned.VersionedObject) minio.ObjectInfo {
	if object == nil {
		object = &versioned.VersionedObject{}
	}

	if object.Retention != nil && object.Retention.Mode != storj.NoRetention {
		if object.Custom == nil {
			object.Custom = uplink.CustomMetadata{}
		}
		lockMode := objectlock.RetCompliance
		if object.Retention.Mode == storj.GovernanceMode {
			lockMode = objectlock.RetGovernance
		}
		object.Custom[strings.ToLower(objectlock.AmzObjectLockMode)] = string(lockMode)
		object.Custom[strings.ToLower(objectlock.AmzObjectLockRetainUntilDate)] = object.Retention.RetainUntil.Format(time.RFC3339)
	}

	if object.LegalHold != nil {
		if object.Custom == nil {
			object.Custom = uplink.CustomMetadata{}
		}
		legalHoldStatus := objectlock.LegalHoldOff
		if *object.LegalHold {
			legalHoldStatus = objectlock.LegalHoldOn
		}
		object.Custom[strings.ToLower(objectlock.AmzObjectLockLegalHold)] = string(legalHoldStatus)
	}

	minioObject := minioObjectInfo(bucket, etag, &object.Object)

	minioObject.VersionID = encodeVersionID(object.Version)
	minioObject.DeleteMarker = object.IsDeleteMarker
	minioObject.IsLatest = object.IsLatest

	return minioObject
}

func parseTTL(userDefined map[string]string) (time.Time, error) {
	for _, alias := range objectTTLKeyAliases {
		date, ok := userDefined[alias]
		if !ok {
			continue
		}
		switch {
		case date == "none":
			return time.Time{}, nil
		case date == "":
			return time.Time{}, nil
		case strings.HasPrefix(date, "+"):
			d, err := time.ParseDuration(date)
			return time.Now().Add(d), err
		default:
			return time.Parse(time.RFC3339, date)
		}
	}

	return time.Time{}, nil
}

// verifyIfNoneMatch verifies the requested value is correct.
// The only supported value is "*".
// Note for S3 compatibility we return 501 instead of 400 for anything unknown.
// This function should be kept in sync with metabase.(IfNoneMatch).Verify() if
// that changes.
func verifyIfNoneMatch(val []string) error {
	if len(val) == 0 {
		return nil
	}
	if len(val) > 1 || val[0] != "*" {
		return minio.NotImplemented{Message: "If-None-Match only supports a single value of '*'"}
	}
	return nil
}

// limitResultsWithAlignment is like limitResults, but it aligns with
// paging limitations on the satellite side.
func limitResultsWithAlignment(limit int, configuredLimit int) int {
	if limit < 0 || limit >= configuredLimit {
		// Return max results with a buffer to gather the continuation
		// token to avoid paging problems until we have a method in
		// libuplink to get more info about page boundaries.
		if configuredLimit-1 == 0 {
			return 1
		}
		return configuredLimit - 1
	}
	return limit
}

// limitResults returns limit restricted to configuredLimit. It will
// also return the highest limit possible if limit is negative, which is
// compatible with AWS S3.
func limitResults(limit int, configuredLimit int) int {
	if limit < 0 || limit >= configuredLimit {
		return configuredLimit
	}
	return limit
}

type listingKind int

const (
	fastListing listingKind = iota
	singleListing
	exhaustiveListing
)

func (kind listingKind) String() string {
	switch kind {
	case fastListing:
		return "fast"
	case singleListing:
		return "single"
	case exhaustiveListing:
		return "exhaustive"
	default:
		return "unknown"
	}
}

// monItrNext is a helper function to track the number of items returned
// by the object iterator.
func monItrNext(it *uplink.ObjectIterator, kind listingKind) bool {
	monLstNext(1, kind)
	return it.Next()
}

// monLstNext is a helper function to track the number of items returned
// by the listing call.
func monLstNext(limit int64, kind listingKind) {
	mon.Counter("ListObjects_items", monkit.NewSeriesTag("kind", kind.String())).Inc(limit)
}

func decodeVersionID(versionID string) ([]byte, error) {
	if versionID == "" {
		return nil, nil
	}

	version, err := hex.DecodeString(versionID)
	return version, err
}

func encodeVersionID(version []byte) string {
	return hex.EncodeToString(version)
}
