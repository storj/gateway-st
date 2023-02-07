// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"bytes"
	"context"
	"errors"
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
	minio "storj.io/minio/cmd"
	"storj.io/minio/cmd/config/storageclass"
	xhttp "storj.io/minio/cmd/http"
	"storj.io/minio/pkg/auth"
	"storj.io/minio/pkg/hash"
	"storj.io/minio/pkg/madmin"
	"storj.io/private/version"
	"storj.io/uplink"
)

var (
	mon = monkit.Package()

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
		Message:    "You have reached your Storj project bandwidth limit on the Satellite.",
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
		Message:    "You have reached your Storj project storage limit on the Satellite.",
	}

	// ErrSegmentsLimitExceeded is a custom error for when a user has reached their
	// Satellite segment limit.
	ErrSegmentsLimitExceeded = miniogo.ErrorResponse{
		Code:       "SegmentsLimitExceeded",
		StatusCode: http.StatusForbidden,
		Message:    "You have reached your Storj project segment limit on the Satellite.",
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

func (layer *gatewayLayer) StorageInfo(ctx context.Context) (minio.StorageInfo, []error) {
	return minio.StorageInfo{
		Backend: madmin.BackendInfo{
			Type:          madmin.Gateway,
			GatewayOnline: true,
		},
	}, nil
}

func (layer *gatewayLayer) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) (err error) {
	defer mon.Task()(&ctx)(&err)

	if err := validateBucket(ctx, bucket); err != nil {
		return minio.BucketNameInvalid{Bucket: bucket}
	}

	project, err := projectFromContext(ctx, bucket, "")
	if err != nil {
		return err
	}

	_, err = project.CreateBucket(ctx, bucket)

	return ConvertError(err, bucket, "")
}

func (layer *gatewayLayer) GetBucketInfo(ctx context.Context, bucketName string) (bucketInfo minio.BucketInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := validateBucket(ctx, bucketName); err != nil {
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

	if err := validateBucket(ctx, bucket); err != nil {
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
		// Check if the bucket contains any non-pending objects. If it doesn't,
		// this would mean there were initiated non-committed and non-aborted
		// multipart uploads. Other S3 implementations allow deletion of such
		// buckets, but with uplink, we need to explicitly force bucket
		// deletion.
		it := project.ListObjects(ctx, bucket, nil)
		if !it.Next() && it.Err() == nil {
			_, err = project.DeleteBucketWithObjects(ctx, bucket)
			return ConvertError(err, bucket, "")
		}
	}

	return ConvertError(err, bucket, "")
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

	list := project.ListObjects(ctx, bucket, &uplink.ListObjectsOptions{
		Prefix:    prefix,
		Cursor:    strings.TrimPrefix(after, prefix),
		Recursive: recursive,
		System:    true,
		Custom:    layer.compatibilityConfig.IncludeCustomMetadataListing,
	})

	limit := limitResults(maxKeys, layer.compatibilityConfig.MaxKeysLimit)

	var more bool
	for more = list.Next(); limit > 0 && more; more = list.Next() {
		item := list.Item()

		limit--

		if item.IsPrefix {
			prefixes = append(prefixes, item.Key)
		} else {
			objects = append(objects, minioObjectInfo(bucket, "", item))
		}

		nextContinuationToken = item.Key
	}
	if list.Err() != nil {
		return nil, nil, "", list.Err()
	}

	if !more {
		nextContinuationToken = ""
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

	if limit > 0 && after == "" {
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

	if limit > 0 && delimiter == "/" && (after == "" || after == prefix) {
		p := prefix + "/"

		list := project.ListObjects(ctx, bucket, &uplink.ListObjectsOptions{
			Prefix:    p,
			Recursive: true,
			// Limit: 1, would be nice to set here
		})

		if list.Next() {
			prefixes = append(prefixes, p)
		}
		if list.Err() != nil {
			return nil, nil, "", list.Err()
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

	for i := 0; list.Next(); i++ {
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

// listObjectsGeneral lists bucket trying to best-effort conform to AWS S3's
// listing APIs behavior.
//
// It tries to list the bucket in three, mutually-exclusive ways:
//
//  1. Fast;
//  2. Optimized for non-terminated prefix;
//  3. Exhaustive.
//
// If prefix is empty or terminated with a forward slash and delimiter is empty
// or a forward slash, it will call listObjectsFast to list items the fastest
// way possible. For requests that come with prefix non-terminated with a
// forward slash, it will call listObjectsSingle to perform a specific
// optimization for this type of listing. Finally, for all other requests, e.g.,
// these that come with a non-forward slash delimiter, it will perform an
// exhaustive listing calling listObjectsExhaustive as it's the only way to make
// such bucket listing using libuplink.
//
// Optimization for non-terminated prefixes relies on the fact that much of
// S3-compatible software uses a call to ListObjects(V2) with a non-terminated
// prefix to check whether a key that is equal to this prefix exists. It only
// cares about the first result. Since we can't do such listing with libuplink
// directly and making an exhaustive listing for this kind of query has terrible
// performance, we only check if an object exists. We additionally check if
// there's a prefix object for this prefix because it doesn't cost much more. We
// return these items in a list alone, setting continuation token to one of
// them, signaling that there might be more (S3 only guarantees to return not
// too many items and not as many as possible). If we didn't find anything, we
// fall back to the exhaustive listing.
//
// The only S3-incompatible thing that listObjectsGeneral represents is that for
// fast listing, it will not return items lexicographically ordered, but it is a
// burden we must all bear.
//
// If layer.compatibilityConfig.FullyCompatibleListing is true, it will always
// list exhaustively to achieve full S3 compatibility. Use at your own risk.
func (layer *gatewayLayer) listObjectsGeneral(
	ctx context.Context,
	project *uplink.Project,
	bucket, prefix, continuationToken, delimiter string,
	maxKeys int,
	startAfter string,
) (_ minio.ListObjectsV2Info, err error) {
	defer mon.Task()(&ctx)(&err)

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

	if err := validateBucket(ctx, bucket); err != nil {
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

	if err := validateBucket(ctx, bucket); err != nil {
		return minio.ListObjectsV2Info{}, minio.BucketNameInvalid{Bucket: bucket}
	}

	project, err := projectFromContext(ctx, bucket, "")
	if err != nil {
		return minio.ListObjectsV2Info{}, err
	}

	result, err := layer.listObjectsGeneral(ctx, project, bucket, prefix, continuationToken, delimiter, maxKeys, startAfter)

	return result, ConvertError(err, bucket, "")
}

func (layer *gatewayLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (reader *minio.GetObjectReader, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := validateBucket(ctx, bucket); err != nil {
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

	download, err := project.DownloadObject(ctx, bucket, object, downloadOpts)
	if err != nil {
		return nil, ConvertError(err, bucket, object)
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
		// note: we do not specify ResourceSize here because that would require
		// an additional stat call to get the ContentLength from metadata.
		return nil, minio.InvalidRange{OffsetBegin: rs.Start, OffsetEnd: rs.End}
	}
}

func (layer *gatewayLayer) GetObjectInfo(ctx context.Context, bucket, objectPath string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := validateBucket(ctx, bucket); err != nil {
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

	return minioObjectInfo(bucket, "", object), nil
}

func (layer *gatewayLayer) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := validateBucket(ctx, bucket); err != nil {
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

	// TODO this should be removed and implemented on satellite side
	defer func() {
		err = checkBucketError(ctx, project, bucket, object, err)
	}()

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
	upload, err := project.UploadObject(ctx, bucket, object, &uplink.UploadOptions{
		Expires: e,
	})
	if err != nil {
		return minio.ObjectInfo{}, ConvertError(err, bucket, object)
	}

	_, err = io.Copy(upload, data)
	if err != nil {
		abortErr := upload.Abort()
		err = errs.Combine(err, abortErr)
		return minio.ObjectInfo{}, ConvertError(err, bucket, object)
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

	return minioObjectInfo(bucket, etag, upload.Info()), nil
}

func (layer *gatewayLayer) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, destOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

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

	if err := validateBucket(ctx, srcBucket); err != nil {
		return minio.ObjectInfo{}, minio.BucketNameInvalid{Bucket: srcBucket}
	}
	if err := validateBucket(ctx, destBucket); err != nil {
		return minio.ObjectInfo{}, minio.BucketNameInvalid{Bucket: destBucket}
	}

	project, err := projectFromContext(ctx, srcBucket, srcObject)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	if srcAndDestSame {
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

	object, err := project.CopyObject(ctx, srcBucket, srcObject, destBucket, destObject, nil)
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

	return minioObjectInfo(destBucket, "", object), nil
}

func (layer *gatewayLayer) DeleteObject(ctx context.Context, bucket, objectPath string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := validateBucket(ctx, bucket); err != nil {
		return minio.ObjectInfo{}, minio.BucketNameInvalid{Bucket: bucket}
	}

	project, err := projectFromContext(ctx, bucket, objectPath)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	// TODO this should be removed and implemented on satellite side.
	//
	// This call needs to occur prior to the DeleteObject call below, because
	// project.DeleteObject will return a nil error for a missing bucket. To
	// maintain consistency, we need to manually check if the bucket exists.
	_, err = project.StatBucket(ctx, bucket)
	if err != nil {
		return minio.ObjectInfo{}, ConvertError(err, bucket, objectPath)
	}

	object, err := project.DeleteObject(ctx, bucket, objectPath)
	if err != nil {
		return minio.ObjectInfo{}, ConvertError(err, bucket, objectPath)
	}

	return minioObjectInfo(bucket, "", object), nil
}

func (layer *gatewayLayer) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) ([]minio.DeletedObject, []error) {
	// TODO: implement multiple object deletion in libuplink API
	deleted, errs := make([]minio.DeletedObject, len(objects)), make([]error, len(objects))
	for i, object := range objects {
		_, deleteErr := layer.DeleteObject(ctx, bucket, object.ObjectName, opts)
		if deleteErr != nil && !errors.As(deleteErr, &minio.ObjectNotFound{}) {
			errs[i] = ConvertError(deleteErr, bucket, object.ObjectName)
			continue
		}
		deleted[i].ObjectName = object.ObjectName
	}
	return deleted, errs
}

func (layer *gatewayLayer) IsTaggingSupported() bool {
	return true
}

func (layer *gatewayLayer) PutObjectTags(ctx context.Context, bucket, objectPath string, tags string, opts minio.ObjectOptions) (_ minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	if err := validateBucket(ctx, bucket); err != nil {
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

	if err := validateBucket(ctx, bucket); err != nil {
		return nil, minio.BucketNameInvalid{Bucket: bucket}
	}

	project, err := projectFromContext(ctx, bucket, objectPath)
	if err != nil {
		return nil, err
	}

	object, err := project.StatObject(ctx, bucket, objectPath)
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

	if err := validateBucket(ctx, bucket); err != nil {
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

// ConvertError translates Storj-specific err associated with object to
// MinIO/S3-specific error. It returns nil if err is nil.
func ConvertError(err error, bucket, object string) error {
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
		return ErrBandwidthLimitExceeded
	case errors.Is(err, uplink.ErrStorageLimitExceeded):
		return ErrStorageLimitExceeded
	case errors.Is(err, uplink.ErrSegmentsLimitExceeded):
		return ErrSegmentsLimitExceeded
	case errors.Is(err, uplink.ErrPermissionDenied):
		return minio.PrefixAccessDenied{Bucket: bucket, Object: object}
	case errors.Is(err, uplink.ErrTooManyRequests):
		return ErrSlowDown
	case errors.Is(err, io.ErrUnexpectedEOF):
		return minio.IncompleteBody{Bucket: bucket, Object: object}
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

// limitResults returns limit restricted to configuredLimit, aligned with paging
// limitations on the satellite side. It will also return the highest limit
// possible if limit is negative. If limit is zero, no keys are returned, which
// is compatible with AWS S3.
func limitResults(limit int, configuredLimit int) int {
	if limit < 0 || limit >= configuredLimit {
		// Return max results with a buffer to gather the continuation token to
		// avoid paging problems until we have a method in libuplink to get more
		// info about page boundaries.
		if configuredLimit-1 == 0 {
			return 1
		}
		return configuredLimit - 1
	}
	return limit
}
