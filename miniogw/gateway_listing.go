// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/spacemonkeygo/monkit/v3"

	"storj.io/eventkit"
	minio "storj.io/minio/cmd"
	"storj.io/uplink"
	"storj.io/uplink/private/access"
	versioned "storj.io/uplink/private/object"
)

// ErrTooManyItemsToList indicates that ListObjects/ListObjectsV2 failed
// because of too many items to list for gateway-side filtering using an
// arbitrary delimiter and/or prefix.
var ErrTooManyItemsToList = minio.NotImplemented{
	Message: "ListObjects(V2): listing too many items for gateway-side filtering using arbitrary delimiter/prefix",
}

// ErrVersionIDMarkerWithoutKeyMarker is returned for ListObjectVersions
// when a version-id marker has been specified without a key marker.
var ErrVersionIDMarkerWithoutKeyMarker = func(bucketName string) miniogo.ErrorResponse {
	return miniogo.ErrorResponse{
		Code:       "InvalidArgument",
		Message:    "A version-id marker cannot be specified without a key marker.",
		BucketName: bucketName,
		StatusCode: http.StatusBadRequest,
	}
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
	monLstNext(int64(len(list)), fastListing)

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

func ctxCredentialsToTags(ctx context.Context) []eventkit.Tag {
	var tags []eventkit.Tag
	credentials, err := credentialsFromContext(ctx)
	if err != nil {
		return tags
	}
	if credentials.PublicProjectID != "" {
		tags = append(tags, eventkit.String("public_project_id", credentials.PublicProjectID))
	}
	ag := credentials.Access
	if ag != nil {
		tags = append(tags, eventkit.Bytes("macaroon_head", access.APIKey(ag).Head()))
		tags = append(tags, eventkit.String("satellite_address", ag.SatelliteAddress()))
	}
	return tags
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

	var fetchedCount int64
	now := time.Now()
	defer func() {
		tags := ctxCredentialsToTags(ctx)
		tags = append(tags, eventkit.Int64("fetched_count", fetchedCount))
		tags = append(tags, eventkit.Duration("duration", time.Since(now)))
		tags = append(tags, eventkit.Bool("prefix_optimization_applied", listPrefix != ""))
		tags = append(tags, eventkit.Bool("well_known_prefix", strings.HasSuffix(prefix, "/")))
		tags = append(tags, eventkit.Bool("well_known_delimiter", delimiter == "/" || delimiter == ""))
		ek.Event("ListObjects_exhaustive", tags...)
	}()

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
		fetchedCount++

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

	if maxKeys == 0 {
		return minio.ListObjectVersionsInfo{}, nil
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
func monLstNext(delta int64, kind listingKind) {
	mon.Counter("ListObjects_items", monkit.NewSeriesTag("kind", kind.String())).Inc(delta)
}
