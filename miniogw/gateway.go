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
	"sort"
	"strings"
	"syscall"

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

	// ErrTooManyItemsToList indicates that ListObjects/ListObjectsV2 failed
	// because of too many items to list for gateway-side filtering using an
	// arbitrary delimiter and/or prefix.
	ErrTooManyItemsToList = minio.NotImplemented{
		API: "ListObjects(V2): listing too many items for gateway-side filtering using arbitrary delimiter/prefix",
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
		if layer.compatibilityConfig.MaxKeysLimit-1 == 0 {
			return 1
		}
		return layer.compatibilityConfig.MaxKeysLimit - 1
	}
	return maxKeys
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

	limit := layer.limitMaxKeys(maxKeys)

	for limit > 0 && list.Next() {
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

	more := list.Next()
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

	if after == "" {
		object, err := project.StatObject(ctx, bucket, prefix)
		if err != nil {
			if !errors.Is(err, uplink.ErrObjectNotFound) {
				return nil, nil, "", err
			}
		} else {
			objects = append(objects, minioObjectInfo(bucket, "", object))

			if layer.limitMaxKeys(maxKeys) == 1 {
				return prefixes, objects, object.Key, nil
			}
		}
	}

	if delimiter == "/" && (after == "" || after == prefix) {
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

	limit := layer.limitMaxKeys(maxKeys)
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

	if strings.HasSuffix(prefix, "/") {
		listPrefix = prefix
	}

	list := project.ListObjects(ctx, bucket, &uplink.ListObjectsOptions{
		Prefix:    listPrefix,
		Recursive: delimiter != "/" || (strings.Contains(prefix, "/") && !strings.HasSuffix(prefix, "/")),
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

	return result, convertError(err, bucket, "")
}

// ListObjectsV2 calls listObjectsGeneral.
func (layer *gatewayLayer) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (_ minio.ListObjectsV2Info, err error) {
	defer mon.Task()(&ctx)(&err)

	project, err := projectFromContext(ctx, bucket, "")
	if err != nil {
		return minio.ListObjectsV2Info{}, err
	}

	result, err := layer.listObjectsGeneral(ctx, project, bucket, prefix, continuationToken, delimiter, maxKeys, startAfter)

	return result, convertError(err, bucket, "")
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

	if len(object) > memory.KiB.Int() { // https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
		return minio.ObjectInfo{}, minio.ObjectNameTooLong{Bucket: bucket, Object: object}
	}

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

	srcAndDestSame := srcBucket == destBucket && srcObject == destObject

	if layer.compatibilityConfig.DisableCopyObject && !srcAndDestSame {
		// Note: In production Gateway-MT, we want to return Not Implemented until we implement server-side copy
		return minio.ObjectInfo{}, minio.NotImplemented{API: "CopyObject"}
	}

	if len(destObject) > memory.KiB.Int() { // https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
		return minio.ObjectInfo{}, minio.ObjectNameTooLong{Bucket: destBucket, Object: destObject}
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

	if srcAndDestSame {
		// Source and destination are the same. Do nothing, apart from ensuring
		// metadata is updated. Tools like rclone sync use CopyObject with the
		// same source and destination as a way of updating existing metadata
		// like last modified date/time, as there's no S3 endpoint for
		// manipulating existing object's metadata.
		info, err := project.StatObject(ctx, srcBucket, srcObject)
		if err != nil {
			return minio.ObjectInfo{}, convertError(err, srcBucket, srcObject)
		}

		upsertObjectMetadata(srcInfo.UserDefined, info.Custom)

		err = project.UpdateObjectMetadata(ctx, srcBucket, srcObject, srcInfo.UserDefined, nil)
		if err != nil {
			return minio.ObjectInfo{}, convertError(err, srcBucket, srcObject)
		}

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

	upsertObjectMetadata(srcInfo.UserDefined, info.Custom)

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
	//
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
