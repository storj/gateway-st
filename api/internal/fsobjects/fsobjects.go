// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

// Package fsobjects implements an object layer on a local directory. It's for validating the api
// package without a satellite, so it follows miniogw's and the satellite's behavior where that
// differs from S3's. It doesn't support notifications.
package fsobjects

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"hash"
	"io"
	"io/fs"
	"maps"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"storj.io/gateway/api/apierr"
	"storj.io/gateway/api/objectlayer"
)

const (
	// maxKeysLimit and maxUploadsLimit are miniogw's default listing limits.
	maxKeysLimit    = 1000
	maxUploadsLimit = 1000
	// minPartSize is miniogw's default minimum size of a part other than the last one.
	minPartSize = 5 << 20
	// maxKeyLength is the longest object key, in bytes.
	maxKeyLength = 1024

	tagsKey = "s3:tags"

	amzObjectLockMode            = "X-Amz-Object-Lock-Mode"
	amzObjectLockRetainUntilDate = "X-Amz-Object-Lock-Retain-Until-Date"
	amzObjectLockLegalHold       = "X-Amz-Object-Lock-Legal-Hold"
	amzObjectTagging             = "X-Amz-Tagging"
	amzTagDirective              = "X-Amz-Tagging-Directive"
	amzStorageClass              = "X-Amz-Storage-Class"
	storageClassNormal           = "STANDARD"
)

// The object lock errors of miniogw.
var (
	errObjectLockNotEnabled = apierr.Response{
		Code:           "InvalidRequest",
		Description:    "Bucket is missing Object Lock Configuration",
		HTTPStatusCode: http.StatusBadRequest,
	}
	errInvalidBucketStateObjectLock = apierr.Response{
		Code:           "InvalidBucketState",
		Description:    "Object lock requires bucket versioning to be enabled",
		HTTPStatusCode: http.StatusConflict,
	}
	errRetentionNotFound = apierr.Response{
		Code:           "NoSuchObjectLockConfiguration",
		Description:    "Object is missing retention configuration",
		HTTPStatusCode: http.StatusNotFound,
	}
	errObjectProtected = apierr.Response{
		Code:           "AccessDenied",
		Description:    "Access Denied because object protected by object lock",
		HTTPStatusCode: http.StatusForbidden,
	}
)

// Layer is an object layer that stores buckets as directories under a root directory.
//
// The layout is:
//
//	<root>/<bucket>/bucket.json                   bucket metadata
//	<root>/<bucket>/objects/<hash>.json           metadata of a key's versions, <hash> is its SHA-256
//	<root>/<bucket>/objects/<hash>.<version>.data data of a version
//	<root>/<bucket>/uploads/<id>/upload.json      multipart upload metadata
//	<root>/<bucket>/uploads/<id>/<random>.data    part data
//	<root>/<bucket>/tmp/                          data being written
//
// Data is written outside of the lock and renamed into place under it, so a slow upload doesn't
// block other requests. Readers keep the file open, so an overwrite doesn't affect them.
type Layer struct {
	root string

	// mu guards all metadata. A single lock is enough for test workloads.
	mu sync.Mutex
}

// New returns a Layer that stores data in root, which must exist.
func New(root string) *Layer {
	return &Layer{root: root}
}

var _ objectlayer.ObjectLayer = (*Layer)(nil)

type bucketMeta struct {
	Created time.Time
	// Tags is the XML encoding of the bucket's tags, or empty if it has none.
	Tags []byte `json:",omitempty"`
	// Versioning is empty if versioning was never enabled. Like in Storj, a bucket can't become
	// unversioned again.
	Versioning objectlayer.VersioningState `json:",omitempty"`
	// ObjectLock is whether object lock is enabled, which keeps versioning enabled.
	ObjectLock bool `json:",omitempty"`
	// DefaultRetention is the retention of new versions that don't specify one.
	DefaultRetention *objectlayer.DefaultRetention `json:",omitempty"`
}

// objectMeta is the metadata of a key's versions.
type objectMeta struct {
	Key string
	// Versions are ordered from the newest.
	Versions []versionMeta
}

type versionMeta struct {
	// VersionID is new for every write. Like in Storj, a version has a version ID even if it was
	// written while versioning wasn't enabled.
	VersionID string
	// Versioned is whether the version was written while versioning was enabled. Like in Storj,
	// other writes replace the key's unversioned version rather than adding one.
	Versioned    bool `json:",omitempty"`
	DeleteMarker bool `json:",omitempty"`
	Size         int64
	ETag         string
	ModTime      time.Time
	UserDefined  map[string]string
	// Data is the name of the data file in the objects directory, empty for a delete marker.
	Data      string            `json:",omitempty"`
	Retention *versionRetention `json:",omitempty"`
	// LegalHold is set for the versions in a bucket with object lock enabled.
	LegalHold *bool `json:",omitempty"`
}

type versionRetention struct {
	Mode        objectlayer.RetentionMode
	RetainUntil time.Time
}

type uploadMeta struct {
	Key         string
	Initiated   time.Time
	UserDefined map[string]string
	Retention   *versionRetention `json:",omitempty"`
	LegalHold   *bool             `json:",omitempty"`
	Parts       map[int]partMeta
}

type partMeta struct {
	Size    int64
	ETag    string
	ModTime time.Time
	// Data is the name of the data file in the upload directory.
	Data string
}

func (l *Layer) bucketDir(bucket string) string { return filepath.Join(l.root, bucket) }

func (l *Layer) objectsDir(bucket string) string { return filepath.Join(l.root, bucket, "objects") }

func (l *Layer) uploadDir(bucket, uploadID string) string {
	return filepath.Join(l.root, bucket, "uploads", uploadID)
}

func objectHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func randomID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// writeJSON writes v to path atomically.
func writeJSON(path string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := path + "." + randomID() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// validateBucket checks bucket names like miniogw.ValidateBucket.
func validateBucket(bucket string) error {
	invalid := objectlayer.BucketNameInvalid{Bucket: bucket}
	if len(bucket) < 3 || len(bucket) > 63 {
		return invalid
	}
	for label := range strings.SplitSeq(bucket, ".") {
		if label == "" {
			return invalid
		}
		for i := range len(label) {
			c := label[i]
			alnum := 'a' <= c && c <= 'z' || '0' <= c && c <= '9'
			if !alnum && (i == 0 || i == len(label)-1 || c != '-') {
				return invalid
			}
		}
	}
	isIP := true
	for label := range strings.SplitSeq(bucket, ".") {
		if n, err := strconv.Atoi(label); err != nil || n > 255 || strconv.Itoa(n) != label {
			isIP = false
		}
	}
	if isIP && strings.Count(bucket, ".") == 3 {
		return invalid
	}
	return nil
}

// readBucket returns the bucket's metadata. The caller must hold l.mu.
func (l *Layer) readBucket(bucket string) (bucketMeta, error) {
	if err := validateBucket(bucket); err != nil {
		return bucketMeta{}, err
	}
	var meta bucketMeta
	err := readJSON(filepath.Join(l.bucketDir(bucket), "bucket.json"), &meta)
	if errors.Is(err, fs.ErrNotExist) {
		return bucketMeta{}, objectlayer.BucketNotFound{Bucket: bucket}
	}
	return meta, err
}

func (l *Layer) checkBucket(bucket string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err := l.readBucket(bucket)
	return err
}

// readObject returns the object's metadata. The caller must hold l.mu.
func (l *Layer) readObject(bucket, key string) (objectMeta, error) {
	if _, err := l.readBucket(bucket); err != nil {
		return objectMeta{}, err
	}
	var meta objectMeta
	err := readJSON(filepath.Join(l.objectsDir(bucket), objectHash(key)+".json"), &meta)
	if errors.Is(err, fs.ErrNotExist) {
		return objectMeta{}, objectlayer.ObjectNotFound{Bucket: bucket, Object: key}
	}
	return meta, err
}

// readObjects returns the metadata of all objects in the bucket, sorted by key. The caller must
// hold l.mu.
func (l *Layer) readObjects(bucket string) ([]objectMeta, error) {
	if _, err := l.readBucket(bucket); err != nil {
		return nil, err
	}
	paths, err := filepath.Glob(filepath.Join(l.objectsDir(bucket), "*.json"))
	if err != nil {
		return nil, err
	}
	objects := make([]objectMeta, 0, len(paths))
	for _, path := range paths {
		var meta objectMeta
		if err := readJSON(path, &meta); err != nil {
			return nil, err
		}
		objects = append(objects, meta)
	}
	slices.SortFunc(objects, func(a, b objectMeta) int { return strings.Compare(a.Key, b.Key) })
	return objects, nil
}

// writeObject replaces the metadata of a key's versions, or removes it if there are none. The
// caller must hold l.mu.
func (l *Layer) writeObject(bucket string, meta objectMeta) error {
	path := filepath.Join(l.objectsDir(bucket), objectHash(meta.Key)+".json")
	if len(meta.Versions) == 0 {
		return os.Remove(path)
	}
	return writeJSON(path, meta)
}

// removeVersions removes the versions for which remove returns true from meta, and their data.
func (l *Layer) removeVersions(bucket string, meta *objectMeta, remove func(versionMeta) bool) {
	meta.Versions = slices.DeleteFunc(meta.Versions, func(v versionMeta) bool {
		if !remove(v) {
			return false
		}
		if v.Data != "" {
			_ = os.Remove(filepath.Join(l.objectsDir(bucket), v.Data))
		}
		return true
	})
}

// readVersion returns the metadata of a key's versions and the index of the version versionID,
// or of the latest version if versionID is empty. The caller must hold l.mu.
//
// Like in Storj, a key whose latest version is a delete marker isn't found, and addressing a
// delete marker by its version ID isn't allowed.
func (l *Layer) readVersion(bucket, key, versionID string) (objectMeta, int, error) {
	meta, err := l.readObject(bucket, key)
	if err != nil {
		return objectMeta{}, 0, err
	}
	i := 0
	if versionID != "" {
		i = slices.IndexFunc(meta.Versions, func(v versionMeta) bool { return v.VersionID == versionID })
		if i < 0 {
			return objectMeta{}, 0, objectlayer.VersionNotFound{Bucket: bucket, Object: key, VersionID: versionID}
		}
	}
	if meta.Versions[i].DeleteMarker {
		if versionID == "" {
			return objectMeta{}, 0, objectlayer.ObjectNotFound{Bucket: bucket, Object: key}
		}
		return objectMeta{}, 0, objectlayer.MethodNotAllowed{Bucket: bucket, Object: key}
	}
	return meta, i, nil
}

// readUpload returns the upload's metadata. The caller must hold l.mu.
func (l *Layer) readUpload(bucket, key, uploadID string) (uploadMeta, error) {
	if _, err := l.readBucket(bucket); err != nil {
		return uploadMeta{}, err
	}
	invalid := objectlayer.InvalidUploadID{Bucket: bucket, Object: key, UploadID: uploadID}
	if _, err := hex.DecodeString(uploadID); err != nil || len(uploadID) != 32 {
		return uploadMeta{}, invalid
	}
	var meta uploadMeta
	err := readJSON(filepath.Join(l.uploadDir(bucket, uploadID), "upload.json"), &meta)
	if errors.Is(err, fs.ErrNotExist) || err == nil && meta.Key != key {
		return uploadMeta{}, invalid
	}
	return meta, err
}

// writeTemp writes r to a new file in the bucket's tmp directory and returns its path.
func (l *Layer) writeTemp(bucket string, r io.Reader) (path string, size int64, err error) {
	f, err := os.CreateTemp(filepath.Join(l.bucketDir(bucket), "tmp"), "*.data")
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", 0, objectlayer.BucketNotFound{Bucket: bucket}
		}
		return "", 0, err
	}
	size, err = io.Copy(f, r)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(f.Name())
		return "", 0, err
	}
	return f.Name(), size, nil
}

// commitObject moves the data file at tmp into place as the data of a new version of key.
func (l *Layer) commitObject(bucket, key, tmp string, version versionMeta, ifNoneMatch []string) (objectlayer.ObjectInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	info, err := l.addVersion(bucket, key, tmp, version, ifNoneMatch)
	if err != nil {
		_ = os.Remove(tmp)
	}
	return info, err
}

// addVersion adds version as the latest version of key, with the data file at tmp. The caller
// must hold l.mu.
func (l *Layer) addVersion(bucket, key, tmp string, version versionMeta, ifNoneMatch []string) (objectlayer.ObjectInfo, error) {
	bucketMeta, err := l.readBucket(bucket)
	if err != nil {
		return objectlayer.ObjectInfo{}, err
	}
	if err := applyObjectLock(bucketMeta, &version); err != nil {
		return objectlayer.ObjectInfo{}, err
	}
	meta, err := l.readObject(bucket, key)
	if errors.As(err, new(objectlayer.ObjectNotFound)) {
		meta, err = objectMeta{Key: key}, nil
	}
	if err != nil {
		return objectlayer.ObjectInfo{}, err
	}
	if len(ifNoneMatch) > 0 && len(meta.Versions) > 0 && !meta.Versions[0].DeleteMarker {
		return objectlayer.ObjectInfo{}, apierr.CodePreconditionFailed
	}

	version.VersionID = randomID()
	version.Versioned = bucketMeta.Versioning == objectlayer.VersioningEnabled
	version.Data = objectHash(key) + "." + version.VersionID + ".data"
	version.ModTime = time.Now().UTC()
	if err := os.Rename(tmp, filepath.Join(l.objectsDir(bucket), version.Data)); err != nil {
		return objectlayer.ObjectInfo{}, err
	}
	if !version.Versioned {
		l.removeVersions(bucket, &meta, func(v versionMeta) bool { return !v.Versioned })
	}
	meta.Versions = slices.Insert(meta.Versions, 0, version)
	if err := l.writeObject(bucket, meta); err != nil {
		return objectlayer.ObjectInfo{}, err
	}
	return objectInfo(bucket, key, version, true), nil
}

// objectInfo returns the information of a version of key. Like miniogw, the object lock settings
// are in the metadata.
func objectInfo(bucket, key string, version versionMeta, latest bool) objectlayer.ObjectInfo {
	metadata := maps.Clone(version.UserDefined)
	var contentType string
	for k, v := range metadata {
		if strings.EqualFold(k, "content-type") {
			contentType = v
			break
		}
	}
	if version.Retention != nil || version.LegalHold != nil {
		if metadata == nil {
			metadata = make(map[string]string)
		}
	}
	if r := version.Retention; r != nil {
		metadata[strings.ToLower(amzObjectLockMode)] = string(r.Mode)
		metadata[strings.ToLower(amzObjectLockRetainUntilDate)] = r.RetainUntil.Format(time.RFC3339)
	}
	if version.LegalHold != nil {
		status := objectlayer.LegalHoldOff
		if *version.LegalHold {
			status = objectlayer.LegalHoldOn
		}
		metadata[strings.ToLower(amzObjectLockLegalHold)] = string(status)
	}
	return objectlayer.ObjectInfo{
		Bucket:       bucket,
		Name:         key,
		Size:         version.Size,
		ETag:         version.ETag,
		ModTime:      version.ModTime,
		ContentType:  contentType,
		UserDefined:  metadata,
		VersionID:    version.VersionID,
		IsLatest:     latest,
		DeleteMarker: version.DeleteMarker,
	}
}

// checkPutOptions checks the options of a new object like miniogw does.
func checkPutOptions(bucket, key string, opts objectlayer.ObjectOptions) error {
	if len(key) > maxKeyLength {
		return objectlayer.ObjectNameTooLong{Bucket: bucket, Object: key}
	}
	if storageClass, ok := opts.UserDefined[amzStorageClass]; ok && storageClass != storageClassNormal {
		return objectlayer.NotImplemented{Message: "storage class"}
	}
	return checkIfNoneMatch(opts.IfNoneMatch)
}

// checkIfNoneMatch accepts only "*", like miniogw.
func checkIfNoneMatch(values []string) error {
	if len(values) == 0 || len(values) == 1 && values[0] == "*" {
		return nil
	}
	return objectlayer.NotImplemented{Message: "If-None-Match only supports a single value of '*'"}
}

// moveTagging stores the X-Amz-Tagging metadata as s3:tags, like miniogw.
func moveTagging(metadata map[string]string) map[string]string {
	metadata = maps.Clone(metadata)
	if metadata == nil {
		metadata = make(map[string]string)
	}
	if tags, ok := metadata[amzObjectTagging]; ok {
		metadata[tagsKey] = tags
		delete(metadata, amzObjectTagging)
	}
	return metadata
}

// MakeBucketWithLocation creates a bucket.
func (l *Layer) MakeBucketWithLocation(ctx context.Context, bucket string, opts objectlayer.BucketOptions) error {
	if err := validateBucket(bucket); err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	dir := l.bucketDir(bucket)
	if err := os.Mkdir(dir, 0o700); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return objectlayer.BucketAlreadyExists{Bucket: bucket}
		}
		return err
	}
	for _, sub := range []string{"objects", "uploads", "tmp"} {
		if err := os.Mkdir(filepath.Join(dir, sub), 0o700); err != nil {
			return err
		}
	}
	meta := bucketMeta{Created: time.Now().UTC()}
	if opts.LockEnabled {
		meta.Versioning, meta.ObjectLock = objectlayer.VersioningEnabled, true
	}
	return writeJSON(filepath.Join(dir, "bucket.json"), meta)
}

// GetBucketInfo returns a bucket's information.
func (l *Layer) GetBucketInfo(ctx context.Context, bucket string) (objectlayer.BucketInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	meta, err := l.readBucket(bucket)
	if err != nil {
		return objectlayer.BucketInfo{}, err
	}
	return objectlayer.BucketInfo{Name: bucket, Created: meta.Created}, nil
}

// ListBuckets lists all buckets.
func (l *Layer) ListBuckets(ctx context.Context) ([]objectlayer.BucketInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entries, err := os.ReadDir(l.root)
	if err != nil {
		return nil, err
	}
	var buckets []objectlayer.BucketInfo
	for _, entry := range entries {
		meta, err := l.readBucket(entry.Name())
		if err != nil {
			continue
		}
		buckets = append(buckets, objectlayer.BucketInfo{Name: entry.Name(), Created: meta.Created})
	}
	return buckets, nil
}

// DeleteBucket deletes a bucket. Like miniogw, a bucket that only has pending multipart uploads is
// deleted with them.
func (l *Layer) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	objects, err := l.readObjects(bucket)
	if err != nil {
		return err
	}
	if len(objects) > 0 && !forceDelete {
		return objectlayer.BucketNotEmpty{Bucket: bucket}
	}
	return os.RemoveAll(l.bucketDir(bucket))
}

// listEntry is an object or a common prefix in a listing.
type listEntry struct {
	key    string
	prefix bool
	index  int
}

// list returns the keys after marker that start with prefix, with the keys that contain delimiter
// after the prefix collapsed into common prefixes. It returns at most limit entries and whether
// there are more.
func list(keys []string, prefix, marker, delimiter string, limit int) (entries []listEntry, more bool) {
	for i, key := range keys {
		if !strings.HasPrefix(key, prefix) || key <= marker {
			continue
		}
		entry := listEntry{key: key, index: i}
		if delimiter != "" {
			if j := strings.Index(key[len(prefix):], delimiter); j >= 0 {
				entry = listEntry{key: key[:len(prefix)+j+len(delimiter)], prefix: true}
				if entry.key <= marker || len(entries) > 0 && entries[len(entries)-1].key == entry.key {
					continue
				}
			}
		}
		if len(entries) == limit {
			return entries, true
		}
		entries = append(entries, entry)
	}
	return entries, false
}

// limitResults clamps a requested number of results like miniogw.
func limitResults(limit, configured int) int {
	if limit < 0 || limit > configured {
		return configured
	}
	return limit
}

func (l *Layer) listObjects(bucket, prefix, marker, delimiter string, maxKeys int) (objects []objectlayer.ObjectInfo, prefixes []string, next string, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	metas, err := l.readObjects(bucket)
	if err != nil {
		return nil, nil, "", err
	}
	metas = slices.DeleteFunc(metas, func(meta objectMeta) bool { return meta.Versions[0].DeleteMarker })
	keys := make([]string, len(metas))
	for i, meta := range metas {
		keys[i] = meta.Key
	}

	// Like miniogw, listing no keys returns no keys and isn't truncated.
	if maxKeys == 0 {
		return nil, nil, "", nil
	}
	entries, more := list(keys, prefix, marker, delimiter, limitResults(maxKeys, maxKeysLimit))
	for _, entry := range entries {
		if entry.prefix {
			prefixes = append(prefixes, entry.key)
		} else {
			meta := metas[entry.index]
			objects = append(objects, objectInfo(bucket, meta.Key, meta.Versions[0], true))
		}
	}
	if more {
		next = entries[len(entries)-1].key
	}
	return objects, prefixes, next, nil
}

// ListObjects lists objects.
func (l *Layer) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (objectlayer.ListObjectsInfo, error) {
	objects, prefixes, next, err := l.listObjects(bucket, prefix, marker, delimiter, maxKeys)
	return objectlayer.ListObjectsInfo{
		IsTruncated: next != "",
		NextMarker:  next,
		Objects:     objects,
		Prefixes:    prefixes,
	}, err
}

// ListObjectsV2 lists objects. The continuation token is the key or prefix to list after.
func (l *Layer) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (objectlayer.ListObjectsV2Info, error) {
	marker := startAfter
	if continuationToken != "" {
		marker = continuationToken
	}
	objects, prefixes, next, err := l.listObjects(bucket, prefix, marker, delimiter, maxKeys)
	return objectlayer.ListObjectsV2Info{
		IsTruncated:           next != "",
		ContinuationToken:     continuationToken,
		NextContinuationToken: next,
		Objects:               objects,
		Prefixes:              prefixes,
	}, err
}

// ListObjectVersions lists the versions of objects, ordered by key and then from the newest.
// Like S3, the versions of the key marker after the version marker are listed, or none of them
// without it.
func (l *Layer) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (objectlayer.ListObjectVersionsInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var result objectlayer.ListObjectVersionsInfo
	metas, err := l.readObjects(bucket)
	if err != nil || maxKeys == 0 {
		return result, err
	}
	limit := limitResults(maxKeys, maxKeysLimit)
	count := 0
	var lastKey, lastVersionID string
	add := func(key, versionID string) bool {
		if count == limit {
			result.IsTruncated = true
			return false
		}
		count++
		lastKey, lastVersionID = key, versionID
		return true
	}
list:
	for _, meta := range metas {
		key := meta.Key
		if !strings.HasPrefix(key, prefix) || key < marker {
			continue
		}
		if delimiter != "" {
			if j := strings.Index(key[len(prefix):], delimiter); j >= 0 {
				commonPrefix := key[:len(prefix)+j+len(delimiter)]
				if commonPrefix <= marker || slices.Contains(result.Prefixes, commonPrefix) {
					continue
				}
				if !add(commonPrefix, "") {
					break
				}
				result.Prefixes = append(result.Prefixes, commonPrefix)
				continue
			}
		}
		start := 0
		if key == marker {
			i := slices.IndexFunc(meta.Versions, func(v versionMeta) bool { return v.VersionID == versionMarker })
			if versionMarker == "" || i < 0 {
				continue
			}
			start = i + 1
		}
		for i := start; i < len(meta.Versions); i++ {
			if !add(key, meta.Versions[i].VersionID) {
				break list
			}
			result.Objects = append(result.Objects, objectInfo(bucket, key, meta.Versions[i], i == 0))
		}
	}
	if result.IsTruncated {
		result.NextMarker, result.NextVersionIDMarker = lastKey, lastVersionID
	}
	return result, nil
}

// SetBucketVersioning enables or suspends versioning. Like in Storj, versioning can't be suspended
// while object lock is enabled.
func (l *Layer) SetBucketVersioning(ctx context.Context, bucket string, v *objectlayer.Versioning) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	meta, err := l.readBucket(bucket)
	if err != nil {
		return err
	}
	if v.Suspended() && meta.ObjectLock {
		return errInvalidBucketStateObjectLock
	}
	meta.Versioning = v.Status
	return writeJSON(filepath.Join(l.bucketDir(bucket), "bucket.json"), meta)
}

// GetBucketVersioning returns a bucket's versioning configuration, which is empty if versioning
// was never enabled.
func (l *Layer) GetBucketVersioning(ctx context.Context, bucket string) (*objectlayer.Versioning, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	meta, err := l.readBucket(bucket)
	if err != nil {
		return nil, err
	}
	return &objectlayer.Versioning{Status: meta.Versioning}, nil
}

// GetBucketTagging returns a bucket's tags.
func (l *Layer) GetBucketTagging(ctx context.Context, bucket string) (*objectlayer.Tags, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	meta, err := l.readBucket(bucket)
	if err != nil {
		return nil, err
	}
	if len(meta.Tags) == 0 {
		return nil, objectlayer.BucketTaggingNotFound{Bucket: bucket}
	}
	var tags objectlayer.Tags
	if err := xml.Unmarshal(meta.Tags, &tags); err != nil {
		return nil, err
	}
	return &tags, nil
}

// SetBucketTagging replaces a bucket's tags, or removes them if tags is nil.
func (l *Layer) SetBucketTagging(ctx context.Context, bucket string, tags *objectlayer.Tags) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	meta, err := l.readBucket(bucket)
	if err != nil {
		return err
	}
	meta.Tags = nil
	if tags != nil {
		if meta.Tags, err = xml.Marshal(tags); err != nil {
			return err
		}
	}
	return writeJSON(filepath.Join(l.bucketDir(bucket), "bucket.json"), meta)
}

// GetBucketNotificationConfig returns an empty configuration.
func (l *Layer) GetBucketNotificationConfig(ctx context.Context, bucket string) (*objectlayer.EventConfig, error) {
	if err := l.checkBucket(bucket); err != nil {
		return nil, err
	}
	return &objectlayer.EventConfig{}, nil
}

// SetBucketNotificationConfig isn't supported.
func (l *Layer) SetBucketNotificationConfig(ctx context.Context, bucket string, config *objectlayer.EventConfig) error {
	if err := l.checkBucket(bucket); err != nil {
		return err
	}
	return objectlayer.NotImplemented{Message: "notifications"}
}

// openObject returns the information of the version versionID of key, or of its latest version if
// versionID is empty, and its open data file.
func (l *Layer) openObject(bucket, key, versionID string) (objectlayer.ObjectInfo, *os.File, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	meta, i, err := l.readVersion(bucket, key, versionID)
	if err != nil {
		return objectlayer.ObjectInfo{}, nil, err
	}
	f, err := os.Open(filepath.Join(l.objectsDir(bucket), meta.Versions[i].Data))
	return objectInfo(bucket, key, meta.Versions[i], i == 0), f, err
}

// GetObjectNInfo returns a reader of an object's range.
func (l *Layer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *objectlayer.HTTPRangeSpec, h http.Header, lockType objectlayer.LockType, opts objectlayer.ObjectOptions) (*objectlayer.GetObjectReader, error) {
	info, f, err := l.openObject(bucket, object, opts.VersionID)
	if err != nil {
		return nil, err
	}
	offset, length, err := rs.GetOffsetLength(info.Size)
	if err != nil {
		_ = f.Close()
		return nil, objectlayer.InvalidRange{OffsetBegin: rs.Start, OffsetEnd: rs.End, ResourceSize: info.Size}
	}
	r := io.NewSectionReader(f, offset, length)
	return objectlayer.NewGetObjectReader(r, info, opts, func() { _ = f.Close() })
}

// GetObjectInfo returns an object's information.
func (l *Layer) GetObjectInfo(ctx context.Context, bucket, object string, opts objectlayer.ObjectOptions) (objectlayer.ObjectInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	meta, i, err := l.readVersion(bucket, object, opts.VersionID)
	if err != nil {
		return objectlayer.ObjectInfo{}, err
	}
	return objectInfo(bucket, object, meta.Versions[i], i == 0), nil
}

// PutObject uploads an object.
func (l *Layer) PutObject(ctx context.Context, bucket, object string, data *objectlayer.PutObjReader, opts objectlayer.ObjectOptions) (objectlayer.ObjectInfo, error) {
	if err := validateBucket(bucket); err != nil {
		return objectlayer.ObjectInfo{}, err
	}
	if err := checkPutOptions(bucket, object, opts); err != nil {
		return objectlayer.ObjectInfo{}, err
	}
	if err := l.checkBucket(bucket); err != nil {
		return objectlayer.ObjectInfo{}, err
	}

	tmp, size, err := l.writeTemp(bucket, data)
	if err != nil {
		return objectlayer.ObjectInfo{}, err
	}

	lengthRange := opts.PostPolicy.Conditions.ContentLengthRange
	switch {
	case lengthRange.Valid && size < lengthRange.Min:
		_ = os.Remove(tmp)
		return objectlayer.ObjectInfo{}, objectlayer.ObjectTooSmall{}
	case lengthRange.Valid && size > lengthRange.Max:
		_ = os.Remove(tmp)
		return objectlayer.ObjectInfo{}, objectlayer.ObjectTooLarge{}
	}

	etag, err := data.MD5HexString()
	if err != nil {
		_ = os.Remove(tmp)
		return objectlayer.ObjectInfo{}, err
	}

	return l.commitObject(bucket, object, tmp, versionMeta{
		Size:        size,
		ETag:        etag,
		UserDefined: moveTagging(opts.UserDefined),
		Retention:   newRetention(opts.Retention),
		LegalHold:   newLegalHold(opts.LegalHold),
	}, opts.IfNoneMatch)
}

// copyMetadata returns the metadata of a copy like miniogw's upsertObjectMetadata: metadata is the
// requested metadata, and the source's tags are kept unless they're replaced.
func copyMetadata(metadata, source map[string]string) map[string]string {
	metadata = maps.Clone(metadata)
	if metadata[amzTagDirective] == "REPLACE" {
		metadata[tagsKey] = metadata[amzObjectTagging]
	} else if tags, ok := source[tagsKey]; ok {
		metadata[tagsKey] = tags
	}
	delete(metadata, amzObjectTagging)
	delete(metadata, amzTagDirective)
	return metadata
}

// CopyObject copies an object. srcInfo.UserDefined is the metadata of the copy.
func (l *Layer) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo objectlayer.ObjectInfo, srcOpts, dstOpts objectlayer.ObjectOptions) (objectlayer.ObjectInfo, error) {
	if dstOpts.VersionID != "" {
		return objectlayer.ObjectInfo{}, objectlayer.NotImplemented{}
	}
	if len(dstObject) > maxKeyLength {
		return objectlayer.ObjectInfo{}, objectlayer.ObjectNameTooLong{Bucket: dstBucket, Object: dstObject}
	}
	if storageClass, ok := srcInfo.UserDefined[amzStorageClass]; ok && storageClass != storageClassNormal {
		return objectlayer.ObjectInfo{}, objectlayer.NotImplemented{Message: "storage class"}
	}
	switch {
	case srcObject == "":
		return objectlayer.ObjectInfo{}, objectlayer.ObjectNameInvalid{Bucket: srcBucket}
	case dstObject == "":
		return objectlayer.ObjectInfo{}, objectlayer.ObjectNameInvalid{Bucket: dstBucket}
	}
	if err := validateBucket(srcBucket); err != nil {
		return objectlayer.ObjectInfo{}, err
	}
	if err := validateBucket(dstBucket); err != nil {
		return objectlayer.ObjectInfo{}, err
	}

	if srcBucket == dstBucket && srcObject == dstObject && srcInfo.VersionID == "" {
		// Like miniogw, copying an object onto itself only updates its metadata.
		l.mu.Lock()
		defer l.mu.Unlock()

		meta, _, err := l.readVersion(srcBucket, srcObject, "")
		if err != nil {
			return objectlayer.ObjectInfo{}, err
		}
		latest := &meta.Versions[0]
		latest.UserDefined = copyMetadata(srcInfo.UserDefined, latest.UserDefined)
		if err := l.writeObject(srcBucket, meta); err != nil {
			return objectlayer.ObjectInfo{}, err
		}
		srcInfo.UserDefined = latest.UserDefined
		return srcInfo, nil
	}

	if err := checkIfNoneMatch(dstOpts.IfNoneMatch); err != nil {
		return objectlayer.ObjectInfo{}, err
	}
	if err := l.checkBucket(dstBucket); err != nil {
		return objectlayer.ObjectInfo{}, err
	}

	src, f, err := l.openObject(srcBucket, srcObject, srcInfo.VersionID)
	if err != nil {
		return objectlayer.ObjectInfo{}, err
	}
	tmp, size, err := l.writeTemp(dstBucket, f)
	_ = f.Close()
	if err != nil {
		return objectlayer.ObjectInfo{}, err
	}

	metadata := withoutObjectLock(src.UserDefined)
	if len(srcInfo.UserDefined) > 0 {
		metadata = copyMetadata(srcInfo.UserDefined, metadata)
	}
	return l.commitObject(dstBucket, dstObject, tmp, versionMeta{
		Size:        size,
		ETag:        src.ETag,
		UserDefined: metadata,
		Retention:   newRetention(dstOpts.Retention),
		LegalHold:   newLegalHold(dstOpts.LegalHold),
	}, dstOpts.IfNoneMatch)
}

// deleted describes what deleting an object did. Both are nil if there was nothing to delete.
type deleted struct {
	// removed is the version that was removed.
	removed *versionMeta
	// marker is the delete marker that was added.
	marker *versionMeta
}

// deleteObject deletes the version versionID of an object, or the object itself if versionID is
// empty. Like in Storj, deleting the object removes it in a bucket that was never versioned, and
// adds a delete marker otherwise, which replaces its unversioned version if versioning is
// suspended.
func (l *Layer) deleteObject(bucket, object, versionID string, bypassGovernance bool) (deleted, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	bucketMeta, err := l.readBucket(bucket)
	if err != nil {
		return deleted{}, err
	}
	meta, err := l.readObject(bucket, object)
	if errors.As(err, new(objectlayer.ObjectNotFound)) {
		meta, err = objectMeta{Key: object}, nil
	}
	if err != nil {
		return deleted{}, err
	}

	if versionID != "" || bucketMeta.Versioning == "" {
		i := 0
		if versionID != "" {
			i = slices.IndexFunc(meta.Versions, func(v versionMeta) bool { return v.VersionID == versionID })
		}
		if i < 0 || i >= len(meta.Versions) {
			return deleted{}, nil
		}
		removed := meta.Versions[i]
		if isProtected(removed, bypassGovernance) {
			return deleted{}, errObjectProtected
		}
		l.removeVersions(bucket, &meta, func(v versionMeta) bool { return v.VersionID == removed.VersionID })
		return deleted{removed: &removed}, l.writeObject(bucket, meta)
	}

	marker := versionMeta{
		VersionID:    randomID(),
		Versioned:    bucketMeta.Versioning == objectlayer.VersioningEnabled,
		DeleteMarker: true,
		ModTime:      time.Now().UTC(),
	}
	if !marker.Versioned {
		l.removeVersions(bucket, &meta, func(v versionMeta) bool { return !v.Versioned })
	}
	meta.Versions = slices.Insert(meta.Versions, 0, marker)
	return deleted{marker: &marker}, l.writeObject(bucket, meta)
}

// DeleteObject deletes an object or one of its versions. A missing object or version isn't an
// error.
func (l *Layer) DeleteObject(ctx context.Context, bucket, object string, opts objectlayer.ObjectOptions) (objectlayer.ObjectInfo, error) {
	result, err := l.deleteObject(bucket, object, opts.VersionID, opts.BypassGovernanceRetention)
	switch {
	case err != nil:
		return objectlayer.ObjectInfo{}, err
	case result.marker != nil:
		return objectInfo(bucket, object, *result.marker, true), nil
	case result.removed != nil:
		return objectInfo(bucket, object, *result.removed, false), nil
	}
	return objectlayer.ObjectInfo{}, nil
}

// DeleteObjects deletes objects or their versions, reporting them like miniogw.
func (l *Layer) DeleteObjects(ctx context.Context, bucket string, objects []objectlayer.ObjectToDelete, opts objectlayer.ObjectOptions) ([]objectlayer.DeletedObject, []objectlayer.DeleteObjectsError, error) {
	if err := l.checkBucket(bucket); err != nil {
		return nil, nil, err
	}
	var deletedObjects []objectlayer.DeletedObject
	var deleteErrs []objectlayer.DeleteObjectsError
	for _, object := range objects {
		result, err := l.deleteObject(bucket, object.ObjectName, object.VersionID, opts.BypassGovernanceRetention)
		if err != nil {
			deleteErrs = append(deleteErrs, objectlayer.DeleteObjectsError{ObjectName: object.ObjectName, VersionID: object.VersionID, Error: err})
			continue
		}
		deletedObject := objectlayer.DeletedObject{ObjectName: object.ObjectName, VersionID: object.VersionID}
		if result.removed != nil {
			deletedObject.VersionID = result.removed.VersionID
			if result.removed.DeleteMarker {
				deletedObject.DeleteMarker = true
				deletedObject.DeleteMarkerVersionID = result.removed.VersionID
			}
		}
		if result.marker != nil {
			deletedObject.DeleteMarker = true
			deletedObject.DeleteMarkerVersionID = result.marker.VersionID
		}
		deletedObjects = append(deletedObjects, deletedObject)
	}
	return deletedObjects, deleteErrs, nil
}

// setTags replaces an object's s3:tags metadata, or removes it if tags is empty.
func (l *Layer) setTags(bucket, object, tags string, opts objectlayer.ObjectOptions) (objectlayer.ObjectInfo, error) {
	if opts.VersionID != "" {
		return objectlayer.ObjectInfo{}, objectlayer.NotImplemented{}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	meta, _, err := l.readVersion(bucket, object, "")
	if err != nil {
		return objectlayer.ObjectInfo{}, err
	}
	latest := &meta.Versions[0]
	if latest.UserDefined == nil {
		latest.UserDefined = make(map[string]string)
	}
	if tags == "" {
		delete(latest.UserDefined, tagsKey)
	} else {
		latest.UserDefined[tagsKey] = tags
	}
	if err := l.writeObject(bucket, meta); err != nil {
		return objectlayer.ObjectInfo{}, err
	}
	return objectInfo(bucket, object, *latest, true), nil
}

// PutObjectTags replaces an object's tags.
func (l *Layer) PutObjectTags(ctx context.Context, bucket, object, tags string, opts objectlayer.ObjectOptions) (objectlayer.ObjectInfo, error) {
	return l.setTags(bucket, object, tags, opts)
}

// DeleteObjectTags removes an object's tags.
func (l *Layer) DeleteObjectTags(ctx context.Context, bucket, object string, opts objectlayer.ObjectOptions) (objectlayer.ObjectInfo, error) {
	return l.setTags(bucket, object, "", opts)
}

// ListMultipartUploads lists multipart uploads, ordered by key and then by initiation time.
func (l *Layer) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (objectlayer.ListMultipartsInfo, error) {
	result := objectlayer.ListMultipartsInfo{
		KeyMarker:      keyMarker,
		UploadIDMarker: uploadIDMarker,
		MaxUploads:     maxUploads,
		Prefix:         prefix,
		Delimiter:      delimiter,
	}
	if delimiter != "" && delimiter != "/" {
		return result, objectlayer.NotImplemented{Message: "Unsupported delimiter: " + strconv.Quote(delimiter)}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if _, err := l.readBucket(bucket); err != nil {
		return result, err
	}
	dirs, err := os.ReadDir(filepath.Join(l.bucketDir(bucket), "uploads"))
	if err != nil {
		return result, err
	}
	var uploads []objectlayer.MultipartInfo
	for _, dir := range dirs {
		var meta uploadMeta
		if err := readJSON(filepath.Join(l.uploadDir(bucket, dir.Name()), "upload.json"), &meta); err != nil {
			continue
		}
		uploads = append(uploads, objectlayer.MultipartInfo{
			Bucket:      bucket,
			Object:      meta.Key,
			UploadID:    dir.Name(),
			Initiated:   meta.Initiated,
			UserDefined: meta.UserDefined,
		})
	}
	slices.SortFunc(uploads, func(a, b objectlayer.MultipartInfo) int {
		if c := strings.Compare(a.Object, b.Object); c != 0 {
			return c
		}
		if c := a.Initiated.Compare(b.Initiated); c != 0 {
			return c
		}
		return strings.Compare(a.UploadID, b.UploadID)
	})

	// The uploads of keyMarker after uploadIDMarker are listed, or none of them without it.
	if uploadIDMarker != "" {
		if i := slices.IndexFunc(uploads, func(u objectlayer.MultipartInfo) bool {
			return u.Object == keyMarker && u.UploadID == uploadIDMarker
		}); i >= 0 {
			uploads = uploads[i+1:]
			keyMarker = ""
		}
	}
	limit := limitResults(maxUploads, maxUploadsLimit)
	var lastUploadID string
	for _, upload := range uploads {
		if !strings.HasPrefix(upload.Object, prefix) || keyMarker != "" && upload.Object <= keyMarker {
			continue
		}
		key, isPrefix := upload.Object, false
		if delimiter != "" {
			if j := strings.Index(key[len(prefix):], delimiter); j >= 0 {
				key, isPrefix = key[:len(prefix)+j+len(delimiter)], true
				if key <= keyMarker || slices.Contains(result.CommonPrefixes, key) {
					continue
				}
			}
		}
		if len(result.Uploads)+len(result.CommonPrefixes) == limit {
			result.IsTruncated = true
			break
		}
		result.NextKeyMarker, lastUploadID = key, ""
		if isPrefix {
			result.CommonPrefixes = append(result.CommonPrefixes, key)
		} else {
			result.Uploads = append(result.Uploads, upload)
			lastUploadID = upload.UploadID
		}
	}
	if result.IsTruncated {
		result.NextUploadIDMarker = lastUploadID
	} else {
		result.NextKeyMarker = ""
	}
	return result, nil
}

// NewMultipartUpload starts a multipart upload.
func (l *Layer) NewMultipartUpload(ctx context.Context, bucket, object string, opts objectlayer.ObjectOptions) (string, error) {
	if err := validateBucket(bucket); err != nil {
		return "", err
	}
	if err := checkPutOptions(bucket, object, opts); err != nil {
		return "", err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	bucketMeta, err := l.readBucket(bucket)
	if err != nil {
		return "", err
	}
	retention, legalHold := newRetention(opts.Retention), newLegalHold(opts.LegalHold)
	if (retention != nil || legalHold != nil) && !bucketMeta.ObjectLock {
		return "", errObjectLockNotEnabled
	}
	uploadID := randomID()
	dir := l.uploadDir(bucket, uploadID)
	if err := os.Mkdir(dir, 0o700); err != nil {
		return "", err
	}
	return uploadID, writeJSON(filepath.Join(dir, "upload.json"), uploadMeta{
		Key:         object,
		Initiated:   time.Now().UTC(),
		UserDefined: moveTagging(opts.UserDefined),
		Retention:   retention,
		LegalHold:   legalHold,
		Parts:       make(map[int]partMeta),
	})
}

// putPart stores the data in r as a part of an upload.
func (l *Layer) putPart(bucket, object, uploadID string, partID int, r io.Reader, md5Hex func() (string, error)) (objectlayer.PartInfo, error) {
	if partID < 1 || int64(partID) > math.MaxUint32 {
		return objectlayer.PartInfo{}, objectlayer.InvalidArgument{Bucket: bucket, Object: object, Err: errors.New("partID is out of range")}
	}
	l.mu.Lock()
	_, err := l.readUpload(bucket, object, uploadID)
	l.mu.Unlock()
	if err != nil {
		return objectlayer.PartInfo{}, err
	}

	tmp, size, err := l.writeTemp(bucket, r)
	if err != nil {
		return objectlayer.PartInfo{}, err
	}
	etag, err := md5Hex()
	if err != nil {
		_ = os.Remove(tmp)
		return objectlayer.PartInfo{}, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	meta, err := l.readUpload(bucket, object, uploadID)
	if err != nil {
		_ = os.Remove(tmp)
		return objectlayer.PartInfo{}, err
	}
	dir := l.uploadDir(bucket, uploadID)
	part := partMeta{Size: size, ETag: etag, ModTime: time.Now().UTC(), Data: randomID() + ".data"}
	if err := os.Rename(tmp, filepath.Join(dir, part.Data)); err != nil {
		_ = os.Remove(tmp)
		return objectlayer.PartInfo{}, err
	}
	if old, ok := meta.Parts[partID]; ok {
		_ = os.Remove(filepath.Join(dir, old.Data))
	}
	meta.Parts[partID] = part
	if err := writeJSON(filepath.Join(dir, "upload.json"), meta); err != nil {
		return objectlayer.PartInfo{}, err
	}
	return partInfo(partID, part), nil
}

func partInfo(partID int, part partMeta) objectlayer.PartInfo {
	return objectlayer.PartInfo{
		PartNumber:   partID,
		LastModified: part.ModTime,
		ETag:         part.ETag,
		Size:         part.Size,
		ActualSize:   part.Size,
	}
}

// PutObjectPart uploads a part.
func (l *Layer) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *objectlayer.PutObjReader, opts objectlayer.ObjectOptions) (objectlayer.PartInfo, error) {
	return l.putPart(bucket, object, uploadID, partID, data, data.MD5HexString)
}

// CopyObjectPart uploads a range of an object as a part. A negative length copies until the end.
func (l *Layer) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int, startOffset, length int64, srcInfo objectlayer.ObjectInfo, srcOpts, dstOpts objectlayer.ObjectOptions) (objectlayer.PartInfo, error) {
	info, f, err := l.openObject(srcBucket, srcObject, srcOpts.VersionID)
	if err != nil {
		return objectlayer.PartInfo{}, err
	}
	defer func() { _ = f.Close() }()
	if length < 0 {
		length = info.Size - startOffset
	}
	h := newMD5Reader(io.NewSectionReader(f, startOffset, length))
	return l.putPart(dstBucket, dstObject, uploadID, partID, h, h.hexSum)
}

// ListObjectParts lists the parts of an upload after partNumberMarker.
func (l *Layer) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, opts objectlayer.ObjectOptions) (objectlayer.ListPartsInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	meta, err := l.readUpload(bucket, object, uploadID)
	if err != nil {
		return objectlayer.ListPartsInfo{}, err
	}
	result := objectlayer.ListPartsInfo{
		Bucket:               bucket,
		Object:               object,
		UploadID:             uploadID,
		StorageClass:         storageClassNormal,
		PartNumberMarker:     partNumberMarker,
		NextPartNumberMarker: partNumberMarker,
		MaxParts:             maxParts,
	}
	for _, id := range slices.Sorted(maps.Keys(meta.Parts)) {
		if id <= partNumberMarker {
			continue
		}
		if len(result.Parts) == maxParts {
			result.IsTruncated = true
			break
		}
		result.Parts = append(result.Parts, partInfo(id, meta.Parts[id]))
		result.NextPartNumberMarker = id
	}
	return result, nil
}

// AbortMultipartUpload deletes an upload and its parts.
func (l *Layer) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts objectlayer.ObjectOptions) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, err := l.readUpload(bucket, object, uploadID); err != nil {
		return err
	}
	return os.RemoveAll(l.uploadDir(bucket, uploadID))
}

// CompleteMultipartUpload completes an upload. Like miniogw, parts must list every uploaded part
// in order.
func (l *Layer) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []objectlayer.CompletePart, opts objectlayer.ObjectOptions) (objectlayer.ObjectInfo, error) {
	if err := checkIfNoneMatch(opts.IfNoneMatch); err != nil {
		return objectlayer.ObjectInfo{}, err
	}

	l.mu.Lock()
	meta, err := l.readUpload(bucket, object, uploadID)
	l.mu.Unlock()
	if err != nil {
		return objectlayer.ObjectInfo{}, err
	}

	ids := slices.Sorted(maps.Keys(meta.Parts))
	for i, id := range ids {
		part := meta.Parts[id]
		switch {
		case i >= len(parts):
			return objectlayer.ObjectInfo{}, objectlayer.InvalidPart{PartNumber: id, ExpETag: part.ETag}
		case i != len(parts)-1 && part.Size < minPartSize:
			return objectlayer.ObjectInfo{}, objectlayer.PartTooSmall{PartSize: part.Size, PartNumber: id, PartETag: part.ETag}
		case parts[i].PartNumber != id:
			return objectlayer.ObjectInfo{}, objectlayer.InvalidPart{PartNumber: parts[i].PartNumber, GotETag: parts[i].ETag}
		case parts[i].ETag != part.ETag:
			return objectlayer.ObjectInfo{}, objectlayer.InvalidPart{PartNumber: id, ExpETag: part.ETag, GotETag: parts[i].ETag}
		}
	}
	if len(parts) > len(ids) {
		return objectlayer.ObjectInfo{}, objectlayer.InvalidPart{PartNumber: parts[len(ids)].PartNumber, GotETag: parts[len(ids)].ETag}
	}

	readers := make([]io.Reader, 0, len(ids))
	defer func() {
		for _, r := range readers {
			_ = r.(*os.File).Close()
		}
	}()
	for _, id := range ids {
		f, err := os.Open(filepath.Join(l.uploadDir(bucket, uploadID), meta.Parts[id].Data))
		if err != nil {
			return objectlayer.ObjectInfo{}, err
		}
		readers = append(readers, f)
	}
	tmp, size, err := l.writeTemp(bucket, io.MultiReader(readers...))
	if err != nil {
		return objectlayer.ObjectInfo{}, err
	}

	info, err := l.commitObject(bucket, object, tmp, versionMeta{
		Size:        size,
		ETag:        objectlayer.MultipartETag(parts),
		UserDefined: meta.UserDefined,
		Retention:   meta.Retention,
		LegalHold:   meta.LegalHold,
	}, opts.IfNoneMatch)
	if err != nil {
		return objectlayer.ObjectInfo{}, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	return info, os.RemoveAll(l.uploadDir(bucket, uploadID))
}

// md5Reader computes the MD5 of what's read through it.
type md5Reader struct {
	r io.Reader
	h hash.Hash
}

func newMD5Reader(r io.Reader) *md5Reader { return &md5Reader{r: r, h: md5.New()} }

func (r *md5Reader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.h.Write(p[:n])
	return n, err
}

func (r *md5Reader) hexSum() (string, error) { return hex.EncodeToString(r.h.Sum(nil)), nil }
