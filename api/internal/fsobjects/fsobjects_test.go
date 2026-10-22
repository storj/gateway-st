// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package fsobjects_test

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"

	"storj.io/gateway/api"
	"storj.io/gateway/api/internal/fsobjects"
	"storj.io/gateway/api/objectlayer"
	"storj.io/minio/pkg/hash"
)

type testCredentials struct{}

func (testCredentials) Provide(_ context.Context, accessKeyID string) (string, api.AuthData, error) {
	return "secret", api.AuthData{AccessKeyID: accessKeyID}, nil
}

// TestThroughAPI runs common requests through the api package.
func TestThroughAPI(t *testing.T) {
	ctx := t.Context()

	router := mux.NewRouter()
	api.New(fsobjects.New(t.TempDir()), testCredentials{}, api.Config{}).RegisterHandlers(router)
	server := httptest.NewServer(router)
	defer server.Close()

	u, err := url.Parse(server.URL)
	require.NoError(t, err)
	client, err := minio.New(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4("access", "secret", ""),
		BucketLookup: minio.BucketLookupPath,
	})
	require.NoError(t, err)

	require.NoError(t, client.MakeBucket(ctx, "bucket", minio.MakeBucketOptions{}))
	err = client.MakeBucket(ctx, "bucket", minio.MakeBucketOptions{})
	require.Equal(t, "BucketAlreadyExists", minio.ToErrorResponse(err).Code)

	put := func(key, data string) {
		_, err := client.PutObject(ctx, "bucket", key, strings.NewReader(data), int64(len(data)), minio.PutObjectOptions{
			ContentType:  "text/plain",
			UserMetadata: map[string]string{"Foo": "bar"},
			UserTags:     map[string]string{"tag": "value"},
		})
		require.NoError(t, err)
	}
	for _, key := range []string{"a", "b/1", "b/2", "c"} {
		put(key, "data of "+key)
	}
	put("a", "new data of a")

	// Reading, with and without a range.
	obj, err := client.GetObject(ctx, "bucket", "a", minio.GetObjectOptions{})
	require.NoError(t, err)
	data, err := io.ReadAll(obj)
	require.NoError(t, err)
	require.Equal(t, "new data of a", string(data))
	info, err := obj.Stat()
	require.NoError(t, err)
	require.Equal(t, "text/plain", info.ContentType)
	require.Equal(t, "bar", info.UserMetadata["Foo"])

	opts := minio.GetObjectOptions{}
	require.NoError(t, opts.SetRange(4, 7))
	obj, err = client.GetObject(ctx, "bucket", "a", opts)
	require.NoError(t, err)
	data, err = io.ReadAll(obj)
	require.NoError(t, err)
	require.Equal(t, "data", string(data))

	tags, err := client.GetObjectTagging(ctx, "bucket", "a", minio.GetObjectTaggingOptions{})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"tag": "value"}, tags.ToMap())

	// Listing with a delimiter, one entry per page.
	var listed []string
	for item := range client.ListObjects(ctx, "bucket", minio.ListObjectsOptions{MaxKeys: 1}) {
		require.NoError(t, item.Err)
		listed = append(listed, item.Key)
	}
	require.Equal(t, []string{"a", "b/", "c"}, listed)

	// Copying.
	_, err = client.CopyObject(ctx,
		minio.CopyDestOptions{Bucket: "bucket", Object: "copy"},
		minio.CopySrcOptions{Bucket: "bucket", Object: "b/1"})
	require.NoError(t, err)
	obj, err = client.GetObject(ctx, "bucket", "copy", minio.GetObjectOptions{})
	require.NoError(t, err)
	data, err = io.ReadAll(obj)
	require.NoError(t, err)
	require.Equal(t, "data of b/1", string(data))

	// A multipart upload with two parts. minio-go uses a multipart upload for objects larger than
	// PartSize.
	big := bytes.Repeat([]byte("0123456789"), 600_000)
	_, err = client.PutObject(ctx, "bucket", "big", bytes.NewReader(big), int64(len(big)), minio.PutObjectOptions{
		PartSize:         5 << 20,
		DisableMultipart: false,
	})
	require.NoError(t, err)
	obj, err = client.GetObject(ctx, "bucket", "big", minio.GetObjectOptions{})
	require.NoError(t, err)
	data, err = io.ReadAll(obj)
	require.NoError(t, err)
	require.Equal(t, big, data)
	info, err = obj.Stat()
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(info.ETag, "-2"), info.ETag)

	// Deleting.
	require.NoError(t, client.RemoveObject(ctx, "bucket", "a", minio.RemoveObjectOptions{}))
	_, err = client.StatObject(ctx, "bucket", "a", minio.StatObjectOptions{})
	require.Equal(t, "NoSuchKey", minio.ToErrorResponse(err).Code)

	err = client.RemoveBucket(ctx, "bucket")
	require.Equal(t, "BucketNotEmpty", minio.ToErrorResponse(err).Code)
}

func TestListingLimits(t *testing.T) {
	ctx := t.Context()
	layer := fsobjects.New(t.TempDir())
	require.NoError(t, layer.MakeBucketWithLocation(ctx, "bucket", objectlayer.BucketOptions{}))
	for _, key := range []string{"a", "b"} {
		r, err := hash.NewDefaultReader(strings.NewReader(""), 0, "", "", 0)
		require.NoError(t, err)
		_, err = layer.PutObject(ctx, "bucket", key, objectlayer.NewPutObjReader(r), objectlayer.ObjectOptions{})
		require.NoError(t, err)
	}

	// Listing no keys isn't truncated.
	v2, err := layer.ListObjectsV2(ctx, "bucket", "", "", "", 0, false, "")
	require.NoError(t, err)
	require.False(t, v2.IsTruncated)
	require.Empty(t, v2.Objects)

	// A truncated version listing continues after the last version.
	versions, err := layer.ListObjectVersions(ctx, "bucket", "", "", "", "", 1)
	require.NoError(t, err)
	require.True(t, versions.IsTruncated)
	require.Equal(t, "a", versions.NextMarker)
	require.Equal(t, versions.Objects[0].VersionID, versions.NextVersionIDMarker)
	require.NotEmpty(t, versions.NextVersionIDMarker)
}

func put(t *testing.T, layer *fsobjects.Layer, bucket, key string, opts objectlayer.ObjectOptions) objectlayer.ObjectInfo {
	t.Helper()
	r, err := hash.NewDefaultReader(strings.NewReader(key), int64(len(key)), "", "", int64(len(key)))
	require.NoError(t, err)
	info, err := layer.PutObject(t.Context(), bucket, key, objectlayer.NewPutObjReader(r), opts)
	require.NoError(t, err)
	return info
}

func TestVersioning(t *testing.T) {
	ctx := t.Context()
	layer := fsobjects.New(t.TempDir())
	require.NoError(t, layer.MakeBucketWithLocation(ctx, "bucket", objectlayer.BucketOptions{}))

	versionIDs := func() (ids []string, markers []bool) {
		versions, err := layer.ListObjectVersions(ctx, "bucket", "", "", "", "", 1000)
		require.NoError(t, err)
		for _, v := range versions.Objects {
			ids = append(ids, v.VersionID)
			markers = append(markers, v.DeleteMarker)
		}
		return ids, markers
	}

	// Without versioning, a write replaces the object.
	put(t, layer, "bucket", "key", objectlayer.ObjectOptions{})
	unversioned := put(t, layer, "bucket", "key", objectlayer.ObjectOptions{})
	ids, _ := versionIDs()
	require.Equal(t, []string{unversioned.VersionID}, ids)

	// With versioning, writes and deletes add versions, newest first.
	require.NoError(t, layer.SetBucketVersioning(ctx, "bucket", &objectlayer.Versioning{Status: objectlayer.VersioningEnabled}))
	v1 := put(t, layer, "bucket", "key", objectlayer.ObjectOptions{})
	marker, err := layer.DeleteObject(ctx, "bucket", "key", objectlayer.ObjectOptions{})
	require.NoError(t, err)
	require.True(t, marker.DeleteMarker)
	ids, markers := versionIDs()
	require.Equal(t, []string{marker.VersionID, v1.VersionID, unversioned.VersionID}, ids)
	require.Equal(t, []bool{true, false, false}, markers)

	_, err = layer.GetObjectInfo(ctx, "bucket", "key", objectlayer.ObjectOptions{})
	require.ErrorAs(t, err, new(objectlayer.ObjectNotFound))
	_, err = layer.GetObjectInfo(ctx, "bucket", "key", objectlayer.ObjectOptions{VersionID: marker.VersionID})
	require.ErrorAs(t, err, new(objectlayer.MethodNotAllowed))
	info, err := layer.GetObjectInfo(ctx, "bucket", "key", objectlayer.ObjectOptions{VersionID: v1.VersionID})
	require.NoError(t, err)
	require.False(t, info.IsLatest)
	listed, err := layer.ListObjectsV2(ctx, "bucket", "", "", "", 1000, false, "")
	require.NoError(t, err)
	require.Empty(t, listed.Objects)

	// A version listing continues after its version marker.
	page, err := layer.ListObjectVersions(ctx, "bucket", "", "", "", "", 2)
	require.NoError(t, err)
	require.True(t, page.IsTruncated)
	page, err = layer.ListObjectVersions(ctx, "bucket", "", page.NextMarker, page.NextVersionIDMarker, "", 2)
	require.NoError(t, err)
	require.False(t, page.IsTruncated)
	require.Len(t, page.Objects, 1)
	require.Equal(t, unversioned.VersionID, page.Objects[0].VersionID)

	// Deleting the delete marker makes the previous version the latest again.
	_, err = layer.DeleteObject(ctx, "bucket", "key", objectlayer.ObjectOptions{VersionID: marker.VersionID})
	require.NoError(t, err)
	info, err = layer.GetObjectInfo(ctx, "bucket", "key", objectlayer.ObjectOptions{})
	require.NoError(t, err)
	require.Equal(t, v1.VersionID, info.VersionID)

	// With versioning suspended, a write replaces the unversioned version and keeps the others.
	require.NoError(t, layer.SetBucketVersioning(ctx, "bucket", &objectlayer.Versioning{Status: objectlayer.VersioningSuspended}))
	suspended := put(t, layer, "bucket", "key", objectlayer.ObjectOptions{})
	ids, _ = versionIDs()
	require.Equal(t, []string{suspended.VersionID, v1.VersionID}, ids)

	err = layer.DeleteBucket(ctx, "bucket", false)
	require.ErrorAs(t, err, new(objectlayer.BucketNotEmpty))
}

func TestObjectLock(t *testing.T) {
	ctx := t.Context()
	layer := fsobjects.New(t.TempDir())
	require.NoError(t, layer.MakeBucketWithLocation(ctx, "plain", objectlayer.BucketOptions{}))
	require.NoError(t, layer.MakeBucketWithLocation(ctx, "locked", objectlayer.BucketOptions{LockEnabled: true}))

	// Object lock needs versioning enabled.
	err := layer.SetObjectLockConfig(ctx, "plain", &objectlayer.ObjectLockConfig{ObjectLockEnabled: "Enabled"})
	require.ErrorContains(t, err, "versioning")
	err = layer.SetBucketVersioning(ctx, "locked", &objectlayer.Versioning{Status: objectlayer.VersioningSuspended})
	require.ErrorContains(t, err, "versioning")

	future := objectlayer.RetentionDate{Time: time.Now().Add(time.Hour)}
	governance := &objectlayer.ObjectRetention{Mode: objectlayer.RetentionGovernance, RetainUntilDate: future}
	info := put(t, layer, "locked", "key", objectlayer.ObjectOptions{Retention: governance})
	require.Equal(t, "GOVERNANCE", info.UserDefined["x-amz-object-lock-mode"])
	require.Equal(t, "OFF", info.UserDefined["x-amz-object-lock-legal-hold"])

	// A governance mode retention protects the version unless it's bypassed.
	version := objectlayer.ObjectOptions{VersionID: info.VersionID}
	_, err = layer.DeleteObject(ctx, "locked", "key", version)
	require.ErrorContains(t, err, "protected")
	shorter := &objectlayer.ObjectRetention{Mode: objectlayer.RetentionGovernance, RetainUntilDate: objectlayer.RetentionDate{Time: future.Add(-time.Minute)}}
	err = layer.SetObjectRetention(ctx, "locked", "key", info.VersionID, objectlayer.ObjectOptions{Retention: shorter})
	require.ErrorContains(t, err, "protected")
	require.NoError(t, layer.SetObjectRetention(ctx, "locked", "key", info.VersionID, objectlayer.ObjectOptions{Retention: shorter, BypassGovernanceRetention: true}))

	// A legal hold protects the version even with a bypass.
	on := &objectlayer.ObjectLegalHold{Status: objectlayer.LegalHoldOn}
	require.NoError(t, layer.SetObjectLegalHold(ctx, "locked", "key", info.VersionID, on))
	_, err = layer.DeleteObject(ctx, "locked", "key", objectlayer.ObjectOptions{VersionID: info.VersionID, BypassGovernanceRetention: true})
	require.ErrorContains(t, err, "protected")
	require.NoError(t, layer.SetObjectLegalHold(ctx, "locked", "key", info.VersionID, &objectlayer.ObjectLegalHold{Status: objectlayer.LegalHoldOff}))
	_, err = layer.DeleteObject(ctx, "locked", "key", objectlayer.ObjectOptions{VersionID: info.VersionID, BypassGovernanceRetention: true})
	require.NoError(t, err)

	// A compliance mode retention can only be extended.
	compliance := &objectlayer.ObjectRetention{Mode: objectlayer.RetentionCompliance, RetainUntilDate: future}
	info = put(t, layer, "locked", "key", objectlayer.ObjectOptions{Retention: compliance})
	err = layer.SetObjectRetention(ctx, "locked", "key", info.VersionID, objectlayer.ObjectOptions{Retention: governance, BypassGovernanceRetention: true})
	require.ErrorContains(t, err, "protected")
	longer := &objectlayer.ObjectRetention{Mode: objectlayer.RetentionCompliance, RetainUntilDate: objectlayer.RetentionDate{Time: future.Add(time.Hour)}}
	require.NoError(t, layer.SetObjectRetention(ctx, "locked", "key", info.VersionID, objectlayer.ObjectOptions{Retention: longer}))

	// New versions get the default retention.
	days := int32(1)
	require.NoError(t, layer.SetObjectLockConfig(ctx, "locked", &objectlayer.ObjectLockConfig{
		ObjectLockEnabled: "Enabled",
		Rule: &struct {
			DefaultRetention objectlayer.DefaultRetention `xml:"DefaultRetention"`
		}{objectlayer.DefaultRetention{Mode: objectlayer.RetentionGovernance, Days: &days}},
	}))
	info = put(t, layer, "locked", "other", objectlayer.ObjectOptions{})
	retention, err := layer.GetObjectRetention(ctx, "locked", "other", info.VersionID)
	require.NoError(t, err)
	require.Equal(t, objectlayer.RetentionGovernance, retention.Mode)
	require.WithinDuration(t, time.Now().AddDate(0, 0, 1), retention.RetainUntilDate.Time, time.Minute)

	// Object lock settings need object lock enabled.
	put(t, layer, "plain", "key", objectlayer.ObjectOptions{})
	_, err = layer.GetObjectRetention(ctx, "plain", "key", "")
	require.ErrorContains(t, err, "Object Lock")
	r, err := hash.NewDefaultReader(strings.NewReader(""), 0, "", "", 0)
	require.NoError(t, err)
	_, err = layer.PutObject(ctx, "plain", "key", objectlayer.NewPutObjReader(r), objectlayer.ObjectOptions{Retention: governance})
	require.ErrorContains(t, err, "Object Lock")
}
