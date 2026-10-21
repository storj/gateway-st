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
