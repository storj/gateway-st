// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package minioobjectlayer_test

import (
	"context"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/testcontext"
	"storj.io/gateway/api/objectlayer"
	"storj.io/gateway/miniogw/minioobjectlayer"
	minio "storj.io/minio/cmd"
)

type fakeLayer struct {
	minioobjectlayer.ObjectLayer

	info    objectlayer.ObjectInfo
	data    string
	srcInfo objectlayer.ObjectInfo
	opts    objectlayer.ObjectOptions
	cleanup int
}

func (f *fakeLayer) GetObjectInfo(ctx context.Context, bucket, object string, opts objectlayer.ObjectOptions) (objectlayer.ObjectInfo, error) {
	return f.info, nil
}

func (f *fakeLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *objectlayer.HTTPRangeSpec, h http.Header, lockType objectlayer.LockType, opts objectlayer.ObjectOptions) (*objectlayer.GetObjectReader, error) {
	return objectlayer.NewGetObjectReader(strings.NewReader(f.data), f.info, opts, func() { f.cleanup++ })
}

func (f *fakeLayer) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo objectlayer.ObjectInfo, srcOpts, dstOpts objectlayer.ObjectOptions) (objectlayer.ObjectInfo, error) {
	f.srcInfo = srcInfo
	return srcInfo, nil
}

func (f *fakeLayer) PutObject(ctx context.Context, bucket, object string, data *objectlayer.PutObjReader, opts objectlayer.ObjectOptions) (objectlayer.ObjectInfo, error) {
	f.opts = opts
	return objectlayer.ObjectInfo{}, nil
}

func fullObjectInfo(t *testing.T) objectlayer.ObjectInfo {
	info := objectlayer.ObjectInfo{
		Bucket:          "bucket",
		Name:            "object",
		ModTime:         time.Unix(1, 0),
		Size:            2,
		ETag:            "etag",
		VersionID:       "version",
		IsLatest:        true,
		DeleteMarker:    true,
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
		Expires:         time.Unix(3, 0),
		StorageClass:    "STANDARD",
		UserDefined:     map[string]string{"key": "value"},
		UserTags:        "tag=value",
		Checksum:        objectlayer.Checksum{Algorithm: objectlayer.ChecksumAlgorithmCRC32, Value: "DUoRhQ=="},
	}
	// Every field must be checked, so a new one has to be added here.
	v := reflect.ValueOf(info)
	for i := range v.NumField() {
		require.False(t, v.Field(i).IsZero(), "ObjectInfo.%s isn't set", v.Type().Field(i).Name)
	}
	return info
}

func TestObjectInfoRoundTrip(t *testing.T) {
	ctx := testcontext.New(t)

	info := fullObjectInfo(t)
	fake := &fakeLayer{info: info}
	layer := minioobjectlayer.New(fake)

	minioInfo, err := layer.GetObjectInfo(ctx, "bucket", "object", minio.ObjectOptions{})
	require.NoError(t, err)

	copied, err := layer.CopyObject(ctx, "bucket", "object", "bucket", "copy", minioInfo, minio.ObjectOptions{}, minio.ObjectOptions{})
	require.NoError(t, err)
	assert.Equal(t, minioInfo, copied)

	// minio's ObjectInfo can't carry the checksum.
	info.Checksum = objectlayer.Checksum{}
	assert.Equal(t, info, fake.srcInfo)
}

func TestObjectOptions(t *testing.T) {
	ctx := testcontext.New(t)

	fake := &fakeLayer{}
	retention := &objectlayer.ObjectRetention{Mode: objectlayer.RetentionCompliance}
	legalHold := objectlayer.LegalHoldOn
	var opts minio.ObjectOptions
	opts.VersionID = "version"
	opts.PostPolicy.Expiration = time.Unix(1, 0)
	opts.PostPolicy.Conditions.Items = []minio.PostPolicyCondition{{Operator: "eq", Key: "$key", Value: "object"}}
	opts.PostPolicy.Conditions.ContentLengthRange = minio.ContentLengthRange{Min: 1, Max: 2, Valid: true}
	opts.Retention = retention
	opts.BypassGovernanceRetention = true
	opts.LegalHold = &legalHold
	opts.IfNoneMatch = []string{"*"}
	opts.UserDefined = map[string]string{"key": "value"}
	opts.PartNumber = 3
	opts.Quiet = true

	_, err := minioobjectlayer.New(fake).PutObject(ctx, "bucket", "object", nil, opts)
	require.NoError(t, err)

	assert.Equal(t, objectlayer.ObjectOptions{
		VersionID: "version",
		PostPolicy: objectlayer.PostPolicyForm{
			Expiration: time.Unix(1, 0),
			Conditions: objectlayer.PostPolicyConditions{
				Items:              []objectlayer.PostPolicyCondition{{Operator: "eq", Key: "$key", Value: "object"}},
				ContentLengthRange: objectlayer.ContentLengthRange{Min: 1, Max: 2, Valid: true},
			},
		},
		Retention:                 retention,
		BypassGovernanceRetention: true,
		LegalHold:                 &legalHold,
		IfNoneMatch:               []string{"*"},
		UserDefined:               map[string]string{"key": "value"},
		PartNumber:                3,
		Quiet:                     true,
	}, fake.opts)
}

func TestGetObjectNInfo(t *testing.T) {
	ctx := testcontext.New(t)

	t.Run("precondition failed", func(t *testing.T) {
		fake := &fakeLayer{info: fullObjectInfo(t), data: "data"}
		var checked minio.ObjectInfo
		opts := minio.ObjectOptions{CheckPrecondFn: func(info minio.ObjectInfo) bool {
			checked = info
			return true
		}}

		_, err := minioobjectlayer.New(fake).GetObjectNInfo(ctx, "bucket", "object", nil, nil, 0, opts)
		require.ErrorIs(t, err, minio.PreConditionFailed{})
		assert.Equal(t, "etag", checked.ETag)
		assert.Equal(t, 1, fake.cleanup)
	})

	t.Run("read", func(t *testing.T) {
		fake := &fakeLayer{info: fullObjectInfo(t), data: "data"}
		opts := minio.ObjectOptions{CheckPrecondFn: func(minio.ObjectInfo) bool { return false }}

		reader, err := minioobjectlayer.New(fake).GetObjectNInfo(ctx, "bucket", "object", nil, nil, 0, opts)
		require.NoError(t, err)
		assert.Equal(t, "etag", reader.ObjInfo.ETag)

		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, "data", string(data))

		require.NoError(t, reader.Close())
		require.NoError(t, reader.Close())
		assert.Equal(t, 1, fake.cleanup)
	})
}
