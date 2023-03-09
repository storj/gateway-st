// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"storj.io/common/memory"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/gateway/miniogw"
	minio "storj.io/minio/cmd"
	"storj.io/minio/cmd/config/storageclass"
	xhttp "storj.io/minio/cmd/http"
	"storj.io/minio/pkg/auth"
	"storj.io/minio/pkg/hash"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite"
	"storj.io/uplink"
)

const (
	testBucket      = "test-bucket"
	testFile        = "test-file"
	testFile2       = "test-file-2"
	testFile3       = "test-file-3"
	destBucket      = "dest-bucket"
	destFile        = "dest-file"
	segmentSize     = 640 * memory.KiB
	maxKeysLimit    = 1000
	maxUploadsLimit = 1000
)

func TestMakeBucketWithLocation(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Check the error when creating bucket with empty name
		err := layer.MakeBucketWithLocation(ctx, "", minio.BucketOptions{})
		assert.Equal(t, minio.BucketNameInvalid{}, err)

		// Create a bucket with the Minio API
		err = layer.MakeBucketWithLocation(ctx, testBucket, minio.BucketOptions{})
		require.NoError(t, err)

		// Check that the bucket is created using the Uplink API
		bucket, err := project.StatBucket(ctx, testBucket)
		require.NoError(t, err)
		assert.Equal(t, testBucket, bucket.Name)
		assert.True(t, time.Since(bucket.Created) < 1*time.Minute)

		// Check the error when trying to create an existing bucket
		err = layer.MakeBucketWithLocation(ctx, testBucket, minio.BucketOptions{})
		assert.Equal(t, minio.BucketAlreadyExists{Bucket: testBucket}, err)
	})
}

func TestGetBucketInfo(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Check the error when getting info about bucket with empty name
		_, err := layer.GetBucketInfo(ctx, "")
		assert.Equal(t, minio.BucketNameInvalid{}, err)

		// Check the error when getting info about non-existing bucket
		_, err = layer.GetBucketInfo(ctx, testBucket)
		assert.Equal(t, minio.BucketNotFound{Bucket: testBucket}, err)

		// Create the bucket using the Uplink API
		info, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		// Check the bucket info using the Minio API
		bucket, err := layer.GetBucketInfo(ctx, testBucket)
		require.NoError(t, err)
		assert.Equal(t, testBucket, bucket.Name)
		assert.Equal(t, info.Created, bucket.Created)
	})
}

func maybeAbortPartUpload(t *testing.T, err error, part *uplink.PartUpload) {
	if err != nil {
		assert.NoError(t, part.Abort())
	}
}

func maybeAbortMultipartUpload(ctx context.Context, t *testing.T, err error, project *uplink.Project, bucket, key, uploadID string) {
	if err != nil {
		assert.NoError(t, err, project.AbortUpload(ctx, bucket, key, uploadID))
	}
}

func addPendingMultipartUpload(ctx context.Context, t *testing.T, project *uplink.Project, bucket *uplink.Bucket) {
	for i := 0; i < 2; i++ {
		upload, err := project.BeginUpload(ctx, bucket.Name, testFile2, nil)
		require.NoError(t, err)

		t.Logf("%d: started upload of %s with ID=%s", i, upload.Key, upload.UploadID)

		for j := uint32(0); j < 3; j++ {
			part, err := project.UploadPart(ctx, bucket.Name, testFile2, upload.UploadID, j)
			maybeAbortPartUpload(t, err, part)
			maybeAbortMultipartUpload(ctx, t, err, project, bucket.Name, testFile2, upload.UploadID)
			require.NoError(t, err)

			t.Logf("%d/%d: started part upload", i, part.Info().PartNumber)

			_, err = part.Write(make([]byte, 4*memory.KiB))
			maybeAbortPartUpload(t, err, part)
			maybeAbortMultipartUpload(ctx, t, err, project, bucket.Name, testFile2, upload.UploadID)
			require.NoError(t, err)

			err = part.Commit()
			maybeAbortPartUpload(t, err, part)
			maybeAbortMultipartUpload(ctx, t, err, project, bucket.Name, testFile2, upload.UploadID)
			require.NoError(t, err)

			t.Logf("%d/%d: finished part upload (uploaded %d bytes)", i, part.Info().PartNumber, part.Info().Size)
		}

		t.Logf("%d: finished uploading parts of %s (ID=%s)", i, upload.Key, upload.UploadID)
	}
}

func TestDeleteBucket(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		{
			// Check the error when deleting bucket with empty name
			err := layer.DeleteBucket(ctx, "", false)
			assert.Equal(t, minio.BucketNameInvalid{}, err)

			// Check the error when deleting non-existing bucket
			err = layer.DeleteBucket(ctx, testBucket, false)
			assert.Equal(t, minio.BucketNotFound{Bucket: testBucket}, err)

			// Create a bucket with a file using the Uplink API
			bucket, err := project.CreateBucket(ctx, testBucket)
			require.NoError(t, err)

			_, err = createFile(ctx, project, bucket.Name, testFile, nil, nil)
			require.NoError(t, err)

			// Check the error when deleting non-empty bucket
			err = layer.DeleteBucket(ctx, testBucket, false)
			assert.Equal(t, minio.BucketNotEmpty{Bucket: testBucket}, err)

			// Delete the file using the Uplink API, so the bucket becomes empty
			_, err = project.DeleteObject(ctx, bucket.Name, testFile)
			require.NoError(t, err)

			// Delete the bucket info using the Minio API
			err = layer.DeleteBucket(ctx, testBucket, false)
			require.NoError(t, err)

			// Check that the bucket is deleted using the Uplink API
			_, err = project.StatBucket(ctx, testBucket)
			assert.True(t, errors.Is(err, uplink.ErrBucketNotFound))
		}
		{
			// Create a bucket with a file using the Uplink API
			bucket, err := project.CreateBucket(ctx, testBucket)
			require.NoError(t, err)

			_, err = createFile(ctx, project, bucket.Name, testFile, nil, nil)
			require.NoError(t, err)

			// Check deleting bucket with force flag
			err = layer.DeleteBucket(ctx, testBucket, true)
			require.NoError(t, err)

			// Check that the bucket is deleted using the Uplink API
			_, err = project.StatBucket(ctx, testBucket)
			assert.True(t, errors.Is(err, uplink.ErrBucketNotFound))

			// Check the error when deleting non-existing bucket
			err = layer.DeleteBucket(ctx, testBucket, true)
			assert.Equal(t, minio.BucketNotFound{Bucket: testBucket}, err)
		}
		{
			// Test deletion of the empty bucket with pending multipart uploads.
			bucket, err := project.CreateBucket(ctx, testBucket)
			require.NoError(t, err)

			addPendingMultipartUpload(ctx, t, project, bucket)

			// Initiate bucket deletion with forceDelete=false because this flag
			// isn't passed from non-minio clients to the bucket deletion
			// handler anyway.
			require.NoError(t, layer.DeleteBucket(ctx, bucket.Name, false))
		}
		{
			// Test deletion of the empty bucket with pending multipart uploads,
			// but there's an additional non-pending object.
			bucket, err := project.CreateBucket(ctx, testBucket)
			require.NoError(t, err)

			_, err = createFile(ctx, project, bucket.Name, testFile, nil, nil)
			require.NoError(t, err)

			addPendingMultipartUpload(ctx, t, project, bucket)

			assert.ErrorIs(t, layer.DeleteBucket(ctx, bucket.Name, false), minio.BucketNotEmpty{Bucket: bucket.Name})
		}
	})
}

func TestListBuckets(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Check that empty list is return if no buckets exist yet
		bucketInfos, err := layer.ListBuckets(ctx)
		require.NoError(t, err)
		assert.Empty(t, bucketInfos)

		// Create all expected buckets using the Uplink API
		bucketNames := []string{"bucket-1", "bucket-2", "bucket-3"}
		buckets := make([]*uplink.Bucket, len(bucketNames))
		for i, bucketName := range bucketNames {
			bucket, err := project.CreateBucket(ctx, bucketName)
			buckets[i] = bucket
			require.NoError(t, err)
		}

		// Check that the expected buckets can be listed using the Minio API
		bucketInfos, err = layer.ListBuckets(ctx)
		require.NoError(t, err)
		assert.Equal(t, len(bucketNames), len(bucketInfos))
		for i, bucketInfo := range bucketInfos {
			assert.Equal(t, bucketNames[i], bucketInfo.Name)
			assert.Equal(t, buckets[i].Created, bucketInfo.Created)
		}
	})
}

func TestPutObject(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		hashReader, err := hash.NewReader(bytes.NewReader([]byte("test")),
			int64(len("test")),
			"098f6bcd4621d373cade4e832627b4f6",
			"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
			int64(len("test")),
		)
		require.NoError(t, err)
		data := minio.NewPutObjReader(hashReader)

		metadata := map[string]string{
			"content-type":         "media/foo",
			"key1":                 "value1",
			"key2":                 "value2",
			xhttp.AmzObjectTagging: "key3=value3&key4=value4",
		}

		expectedMetaInfo := pb.SerializableMeta{
			ContentType: metadata["content-type"],
			UserDefined: map[string]string{
				"key1":    metadata["key1"],
				"key2":    metadata["key2"],
				"s3:tags": "key3=value3&key4=value4",
			},
		}

		// Check the error when putting an object to a bucket with empty name
		_, err = layer.PutObject(ctx, "", "", nil, minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNameInvalid{}, err)

		// Check the error when putting an object to a non-existing bucket
		_, err = layer.PutObject(ctx, testBucket, testFile, nil, minio.ObjectOptions{UserDefined: metadata})
		assert.Equal(t, minio.BucketNotFound{Bucket: testBucket}, err)

		// Create the bucket using the Uplink API
		testBucketInfo, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		// Check the error when putting an object with empty name
		_, err = layer.PutObject(ctx, testBucket, "", nil, minio.ObjectOptions{})
		assert.Equal(t, minio.ObjectNameInvalid{Bucket: testBucket}, err)

		// Put the object using the Minio API
		info, err := layer.PutObject(ctx, testBucket, testFile, data, minio.ObjectOptions{UserDefined: metadata})
		require.NoError(t, err)
		assert.Equal(t, testFile, info.Name)
		assert.Equal(t, testBucket, info.Bucket)
		assert.False(t, info.IsDir)
		assert.True(t, time.Since(info.ModTime) < 1*time.Minute)
		assert.Equal(t, data.Size(), info.Size)
		assert.NotEmpty(t, info.ETag)
		assert.Equal(t, expectedMetaInfo.ContentType, info.ContentType)

		expectedMetaInfo.UserDefined["s3:etag"] = info.ETag
		expectedMetaInfo.UserDefined["content-type"] = info.ContentType
		assert.Equal(t, expectedMetaInfo.UserDefined, info.UserDefined)

		// Check that the object is uploaded using the Uplink API
		obj, err := project.StatObject(ctx, testBucketInfo.Name, testFile)
		require.NoError(t, err)
		assert.Equal(t, testFile, obj.Key)
		assert.False(t, obj.IsPrefix)

		// TODO upload.Info() is using StreamID creation time but this value is different
		// than last segment creation time, CommitObject request should return latest info
		// about object and those values should be used with upload.Info()
		// This should be working after final fix
		// assert.Equal(t, info.ModTime, obj.Info.Created)
		assert.WithinDuration(t, info.ModTime, obj.System.Created, 1*time.Second)

		assert.Equal(t, info.Size, obj.System.ContentLength)
		assert.Equal(t, info.ETag, obj.Custom["s3:etag"])
		assert.Equal(t, info.ContentType, obj.Custom["content-type"])
		assert.EqualValues(t, info.UserDefined, obj.Custom)
	})
}

func TestPutObjectZeroBytes(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		bucket, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		h, err := hash.NewReader(
			bytes.NewReader(make([]byte, 0)),
			0,
			"d41d8cd98f00b204e9800998ecf8427e",
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			0)
		require.NoError(t, err)

		r := minio.NewPutObjReader(h)

		opts := minio.ObjectOptions{
			UserDefined: make(map[string]string),
		}

		obj, err := layer.PutObject(ctx, bucket.Name, testFile, r, opts)
		require.NoError(t, err)

		assert.Zero(t, obj.Size)

		downloaded, err := project.DownloadObject(ctx, obj.Bucket, obj.Name, nil)
		require.NoError(t, err)

		_, err = downloaded.Read(make([]byte, 1))
		assert.ErrorIs(t, err, io.EOF)

		assert.Zero(t, downloaded.Info().System.ContentLength)

		require.NoError(t, downloaded.Close())
	})
}

func TestGetObjectInfo(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Check the error when getting an object from a bucket with empty name
		_, err := layer.GetObjectInfo(ctx, "", "", minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNameInvalid{}, err)

		// Check the error when getting an object from non-existing bucket
		_, err = layer.GetObjectInfo(ctx, testBucket, testFile, minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNotFound{Bucket: testBucket}, err)

		// Create the bucket using the Uplink API
		testBucketInfo, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		// Check the error when getting an object with empty name
		_, err = layer.GetObjectInfo(ctx, testBucket, "", minio.ObjectOptions{})
		assert.Equal(t, minio.ObjectNameInvalid{Bucket: testBucket}, err)

		// Check the error when getting a non-existing object
		_, err = layer.GetObjectInfo(ctx, testBucket, testFile, minio.ObjectOptions{})
		assert.Equal(t, minio.ObjectNotFound{Bucket: testBucket, Object: testFile}, err)

		// Create the object using the Uplink API
		metadata := map[string]string{
			"content-type": "text/plain",
			"key1":         "value1",
			"key2":         "value2",
		}
		obj, err := createFile(ctx, project, testBucketInfo.Name, testFile, []byte("test"), metadata)
		require.NoError(t, err)

		// Get the object info using the Minio API
		info, err := layer.GetObjectInfo(ctx, testBucket, testFile, minio.ObjectOptions{})
		require.NoError(t, err)
		assert.Equal(t, testFile, info.Name)
		assert.Equal(t, testBucket, info.Bucket)
		assert.False(t, info.IsDir)

		// TODO upload.Info() is using StreamID creation time but this value is different
		// than last segment creation time, CommitObject request should return latest info
		// about object and those values should be used with upload.Info()
		// This should be working after final fix
		// assert.Equal(t, info.ModTime, obj.Info.Created)
		assert.WithinDuration(t, info.ModTime, obj.System.Created, 1*time.Second)

		assert.Equal(t, obj.System.ContentLength, info.Size)
		assert.Equal(t, obj.Custom["s3:etag"], info.ETag)
		assert.Equal(t, "text/plain", info.ContentType)
		assert.Equal(t, metadata, info.UserDefined)
	})
}

func TestGetObjectNInfo(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Check the error when getting an object from a bucket with empty name
		_, err := layer.GetObjectNInfo(ctx, "", "", nil, nil, 0, minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNameInvalid{}, err)

		// Check the error when getting an object from non-existing bucket
		_, err = layer.GetObjectNInfo(ctx, testBucket, testFile, nil, nil, 0, minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNotFound{Bucket: testBucket}, err)

		// Create the bucket using the Uplink API
		testBucketInfo, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		// Check the error when getting an object with empty name
		_, err = layer.GetObjectNInfo(ctx, testBucket, "", nil, nil, 0, minio.ObjectOptions{})
		assert.Equal(t, minio.ObjectNameInvalid{Bucket: testBucket}, err)

		// Check the error when getting a non-existing object
		_, err = layer.GetObjectNInfo(ctx, testBucket, testFile, nil, nil, 0, minio.ObjectOptions{})
		assert.Equal(t, minio.ObjectNotFound{Bucket: testBucket, Object: testFile}, err)

		// Create the object using the Uplink API
		metadata := map[string]string{
			"content-type": "text/plain",
			"key1":         "value1",
			"key2":         "value2",
		}
		_, err = createFile(ctx, project, testBucketInfo.Name, testFile, []byte("abcdef"), metadata)
		require.NoError(t, err)

		for i, tt := range []struct {
			rangeSpec *minio.HTTPRangeSpec
			substr    string
			err       bool
		}{
			{rangeSpec: nil, substr: "abcdef"},
			{rangeSpec: &minio.HTTPRangeSpec{Start: 0, End: 0}, substr: "a"},
			{rangeSpec: &minio.HTTPRangeSpec{Start: 3, End: 3}, substr: "d"},
			{rangeSpec: &minio.HTTPRangeSpec{Start: 0, End: -1}, substr: "abcdef"},
			{rangeSpec: &minio.HTTPRangeSpec{Start: 0, End: 100}, substr: "abcdef"},
			{rangeSpec: &minio.HTTPRangeSpec{Start: 3, End: -1}, substr: "def"},
			{rangeSpec: &minio.HTTPRangeSpec{Start: 3, End: 100}, substr: "def"},
			{rangeSpec: &minio.HTTPRangeSpec{Start: 0, End: 5}, substr: "abcdef"},
			{rangeSpec: &minio.HTTPRangeSpec{Start: 0, End: 4}, substr: "abcde"},
			{rangeSpec: &minio.HTTPRangeSpec{Start: 0, End: 3}, substr: "abcd"},
			{rangeSpec: &minio.HTTPRangeSpec{Start: 1, End: 4}, substr: "bcde"},
			{rangeSpec: &minio.HTTPRangeSpec{Start: 2, End: 5}, substr: "cdef"},
			{rangeSpec: &minio.HTTPRangeSpec{IsSuffixLength: true, Start: 0, End: -1}, substr: ""},
			{rangeSpec: &minio.HTTPRangeSpec{IsSuffixLength: true, Start: -2, End: -1}, substr: "ef"},
			{rangeSpec: &minio.HTTPRangeSpec{IsSuffixLength: true, Start: -100, End: -1}, substr: "abcdef"},
			{rangeSpec: &minio.HTTPRangeSpec{Start: -1, End: 3}, err: true},
			{rangeSpec: &minio.HTTPRangeSpec{Start: 0, End: -2}, err: true},
			{rangeSpec: &minio.HTTPRangeSpec{IsSuffixLength: true, Start: 1}, err: true},
		} {
			errTag := fmt.Sprintf("%d. %v", i, tt)

			// Get the object info using the Minio API
			reader, err := layer.GetObjectNInfo(ctx, testBucket, testFile, tt.rangeSpec, nil, 0, minio.ObjectOptions{})

			if tt.err {
				require.EqualError(t, err, minio.InvalidRange{OffsetBegin: tt.rangeSpec.Start, OffsetEnd: tt.rangeSpec.End}.Error(), errTag)
			} else if assert.NoError(t, err) {
				data, err := io.ReadAll(reader)
				require.NoError(t, err, errTag)

				err = reader.Close()
				require.NoError(t, err, errTag)

				assert.Equal(t, tt.substr, string(data), errTag)
			}
		}
	})
}

// TODO(artur): probably remove TestGetObject for good. GetObject was removed
// from cmd.ObjectLayer and cmd.GetObject adaptor that calls cmd.GetObjectNInfo
// has been introduced. However, this adaptor calculates ranges in a weird way
// where the range end is offset + length. This is mostly incorrect because, for
// example, this will always give us a byte extra for a download from nth byte
// with length m. The tests here were adjusted to adhere to this weird behavior.
// Luckily, cmd.GetObject isn't used anywhere and is supposed to plumb tests.
func TestGetObject(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Check the error when getting an object from a bucket with empty name
		err := minio.GetObject(ctx, layer, "", "", 0, 0, nil, "", minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNameInvalid{}, err)

		// Check the error when getting an object from non-existing bucket
		err = minio.GetObject(ctx, layer, testBucket, testFile, 0, 0, nil, "", minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNotFound{Bucket: testBucket}, err)

		// Create the bucket using the Uplink API
		testBucketInfo, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		// Check the error when getting an object with empty name
		err = minio.GetObject(ctx, layer, testBucket, "", 0, 0, nil, "", minio.ObjectOptions{})
		assert.Equal(t, minio.ObjectNameInvalid{Bucket: testBucket}, err)

		// Check the error when getting a non-existing object
		err = minio.GetObject(ctx, layer, testBucket, testFile, 0, 0, nil, "", minio.ObjectOptions{})
		assert.Equal(t, minio.ObjectNotFound{Bucket: testBucket, Object: testFile}, err)

		// Create the object using the Uplink API
		metadata := map[string]string{
			"content-type": "text/plain",
			"key1":         "value1",
			"key2":         "value2",
		}
		_, err = createFile(ctx, project, testBucketInfo.Name, testFile, []byte("abcdef"), metadata)
		require.NoError(t, err)

		for i, tt := range []struct {
			offset, length int64
			substr         string
			err            bool
		}{
			{
				offset: 0,
				length: -1,
				substr: "abcdef",
			},
			{
				offset: 0,
				length: 100,
				substr: "abcdef",
			},
			{
				offset: 3,
				length: -1,
				substr: "",
			},
			{
				offset: 3,
				length: 100,
				substr: "def",
			},
			{
				offset: 0,
				length: 6,
				substr: "abcdef",
			},
			{
				offset: 0,
				length: 4,
				substr: "abcde",
			},
			{
				offset: 0,
				length: 3,
				substr: "abcd",
			},
			{
				offset: 1,
				length: 3,
				substr: "bcde",
			},
			{
				offset: 2,
				length: 4,
				substr: "cdef",
			},
			{
				offset: -1,
				length: 7,
				err:    true,
			},
			{
				offset: 0,
				length: -2,
				err:    true,
			},
		} {
			errTag := fmt.Sprintf("%d. %+v", i, tt)

			var buf bytes.Buffer

			// Get the object info using the Minio API
			err = minio.GetObject(ctx, layer, testBucket, testFile, tt.offset, tt.length, &buf, "", minio.ObjectOptions{})

			if tt.err {
				assert.Error(t, err, errTag)
			} else if assert.NoError(t, err) {
				assert.Equal(t, tt.substr, buf.String(), errTag)
			}
		}
	})
}

func TestCopyObject(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Check the error when copying an object from a bucket with empty name
		_, err := layer.CopyObject(ctx, "", testFile, destBucket, destFile, minio.ObjectInfo{}, minio.ObjectOptions{}, minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNameInvalid{}, err)

		// Check the error when copying an object from non-existing bucket
		_, err = layer.CopyObject(ctx, testBucket, testFile, destBucket, destFile, minio.ObjectInfo{}, minio.ObjectOptions{}, minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNotFound{Bucket: testBucket}, err)

		// Create the source bucket using the Uplink API
		testBucketInfo, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		// Check the error when copying an object with empty name
		_, err = layer.CopyObject(ctx, testBucket, "", destBucket, destFile, minio.ObjectInfo{}, minio.ObjectOptions{}, minio.ObjectOptions{})
		assert.Equal(t, minio.ObjectNameInvalid{Bucket: testBucket}, err)

		// Create the source object using the Uplink API
		metadata := map[string]string{
			"content-type": "text/plain",
			"key1":         "value1",
			"key2":         "value2",
			"s3:etag":      "123",
		}
		obj, err := createFile(ctx, project, testBucketInfo.Name, testFile, []byte("test"), metadata)
		require.NoError(t, err)

		// Get the source object info using the Minio API
		srcInfo, err := layer.GetObjectInfo(ctx, testBucket, testFile, minio.ObjectOptions{})
		require.NoError(t, err)

		// Check the error when copying an object to a bucket with empty name
		_, err = layer.CopyObject(ctx, testBucket, testFile, "", destFile, srcInfo, minio.ObjectOptions{}, minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNameInvalid{}, err)

		// Check the error when copying an object to a non-existing bucket
		_, err = layer.CopyObject(ctx, testBucket, testFile, destBucket, destFile, srcInfo, minio.ObjectOptions{}, minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNotFound{Bucket: destBucket}, err)

		// Create the destination bucket using the Uplink API
		destBucketInfo, err := project.CreateBucket(ctx, destBucket)
		require.NoError(t, err)

		// Copy the object using the Minio API
		info, err := layer.CopyObject(ctx, testBucket, testFile, destBucket, destFile, srcInfo, minio.ObjectOptions{}, minio.ObjectOptions{})
		require.NoError(t, err)
		assert.Equal(t, destFile, info.Name)
		assert.Equal(t, destBucket, info.Bucket)
		assert.False(t, info.IsDir)

		// TODO upload.Info() is using StreamID creation time but this value is different
		// than last segment creation time, CommitObject request should return latest info
		// about object and those values should be used with upload.Info()
		// This should be working after final fix
		// assert.Equal(t, info.ModTime, obj.Info.Created)
		assert.WithinDuration(t, info.ModTime, obj.System.Created, 5*time.Second)

		assert.Equal(t, obj.System.ContentLength, info.Size)
		assert.Equal(t, "text/plain", info.ContentType)
		assert.EqualValues(t, obj.Custom, info.UserDefined)

		// Check that the destination object is uploaded using the Uplink API
		obj, err = project.StatObject(ctx, destBucketInfo.Name, destFile)
		require.NoError(t, err)
		assert.Equal(t, destFile, obj.Key)
		assert.False(t, obj.IsPrefix)

		// TODO upload.Info() is using StreamID creation time but this value is different
		// than last segment creation time, CommitObject request should return latest info
		// about object and those values should be used with upload.Info()
		// This should be working after final fix
		// assert.Equal(t, info.ModTime, obj.Info.Created)
		assert.WithinDuration(t, info.ModTime, obj.System.Created, 2*time.Second)

		assert.Equal(t, info.Size, obj.System.ContentLength)
		assert.Equal(t, info.ContentType, obj.Custom["content-type"])
		assert.EqualValues(t, info.UserDefined, obj.Custom)
	})
}

func TestCopyObjectMetadata(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Create the source bucket using the Uplink API
		testBucketInfo, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		// Create the destination bucket using the Uplink API
		destBucketInfo, err := project.CreateBucket(ctx, destBucket)
		require.NoError(t, err)

		// Create the source object using the Uplink API
		srcMetadata := map[string]string{
			"content-type": "text/plain",
			"key1":         "value1",
			"key2":         "value2",
			"s3:etag":      "123",
			"s3:tags":      "key=value",
		}
		_, err = createFile(ctx, project, testBucketInfo.Name, testFile, []byte("test"), srcMetadata)
		require.NoError(t, err)

		// Get the source object info using the Minio API
		srcInfo, err := layer.GetObjectInfo(ctx, testBucket, testFile, minio.ObjectOptions{})
		require.NoError(t, err)

		// Copy the object. Metadata and tagging copied from source to destination.
		_, err = layer.CopyObject(ctx, testBucket, testFile, destBucket, destFile, srcInfo, minio.ObjectOptions{}, minio.ObjectOptions{})
		require.NoError(t, err)
		obj, err := project.StatObject(ctx, destBucketInfo.Name, destFile)
		require.NoError(t, err)
		require.EqualValues(t, obj.Custom, srcMetadata)

		// Copy the object with metadata in request. This will be set on the copied object.
		srcInfo.UserDefined = map[string]string{
			"key3": "value3",
		}
		_, err = layer.CopyObject(ctx, testBucket, testFile, destBucket, destFile, srcInfo, minio.ObjectOptions{}, minio.ObjectOptions{})
		require.NoError(t, err)
		obj, err = project.StatObject(ctx, destBucketInfo.Name, destFile)
		require.NoError(t, err)
		require.EqualValues(t, obj.Custom, map[string]string{
			"key3":    "value3",
			"s3:etag": srcMetadata["s3:etag"],
			"s3:tags": srcMetadata["s3:tags"],
		})

		// Tagging directive "REPLACE" will set new tags on the copied object.
		srcInfo.UserDefined = map[string]string{
			xhttp.AmzTagDirective:  "REPLACE",
			xhttp.AmzObjectTagging: "key1=value1,key2=value2",
		}
		_, err = layer.CopyObject(ctx, testBucket, testFile, destBucket, destFile, srcInfo, minio.ObjectOptions{}, minio.ObjectOptions{})
		require.NoError(t, err)
		obj, err = project.StatObject(ctx, destBucketInfo.Name, destFile)
		require.NoError(t, err)
		require.EqualValues(t, obj.Custom, map[string]string{
			"s3:etag": srcMetadata["s3:etag"],
			"s3:tags": "key1=value1,key2=value2",
		})

		// Tagging directive "COPY" will copy existing tags to the copied object.
		srcInfo.UserDefined = map[string]string{
			xhttp.AmzTagDirective:  "COPY",
			xhttp.AmzObjectTagging: "no=effect",
		}
		_, err = layer.CopyObject(ctx, testBucket, testFile, destBucket, destFile, srcInfo, minio.ObjectOptions{}, minio.ObjectOptions{})
		require.NoError(t, err)
		obj, err = project.StatObject(ctx, destBucketInfo.Name, destFile)
		require.NoError(t, err)
		require.EqualValues(t, obj.Custom, map[string]string{
			"s3:etag": srcMetadata["s3:etag"],
			"s3:tags": srcMetadata["s3:tags"],
		})
	})
}

func TestCopyObjectSameSourceAndDestUpdatesMetadata(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Explicitly disable CopyObject, as we want to test that copying metadata
		// for same source and destination works even if the endpoint is disabled
		// as it's an exceptional case.
		s3Compatibility := miniogw.S3CompatibilityConfig{
			DisableCopyObject:            true,
			IncludeCustomMetadataListing: true,
			MaxKeysLimit:                 maxKeysLimit,
		}

		layer, err := miniogw.NewStorjGateway(s3Compatibility).NewGatewayLayer(auth.Credentials{})
		require.NoError(t, err)

		defer func() { require.NoError(t, layer.Shutdown(ctx)) }()

		// Create the source bucket using the Uplink API
		testBucketInfo, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		// Create the source object using the Uplink API
		metadata := map[string]string{
			"content-type": "text/plain",
			"mykey":        "originalvalue",
			"s3:etag":      "123",
			"s3:tags":      "key=value",
		}
		_, err = createFile(ctx, project, testBucketInfo.Name, testFile, []byte("test"), metadata)
		require.NoError(t, err)

		// Get the source object info using the Minio API
		srcInfo, err := layer.GetObjectInfo(ctx, testBucket, testFile, minio.ObjectOptions{})
		require.NoError(t, err)

		// Copy the object. Metadata and tagging copied from source to destination.
		_, err = layer.CopyObject(ctx, testBucket, testFile, testBucket, testFile, srcInfo, minio.ObjectOptions{}, minio.ObjectOptions{})
		require.NoError(t, err)
		obj, err := project.StatObject(ctx, testBucketInfo.Name, testFile)
		require.NoError(t, err)
		require.EqualValues(t, obj.Custom, metadata)

		// Copy the object with new metadata in request. This will be set on the copied object.
		srcInfo.UserDefined = map[string]string{
			"mykey": "newvalue",
		}
		_, err = layer.CopyObject(ctx, testBucket, testFile, testBucket, testFile, srcInfo, minio.ObjectOptions{}, minio.ObjectOptions{})
		require.NoError(t, err)
		obj, err = project.StatObject(ctx, testBucketInfo.Name, testFile)
		require.NoError(t, err)
		require.EqualValues(t, obj.Custom, map[string]string{
			"mykey":   "newvalue",
			"s3:etag": metadata["s3:etag"],
			"s3:tags": metadata["s3:tags"],
		})
	})
}

func TestDeleteObject(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Check the error when deleting an object from a bucket with empty name
		deleted, err := layer.DeleteObject(ctx, "", "", minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNameInvalid{}, err)
		assert.Empty(t, deleted)

		// Check the error when deleting an object from non-existing bucket
		deleted, err = layer.DeleteObject(ctx, testBucket, testFile, minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNotFound{Bucket: testBucket}, err)
		assert.Empty(t, deleted)

		// Create the bucket using the Uplink API
		testBucketInfo, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		// Check the error when deleting an object with empty name
		deleted, err = layer.DeleteObject(ctx, testBucket, "", minio.ObjectOptions{})
		assert.Equal(t, minio.ObjectNameInvalid{Bucket: testBucket}, err)
		assert.Empty(t, deleted)

		// Check that no error being returned when deleting a non-existing object
		_, err = layer.DeleteObject(ctx, testBucket, testFile, minio.ObjectOptions{})
		require.NoError(t, err)

		// Create the object using the Uplink API
		_, err = createFile(ctx, project, testBucketInfo.Name, testFile, nil, nil)
		require.NoError(t, err)

		// Delete the object info using the Minio API
		deleted, err = layer.DeleteObject(ctx, testBucket, testFile, minio.ObjectOptions{})
		require.NoError(t, err)
		assert.Equal(t, testBucket, deleted.Bucket)
		assert.Equal(t, testFile, deleted.Name)

		// Check that the object is deleted using the Uplink API
		_, err = project.StatObject(ctx, testBucketInfo.Name, testFile)
		assert.True(t, errors.Is(err, uplink.ErrObjectNotFound))
	})
}

func TestDeleteObjects(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Check the error when deleting an object from a bucket with empty name
		deletedObjects, deleteErrors := layer.DeleteObjects(ctx, "", []minio.ObjectToDelete{{ObjectName: testFile}}, minio.ObjectOptions{})
		require.Len(t, deleteErrors, 1)
		assert.Equal(t, minio.BucketNameInvalid{}, deleteErrors[0])
		require.Len(t, deletedObjects, 1)
		assert.Empty(t, deletedObjects[0])

		// Check the error when deleting an object from non-existing bucket
		deletedObjects, deleteErrors = layer.DeleteObjects(ctx, testBucket, []minio.ObjectToDelete{{ObjectName: testFile}}, minio.ObjectOptions{})
		require.Len(t, deleteErrors, 1)
		assert.Equal(t, minio.BucketNotFound{Bucket: testBucket}, deleteErrors[0])
		require.Len(t, deletedObjects, 1)
		assert.Empty(t, deletedObjects[0])

		// Create the bucket using the Uplink API
		testBucketInfo, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		// Check the error when deleting an object with empty name
		deletedObjects, deleteErrors = layer.DeleteObjects(ctx, testBucket, []minio.ObjectToDelete{{ObjectName: ""}}, minio.ObjectOptions{})
		require.Len(t, deleteErrors, 1)
		assert.Equal(t, minio.ObjectNameInvalid{Bucket: testBucket}, deleteErrors[0])
		require.Len(t, deletedObjects, 1)
		assert.Empty(t, deletedObjects[0])

		// Check that there is NO error when deleting a non-existing object
		deletedObjects, deleteErrors = layer.DeleteObjects(ctx, testBucket, []minio.ObjectToDelete{{ObjectName: testFile}}, minio.ObjectOptions{})
		require.Len(t, deleteErrors, 1)
		assert.Empty(t, deleteErrors[0])
		require.Len(t, deletedObjects, 1)
		assert.Equal(t, deletedObjects, []minio.DeletedObject{{ObjectName: testFile}})

		// Create the 3 objects using the Uplink API
		_, err = createFile(ctx, project, testBucketInfo.Name, testFile, nil, nil)
		require.NoError(t, err)
		_, err = createFile(ctx, project, testBucketInfo.Name, testFile2, nil, nil)
		require.NoError(t, err)
		_, err = createFile(ctx, project, testBucketInfo.Name, testFile3, nil, nil)
		require.NoError(t, err)

		// Delete the 1st and the 3rd object using the Minio API
		deletedObjects, deleteErrors = layer.DeleteObjects(ctx, testBucket, []minio.ObjectToDelete{{ObjectName: testFile}, {ObjectName: testFile3}}, minio.ObjectOptions{})
		require.Len(t, deleteErrors, 2)
		require.NoError(t, deleteErrors[0])
		require.NoError(t, deleteErrors[1])
		require.Len(t, deletedObjects, 2)
		assert.NotEmpty(t, deletedObjects[0])
		assert.NotEmpty(t, deletedObjects[1])

		// Check using the Uplink API that the 1st and the 3rd objects are deleted, but the 2nd is still there
		_, err = project.StatObject(ctx, testBucketInfo.Name, testFile)
		assert.True(t, errors.Is(err, uplink.ErrObjectNotFound))
		_, err = project.StatObject(ctx, testBucketInfo.Name, testFile2)
		require.NoError(t, err)
		_, err = project.StatObject(ctx, testBucketInfo.Name, testFile3)
		assert.True(t, errors.Is(err, uplink.ErrObjectNotFound))
	})
}

type listObjectsFunc func(ctx context.Context, layer minio.ObjectLayer, bucket, prefix, marker, delimiter string, maxKeys int) ([]string, []minio.ObjectInfo, string, string, bool, error)

func TestListObjects(t *testing.T) {
	t.Parallel()

	f := func(ctx context.Context, layer minio.ObjectLayer, bucket, prefix, marker, delimiter string, maxKeys int) ([]string, []minio.ObjectInfo, string, string, bool, error) {
		list, err := layer.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
		if err != nil {
			return nil, nil, "", "", false, err
		}
		return list.Prefixes, list.Objects, marker, list.NextMarker, list.IsTruncated, nil
	}

	t.Run("once", func(t *testing.T) {
		t.Parallel()

		testListObjects(t, f)
	})
	t.Run("loop", func(t *testing.T) {
		t.Parallel()

		testListObjectsLoop(t, f)
	})
	t.Run("stat", func(t *testing.T) {
		t.Parallel()

		testListObjectsStatLoop(t, f)
	})
	t.Run("arbitrary prefix and delimiter", func(t *testing.T) {
		t.Parallel()

		testListObjectsArbitraryPrefixDelimiter(t, f, "#")
	})
	t.Run("arbitrary prefix and delimiter 2", func(t *testing.T) {
		t.Parallel()

		testListObjectsArbitraryPrefixDelimiter(t, f, "$$")
	})
	t.Run("limits", func(t *testing.T) {
		t.Parallel()

		testListObjectsLimits(t, f)
	})
}

func TestListObjectsV2(t *testing.T) {
	t.Parallel()

	f := func(ctx context.Context, layer minio.ObjectLayer, bucket, prefix, marker, delimiter string, maxKeys int) ([]string, []minio.ObjectInfo, string, string, bool, error) {
		list, err := layer.ListObjectsV2(ctx, bucket, prefix, marker, delimiter, maxKeys, false, "")
		if err != nil {
			return nil, nil, "", "", false, err
		}
		return list.Prefixes, list.Objects, list.ContinuationToken, list.NextContinuationToken, list.IsTruncated, nil
	}

	t.Run("once", func(t *testing.T) {
		t.Parallel()

		testListObjects(t, f)
	})
	t.Run("loop", func(t *testing.T) {
		t.Parallel()

		testListObjectsLoop(t, f)
	})
	t.Run("stat", func(t *testing.T) {
		t.Parallel()

		testListObjectsStatLoop(t, f)
	})
	t.Run("arbitrary prefix and delimiter", func(t *testing.T) {
		t.Parallel()

		testListObjectsArbitraryPrefixDelimiter(t, f, "s")
	})
	t.Run("arbitrary prefix and delimiter 2", func(t *testing.T) {
		t.Parallel()

		testListObjectsArbitraryPrefixDelimiter(t, f, "%separator%")
	})
	t.Run("limits", func(t *testing.T) {
		t.Parallel()

		testListObjectsLimits(t, f)
	})
}

func testListObjects(t *testing.T, listObjects listObjectsFunc) {
	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Check the error when listing objects in a bucket with empty name
		_, _, _, _, _, err := listObjects(ctx, layer, "", "", "", "/", maxKeysLimit)
		assert.Equal(t, minio.BucketNameInvalid{}, err)

		// Check the error when listing objects in a non-existing bucket
		_, _, _, _, _, err = listObjects(ctx, layer, testBucket, "", "", "", maxKeysLimit)
		assert.Equal(t, minio.BucketNotFound{Bucket: testBucket}, err)

		// Check the error when listing objects in a non-existing bucket. This
		// time try to list with a prefix that will trigger a prefix-optimized
		// listing code path.
		_, _, _, _, _, err = listObjects(ctx, layer, testBucket, "p", "", "", maxKeysLimit)
		assert.Equal(t, minio.BucketNotFound{Bucket: testBucket}, err)

		// Create the bucket and files using the Uplink API
		testBucketInfo, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		filePaths := []string{
			"a", "aa", "b", "bb", "c",
			"a/xa", "a/xaa", "a/xb", "a/xbb", "a/xc",
			"b/ya", "b/yaa", "b/yb", "b/ybb", "b/yc",
			"i", "i/i", "ii", "j", "j/i", "k", "kk", "l",
			"m/i", "mm", "n/i", "oo",
		}

		type expected struct {
			object   *uplink.Object
			metadata map[string]string
		}

		files := make(map[string]expected, len(filePaths))

		metadata := map[string]string{
			"content-type": "text/plain",
			"key1":         "value1",
			"key2":         "value2",
		}
		for _, filePath := range filePaths {
			file, err := createFile(ctx, project, testBucketInfo.Name, filePath, []byte("test"), metadata)
			files[filePath] = expected{
				object:   file,
				metadata: metadata,
			}
			require.NoError(t, err)
		}

		sort.Strings(filePaths)

		for i, tt := range []struct {
			name      string
			prefix    string
			marker    string
			delimiter string
			maxKeys   int
			more      bool
			prefixes  []string
			objects   []string
		}{
			{
				name:      "Basic non-recursive",
				delimiter: "/",
				maxKeys:   maxKeysLimit,
				prefixes:  []string{"a/", "b/", "i/", "j/", "m/", "n/"},
				objects:   []string{"a", "aa", "b", "bb", "c", "i", "ii", "j", "k", "kk", "l", "mm", "oo"},
			}, {
				name:      "Basic non-recursive with non-existing mark",
				marker:    "`",
				delimiter: "/",
				maxKeys:   maxKeysLimit,
				prefixes:  []string{"a/", "b/", "i/", "j/", "m/", "n/"},
				objects:   []string{"a", "aa", "b", "bb", "c", "i", "ii", "j", "k", "kk", "l", "mm", "oo"},
			}, {
				name:      "Basic non-recursive with existing mark",
				marker:    "b",
				delimiter: "/",
				maxKeys:   maxKeysLimit,
				prefixes:  []string{"b/", "i/", "j/", "m/", "n/"},
				objects:   []string{"bb", "c", "i", "ii", "j", "k", "kk", "l", "mm", "oo"},
			}, {
				name:      "Basic non-recursive with last mark",
				marker:    "oo",
				delimiter: "/",
				maxKeys:   maxKeysLimit,
			}, {
				name:      "Basic non-recursive with past last mark",
				marker:    "ooa",
				delimiter: "/",
				maxKeys:   maxKeysLimit,
			}, {
				name:      "Basic non-recursive with max key limit of 0",
				marker:    "",
				delimiter: "/",
				maxKeys:   0,
				more:      false,
				prefixes:  nil,
				objects:   nil,
			}, {
				name:      "Basic non-recursive with max key limit of 1",
				delimiter: "/",
				maxKeys:   1,
				more:      true,
				objects:   []string{"a"},
			}, {
				name:      "Basic non-recursive with max key limit of 1 with non-existing mark",
				marker:    "`",
				delimiter: "/",
				maxKeys:   1,
				more:      true,
				objects:   []string{"a"},
			}, {
				name:      "Basic non-recursive with max key limit of 1 with existing mark",
				marker:    "aa",
				delimiter: "/",
				maxKeys:   1,
				more:      true,
				objects:   []string{"b"},
			}, {
				name:      "Basic non-recursive with max key limit of 1 with last mark",
				marker:    "oo",
				delimiter: "/",
				maxKeys:   1,
			}, {
				name:      "Basic non-recursive with max key limit of 1 past last mark",
				marker:    "ooa",
				delimiter: "/",
				maxKeys:   1,
			}, {
				name:      "Basic non-recursive with max key limit of 2",
				delimiter: "/",
				maxKeys:   2,
				more:      true,
				prefixes:  []string{"a/"},
				objects:   []string{"a"},
			}, {
				name:      "Basic non-recursive with max key limit of 2 with non-existing mark",
				marker:    "`",
				delimiter: "/",
				maxKeys:   2,
				more:      true,
				prefixes:  []string{"a/"},
				objects:   []string{"a"},
			}, {
				name:      "Basic non-recursive with max key limit of 2 with existing mark",
				marker:    "aa",
				delimiter: "/",
				maxKeys:   2,
				more:      true,
				prefixes:  []string{"b/"},
				objects:   []string{"b"},
			}, {
				name:      "Basic non-recursive with max key limit of 2 with mark right before the end",
				marker:    "nm",
				delimiter: "/",
				maxKeys:   2,
				objects:   []string{"oo"},
			}, {
				name:      "Basic non-recursive with max key limit of 2 with last mark",
				marker:    "oo",
				delimiter: "/",
				maxKeys:   2,
			}, {
				name:      "Basic non-recursive with max key limit of 2 past last mark",
				marker:    "ooa",
				delimiter: "/",
				maxKeys:   2,
			}, {
				name:      "Prefix non-recursive",
				prefix:    "a/",
				delimiter: "/",
				maxKeys:   maxKeysLimit,
				objects:   []string{"xa", "xaa", "xb", "xbb", "xc"},
			}, {
				name:      "Prefix non-recursive with mark",
				prefix:    "a/",
				marker:    "xb",
				delimiter: "/",
				maxKeys:   maxKeysLimit,
				objects:   []string{"xbb", "xc"},
			}, {
				name:      "Prefix non-recursive with mark and max keys",
				prefix:    "a/",
				marker:    "xaa",
				delimiter: "/",
				maxKeys:   2,
				more:      true,
				objects:   []string{"xb", "xbb"},
			}, {
				name:    "Basic recursive",
				maxKeys: maxKeysLimit,
				objects: filePaths,
			}, {
				name:     "Basic recursive with max key limit of 0",
				marker:   "",
				maxKeys:  0,
				more:     false,
				objects:  nil,
				prefixes: nil,
			}, {
				name:    "Basic recursive with mark and max keys",
				marker:  "a/xbb",
				maxKeys: 5,
				more:    true,
				objects: []string{"a/xc", "aa", "b", "b/ya", "b/yaa"},
			}, {
				name:     "list as stat, recursive, object, prefix, and object-with-prefix exist",
				prefix:   "i",
				more:     true,
				maxKeys:  maxKeysLimit,
				prefixes: nil,
				objects:  []string{"i"},
			}, {
				name:      "list as stat, nonrecursive, object, prefix, and object-with-prefix exist",
				prefix:    "i",
				delimiter: "/",
				maxKeys:   maxKeysLimit,
				more:      true,
				prefixes:  []string{"i/"},
				objects:   []string{"i"},
			}, {
				name:     "list as stat, recursive, object and prefix exist, no object-with-prefix",
				prefix:   "j",
				maxKeys:  maxKeysLimit,
				more:     true,
				prefixes: nil,
				objects:  []string{"j"},
			}, {
				name:      "list as stat, nonrecursive, object and prefix exist, no object-with-prefix",
				prefix:    "j",
				delimiter: "/",
				maxKeys:   maxKeysLimit,
				more:      true,
				prefixes:  []string{"j/"},
				objects:   []string{"j"},
			}, {
				name:     "list as stat, recursive, object and object-with-prefix exist, no prefix",
				prefix:   "k",
				maxKeys:  maxKeysLimit,
				more:     true,
				prefixes: nil,
				objects:  []string{"k"},
			}, {
				name:      "list as stat, nonrecursive, object and object-with-prefix exist, no prefix",
				prefix:    "k",
				delimiter: "/",
				maxKeys:   maxKeysLimit,
				more:      true,
				prefixes:  nil,
				objects:   []string{"k"},
			}, {
				name:     "list as stat, recursive, object exists, no object-with-prefix or prefix",
				prefix:   "l",
				maxKeys:  maxKeysLimit,
				more:     true,
				prefixes: nil,
				objects:  []string{"l"},
			}, {
				name:      "list as stat, nonrecursive, object exists, no object-with-prefix or prefix",
				prefix:    "l",
				delimiter: "/",
				maxKeys:   maxKeysLimit,
				more:      true,
				prefixes:  nil,
				objects:   []string{"l"},
			}, {
				name:     "list as stat, recursive, prefix, and object-with-prefix exist, no object (fallback to exhaustive)",
				prefix:   "m",
				maxKeys:  maxKeysLimit,
				prefixes: nil,
				objects:  []string{"m/i", "mm"},
			}, {
				name:      "list as stat, nonrecursive, prefix, and object-with-prefix exist, no object",
				prefix:    "m",
				delimiter: "/",
				maxKeys:   maxKeysLimit,
				more:      true,
				prefixes:  []string{"m/"},
				objects:   nil,
			}, {
				name:     "list as stat, recursive, prefix exists, no object-with-prefix, no object (fallback to exhaustive)",
				prefix:   "n",
				maxKeys:  maxKeysLimit,
				prefixes: nil,
				objects:  []string{"n/i"},
			}, {
				name:      "list as stat, nonrecursive, prefix exists, no object-with-prefix, no object",
				prefix:    "n",
				delimiter: "/",
				maxKeys:   maxKeysLimit,
				more:      true,
				prefixes:  []string{"n/"},
				objects:   nil,
			}, {
				name:     "list as stat, recursive, object-with-prefix exists, no prefix, no object (fallback to exhaustive)",
				prefix:   "o",
				maxKeys:  maxKeysLimit,
				prefixes: nil,
				objects:  []string{"oo"},
			}, {
				name:      "list as stat, nonrecursive, object-with-prefix exists, no prefix, no object (fallback to exhaustive)",
				prefix:    "o",
				delimiter: "/",
				maxKeys:   maxKeysLimit,
				prefixes:  nil,
				objects:   []string{"oo"},
			}, {
				name:     "list as stat, recursive, no object-with-prefix or prefix or object",
				prefix:   "p",
				prefixes: nil,
				objects:  nil,
			}, {
				name:      "list as stat, nonrecursive, no object-with-prefix or prefix or object",
				prefix:    "p",
				delimiter: "/",
				maxKeys:   maxKeysLimit,
				prefixes:  nil,
				objects:   nil,
			},
		} {
			errTag := fmt.Sprintf("%d. %+v", i, tt)

			// Check that the expected objects can be listed using the Minio API
			prefixes, objects, marker, _, isTruncated, err := listObjects(ctx, layer, testBucket, tt.prefix, tt.marker, tt.delimiter, tt.maxKeys)
			require.NoError(t, err, errTag)
			assert.Equal(t, tt.more, isTruncated, errTag)
			assert.Equal(t, tt.marker, marker, errTag)
			assert.Equal(t, tt.prefixes, prefixes, errTag)
			require.Equal(t, len(tt.objects), len(objects), errTag)
			for i, objectInfo := range objects {
				path := objectInfo.Name
				expected, found := files[path]

				if assert.True(t, found) {
					if tt.prefix != "" && strings.HasSuffix(tt.prefix, "/") {
						assert.Equal(t, tt.prefix+tt.objects[i], objectInfo.Name, errTag)
					} else {
						assert.Equal(t, tt.objects[i], objectInfo.Name, errTag)
					}
					assert.Equal(t, testBucket, objectInfo.Bucket, errTag)
					assert.False(t, objectInfo.IsDir, errTag)

					// TODO upload.Info() is using StreamID creation time but this value is different
					// than last segment creation time, CommitObject request should return latest info
					// about object and those values should be used with upload.Info()
					// This should be working after final fix
					// assert.Equal(t, info.ModTime, obj.Info.Created)
					assert.WithinDuration(t, objectInfo.ModTime, expected.object.System.Created, 1*time.Second)

					assert.Equal(t, expected.object.System.ContentLength, objectInfo.Size, errTag)
					// assert.Equal(t, hex.EncodeToString(obj.Checksum), objectInfo.ETag, errTag)
					assert.Equal(t, expected.metadata["content-type"], objectInfo.ContentType, errTag)
					assert.Equal(t, expected.metadata, objectInfo.UserDefined, errTag)
				}
			}
		}
	})
}

func testListObjectsLoop(t *testing.T, listObjects listObjectsFunc) {
	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		testBucketInfo, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		wantObjects := make(map[string]struct{})
		wantObjectsWithPrefix := make(map[string]struct{})

		wantPrefixes := make(map[string]struct{})

		for i := 1; i <= 5; i++ {
			for j := 1; j <= 10; j++ {
				file, err := createFile(ctx, project, testBucketInfo.Name, fmt.Sprintf("1/%d/%d/o", i, j), nil, nil)
				require.NoError(t, err)

				wantObjects[file.Key] = struct{}{}

				if i == 3 {
					wantObjectsWithPrefix[file.Key] = struct{}{}
					wantPrefixes[fmt.Sprintf("1/%d/%d/", i, j)] = struct{}{}
				}
			}
		}

		wantNonRecursiveObjects := make(map[string]struct{})

		for i := 0; i < 10; i++ {
			file, err := createFile(ctx, project, testBucketInfo.Name, fmt.Sprintf("1/3/%d", i), nil, nil)
			require.NoError(t, err)

			wantObjects[file.Key] = struct{}{}
			wantObjectsWithPrefix[file.Key] = struct{}{}
			wantNonRecursiveObjects[file.Key] = struct{}{}
		}

		for _, tt := range [...]struct {
			name         string
			prefix       string
			delimiter    string
			limit        int
			wantPrefixes map[string]struct{}
			wantObjects  map[string]struct{}
		}{
			{
				name:         "recursive + no prefix",
				prefix:       "",
				delimiter:    "",
				limit:        2,
				wantPrefixes: map[string]struct{}{},
				wantObjects:  wantObjects,
			},
			{
				name:         "recursive + with prefix",
				prefix:       "1/3/",
				delimiter:    "",
				limit:        1,
				wantPrefixes: map[string]struct{}{},
				wantObjects:  wantObjectsWithPrefix,
			},
			{
				name:         "non-recursive + no prefix",
				prefix:       "",
				delimiter:    "/",
				limit:        2,
				wantPrefixes: map[string]struct{}{"1/": {}},
				wantObjects:  map[string]struct{}{},
			},
			{
				name:         "non-recursive + with prefix",
				prefix:       "1/3/",
				delimiter:    "/",
				limit:        1,
				wantPrefixes: wantPrefixes,
				wantObjects:  wantNonRecursiveObjects,
			},
		} {
			prefixes, objects, err := listBucketObjects(ctx, listObjects, layer, tt.prefix, tt.delimiter, tt.limit, "")
			require.NoError(t, err, tt.name)
			assert.Equal(t, tt.wantPrefixes, prefixes, tt.name)
			assert.Equal(t, tt.wantObjects, objects, tt.name)
		}
	})
}

func testListObjectsStatLoop(t *testing.T, listObjects listObjectsFunc) {
	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		testBucketInfo, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		for i := 1; i <= 2; i++ {
			for j := 1; j <= 4; j++ {
				_, err = createFile(ctx, project, testBucketInfo.Name, fmt.Sprintf("1/%d/%d", i, j), nil, nil)
				require.NoError(t, err)
				_, err = createFile(ctx, project, testBucketInfo.Name, fmt.Sprintf("1/%d/%d/o", i, j), nil, nil)
				require.NoError(t, err)
			}
		}

		for _, tt := range [...]struct {
			name         string
			prefix       string
			delimiter    string
			limit        int
			startAfter   string
			wantPrefixes bool
			wantObjects  bool
		}{
			{
				name:         "recursive + unlimited",
				prefix:       "1/1/1",
				delimiter:    "",
				limit:        2,
				startAfter:   "",
				wantPrefixes: false,
				wantObjects:  true,
			},
			{
				name:         "recursive + limited (maxKeys=0)",
				prefix:       "1/1/1",
				delimiter:    "",
				limit:        0,
				startAfter:   "",
				wantPrefixes: false,
				wantObjects:  false,
			},
			{
				name:         "recursive + limited",
				prefix:       "1/1/2",
				delimiter:    "",
				limit:        1,
				startAfter:   "",
				wantPrefixes: false,
				wantObjects:  true,
			},
			{
				name:         "non-recursive + unlimited",
				prefix:       "1/1/3",
				delimiter:    "/",
				limit:        maxKeysLimit,
				startAfter:   "",
				wantPrefixes: true,
				wantObjects:  true,
			},
			{
				name:         "non-recursive + limited (maxKeys=0)",
				prefix:       "1/1/4",
				delimiter:    "/",
				limit:        0,
				startAfter:   "",
				wantPrefixes: false,
				wantObjects:  false,
			},
			{
				name:         "non-recursive + limited",
				prefix:       "1/1/4",
				delimiter:    "/",
				limit:        1,
				startAfter:   "",
				wantPrefixes: true,
				wantObjects:  true,
			},
			{
				name:         "startAfter implies object is listed after prefix",
				prefix:       "1/2/1",
				delimiter:    "/",
				limit:        2,
				startAfter:   "1/2/1/",
				wantPrefixes: false,
				wantObjects:  false,
			},
			{
				name:         "startAfter is garbage",
				prefix:       "1/2/2",
				delimiter:    "/",
				limit:        1,
				startAfter:   "invalid",
				wantPrefixes: false,
				wantObjects:  false,
			},
			{
				name:         "startAfter replaces continuationToken",
				prefix:       "1/2/3",
				delimiter:    "/",
				limit:        maxKeysLimit,
				startAfter:   "1/2/3",
				wantPrefixes: true,
				wantObjects:  false,
			},
		} {
			prefixes, objects, err := listBucketObjects(ctx, listObjects, layer, tt.prefix, tt.delimiter, tt.limit, tt.startAfter)
			require.NoError(t, err, tt.name)

			if tt.wantPrefixes {
				assert.Equal(t, map[string]struct{}{tt.prefix + "/": {}}, prefixes, tt.name)
			} else {
				assert.Empty(t, prefixes, tt.name)
			}

			if tt.wantObjects {
				expected := map[string]struct{}{tt.prefix: {}}
				// We will fall back to exhaustive, and the exhaustive listing
				// will find everything.
				if tt.delimiter == "" {
					expected[tt.prefix+"/"+"o"] = struct{}{}
				}
				assert.Equal(t, expected, objects, tt.name)
			} else {
				assert.Empty(t, objects, tt.name)
			}
		}
	})
}

func testListObjectsArbitraryPrefixDelimiter(t *testing.T, listObjects listObjectsFunc, sep string) {
	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		testBucketInfo, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		for _, key := range []string{
			makeSeparatedPath(sep, "photos", "2022", "January", "photo_2.jpg"),
			makeSeparatedPath(sep, "photos", "2022", "February", "photo_4.jpg"),
			makeSeparatedPath(sep, "photos", "photo_1.jpg"),
			makeSeparatedPath(sep, "photos", "2022", "January", "photo_3.jpg"),
		} {
			_, err := createFile(ctx, project, testBucketInfo.Name, key, nil, nil)
			require.NoError(t, err)
		}

		for _, tt := range []struct {
			name      string
			prefix    string
			delimiter string
			maxKeys   int

			prefixes   []string
			lenObjects int
			nextMarker string
		}{
			{
				name:      "no prefix or delimiter",
				prefix:    "",
				delimiter: "",
				maxKeys:   maxKeysLimit,

				prefixes:   nil,
				lenObjects: 4,
				nextMarker: "",
			},
			{
				name:      "no prefix or delimiter with maxKeys=0",
				prefix:    "",
				delimiter: "",
				maxKeys:   0,

				prefixes:   nil,
				lenObjects: 0,
				nextMarker: "",
			},
			{
				name:      "no prefix or delimiter with maxKeys=4",
				prefix:    "",
				delimiter: "",
				maxKeys:   4,

				prefixes:   nil,
				lenObjects: 4,
				nextMarker: "",
			},
			{
				name:      "no prefix or delimiter with maxKeys=3",
				prefix:    "",
				delimiter: "",
				maxKeys:   3,

				prefixes:   nil,
				lenObjects: 3,
				nextMarker: makeSeparatedPath(sep, "photos", "2022", "January", "photo_3.jpg"),
			},
			{
				name:      "no prefix or delimiter with maxKeys=2",
				prefix:    "",
				delimiter: "",
				maxKeys:   2,

				prefixes:   nil,
				lenObjects: 2,
				nextMarker: makeSeparatedPath(sep, "photos", "2022", "January", "photo_2.jpg"),
			},
			{
				name:      "no prefix or delimiter with maxKeys=1",
				prefix:    "",
				delimiter: "",
				maxKeys:   1,

				prefixes:   nil,
				lenObjects: 1,
				nextMarker: makeSeparatedPath(sep, "photos", "2022", "February", "photo_4.jpg"),
			},
			{
				name:      "getting months",
				prefix:    makeSeparatedPath(sep, "photos", "2022") + sep,
				delimiter: sep,
				maxKeys:   maxKeysLimit,

				prefixes: []string{
					makeSeparatedPath(sep, "photos", "2022", "February") + sep,
					makeSeparatedPath(sep, "photos", "2022", "January") + sep,
				},
				lenObjects: 0,
				nextMarker: "",
			},
			{
				name:      "getting months with maxKeys=0",
				prefix:    makeSeparatedPath(sep, "photos", "2022") + sep,
				delimiter: sep,
				maxKeys:   0,

				prefixes:   nil,
				lenObjects: 0,
				nextMarker: "",
			},
			{
				name:      "getting months with maxKeys=3",
				prefix:    makeSeparatedPath(sep, "photos", "2022") + sep,
				delimiter: sep,
				maxKeys:   3,

				prefixes: []string{
					makeSeparatedPath(sep, "photos", "2022", "February") + sep,
					makeSeparatedPath(sep, "photos", "2022", "January") + sep,
				},
				lenObjects: 0,
				nextMarker: "",
			},
			{
				name:      "getting months with maxKeys=2",
				prefix:    makeSeparatedPath(sep, "photos", "2022") + sep,
				delimiter: sep,
				maxKeys:   2,

				prefixes: []string{
					makeSeparatedPath(sep, "photos", "2022", "February") + sep,
					makeSeparatedPath(sep, "photos", "2022", "January") + sep,
				},
				lenObjects: 0,
				nextMarker: "",
			},
			{
				name:      "getting months with maxKeys=1",
				prefix:    makeSeparatedPath(sep, "photos", "2022") + sep,
				delimiter: sep,
				maxKeys:   1,

				prefixes: []string{
					makeSeparatedPath(sep, "photos", "2022", "February") + sep,
				},
				lenObjects: 0,
				nextMarker: makeSeparatedPath(sep, "photos", "2022", "February") + sep,
			},
			{
				name:      "ensuring multi-byte delimiter",
				prefix:    makeSeparatedPath(sep, "photos", "2022") + sep,
				delimiter: "February" + sep,
				maxKeys:   maxKeysLimit,

				prefixes: []string{
					makeSeparatedPath(sep, "photos", "2022", "February") + sep,
				},
				lenObjects: 2,
				nextMarker: "",
			},
			{
				name:      "ensuring multi-byte delimiter with maxKeys=4",
				prefix:    makeSeparatedPath(sep, "photos", "2022") + sep,
				delimiter: "February" + sep,
				maxKeys:   4,

				prefixes: []string{
					makeSeparatedPath(sep, "photos", "2022", "February") + sep,
				},
				lenObjects: 2,
				nextMarker: "",
			},
			{
				name:      "ensuring multi-byte delimiter with maxKeys=3",
				prefix:    makeSeparatedPath(sep, "photos", "2022") + sep,
				delimiter: "February" + sep,
				maxKeys:   3,

				prefixes: []string{
					makeSeparatedPath(sep, "photos", "2022", "February") + sep,
				},
				lenObjects: 2,
				nextMarker: "",
			},
			{
				name:      "ensuring multi-byte delimiter with maxKeys=2",
				prefix:    makeSeparatedPath(sep, "photos", "2022") + sep,
				delimiter: "February" + sep,
				maxKeys:   2,

				prefixes: []string{
					makeSeparatedPath(sep, "photos", "2022", "February") + sep,
				},
				lenObjects: 1,
				nextMarker: makeSeparatedPath(sep, "photos", "2022", "January", "photo_2.jpg"),
			},
			{
				name:      "ensuring multi-byte delimiter with maxKeys=1",
				prefix:    makeSeparatedPath(sep, "photos", "2022") + sep,
				delimiter: "February" + sep,
				maxKeys:   1,

				prefixes: []string{
					makeSeparatedPath(sep, "photos", "2022", "February") + sep,
				},
				lenObjects: 0,
				nextMarker: makeSeparatedPath(sep, "photos", "2022", "February") + sep,
			},
			{
				name:      "pathological I",
				prefix:    "photos",
				delimiter: sep,
				maxKeys:   maxKeysLimit,

				prefixes: []string{
					"photos" + sep,
				},
				lenObjects: 0,
				nextMarker: "",
			},
			{
				name:      "pathological I with maxKeys=0",
				prefix:    "photos",
				delimiter: sep,
				maxKeys:   0,

				prefixes:   nil,
				lenObjects: 0,
				nextMarker: "",
			},
			{
				name:      "pathological I with maxKeys=2",
				prefix:    "photos",
				delimiter: sep,
				maxKeys:   2,

				prefixes: []string{
					"photos" + sep,
				},
				lenObjects: 0,
				nextMarker: "",
			},
			{
				name:      "pathological I with maxKeys=1",
				prefix:    "photos",
				delimiter: sep,
				maxKeys:   1,

				prefixes: []string{
					"photos" + sep,
				},
				lenObjects: 0,
				nextMarker: "",
			},
			{
				name:      "pathological II",
				prefix:    "photos" + sep + sep,
				delimiter: sep + sep,
				maxKeys:   maxKeysLimit,

				prefixes:   nil,
				lenObjects: 0,
				nextMarker: "",
			},
			{
				name:      "pathological III",
				prefix:    "photos" + sep + sep,
				delimiter: sep,
				maxKeys:   maxKeysLimit,

				prefixes:   nil,
				lenObjects: 0,
				nextMarker: "",
			},
			{
				name:      "pathological IV",
				prefix:    "photos" + sep,
				delimiter: sep + sep,
				maxKeys:   maxKeysLimit,

				prefixes:   nil,
				lenObjects: 4,
				nextMarker: "",
			},
			{
				name:      "pathological V",
				prefix:    "photos" + sep + sep,
				delimiter: "",
				maxKeys:   maxKeysLimit,

				prefixes:   nil,
				lenObjects: 0,
				nextMarker: "",
			},
			{
				name:      "pathological VI",
				prefix:    "photos",
				delimiter: sep + sep,
				maxKeys:   maxKeysLimit,

				prefixes:   nil,
				lenObjects: 4,
				nextMarker: "",
			},
			{
				name:      "unmatched prefix I",
				prefix:    makeSeparatedPath(sep, "photos", "2021") + sep,
				delimiter: sep,
				maxKeys:   1,

				prefixes:   nil,
				lenObjects: 0,
				nextMarker: "",
			},
			{
				name:      "unmatched prefix II",
				prefix:    "2022" + sep,
				delimiter: sep,
				maxKeys:   1,

				prefixes:   nil,
				lenObjects: 0,
				nextMarker: "",
			},
			{
				name:      "unmatched prefix III",
				prefix:    makeSeparatedPath(sep, "photos", "photos"),
				delimiter: sep,
				maxKeys:   1,

				prefixes:   nil,
				lenObjects: 0,
				nextMarker: "",
			},
			{
				name:      "unmatched prefix IV",
				prefix:    makeSeparatedPath(sep, "photos", "photos"),
				delimiter: "",
				maxKeys:   1,

				prefixes:   nil,
				lenObjects: 0,
				nextMarker: "",
			},
			{
				name:      "no delimiter I",
				prefix:    makeSeparatedPath(sep, "photos", "2022") + sep,
				delimiter: "",
				maxKeys:   maxKeysLimit,

				prefixes:   nil,
				lenObjects: 3,
				nextMarker: "",
			},
			{
				name:      "no delimiter II",
				prefix:    makeSeparatedPath(sep, "photos", "2022"),
				delimiter: "",
				maxKeys:   1,

				prefixes:   nil,
				lenObjects: 1,
				nextMarker: makeSeparatedPath(sep, "photos", "2022", "February", "photo_4.jpg"),
			},
			{
				name:      "delimiter is the extension",
				prefix:    "photos" + sep,
				delimiter: ".jpg",
				maxKeys:   maxKeysLimit,

				prefixes: []string{
					makeSeparatedPath(sep, "photos", "2022", "February", "photo_4.jpg"),
					makeSeparatedPath(sep, "photos", "2022", "January", "photo_2.jpg"),
					makeSeparatedPath(sep, "photos", "2022", "January", "photo_3.jpg"),
					makeSeparatedPath(sep, "photos", "photo_1.jpg"),
				},
				lenObjects: 0,
				nextMarker: "",
			},
		} {
			pres, objs, marker, nextMarker, truncated, err := listObjects(ctx, layer, testBucketInfo.Name, tt.prefix, "", tt.delimiter, tt.maxKeys)
			require.NoError(t, err, tt.name)

			assert.Equal(t, tt.prefixes, pres, tt.name)
			assert.Len(t, objs, tt.lenObjects, tt.name)
			assert.Empty(t, marker, tt.name)
			assert.Equal(t, tt.nextMarker, nextMarker, tt.name)
			assert.Equal(t, tt.nextMarker != "", truncated, tt.name)
		}

		{ // Check whether continuation works with arbitrary prefix & delimiter.
			marker := makeSeparatedPath(sep, "photos", "2022", "February", "photo_4.jpg")

			pres, objs, err := listBucketObjects(ctx, listObjects, layer, "photos"+sep, ".jpg", 1, marker)

			msg := "continuation tokens and arbitrary prefix/delimiter"

			require.NoError(t, err, msg)

			expectedPrefixes := map[string]struct{}{
				makeSeparatedPath(sep, "photos", "2022", "January", "photo_2.jpg"): {},
				makeSeparatedPath(sep, "photos", "2022", "January", "photo_3.jpg"): {},
				makeSeparatedPath(sep, "photos", "photo_1.jpg"):                    {},
			}

			assert.Equal(t, expectedPrefixes, pres, msg)
			assert.Empty(t, objs, msg)
		}

		{ // Check whether skipping collapsed keys works.
			marker := makeSeparatedPath(sep, "photos", "2022") + sep

			pres, objs, err := listBucketObjects(ctx, listObjects, layer, "photos"+sep, sep, maxKeysLimit, marker)

			msg := "skipping collapsed keys"

			require.NoError(t, err, msg)

			assert.Empty(t, pres, msg)
			assert.Len(t, objs, 1, msg)
		}
	})
}

func makeSeparatedPath(sep string, elem ...string) string {
	var b strings.Builder

	for i, e := range elem {
		if i > 0 {
			b.WriteString(sep)
		}
		b.WriteString(e)
	}

	return b.String()
}

func listBucketObjects(ctx context.Context, listObjects listObjectsFunc, layer minio.ObjectLayer, prefix, delimiter string, maxKeys int, startAfter string) (map[string]struct{}, map[string]struct{}, error) {
	gotPrefixes, gotObjects := make(map[string]struct{}), make(map[string]struct{})

	for marker, more := "", true; more; {
		if marker == "" {
			marker = startAfter
		}

		prefixes, objects, _, nextContinuationToken, isTruncated, err := listObjects(ctx, layer, testBucket, prefix, marker, delimiter, maxKeys)
		if err != nil {
			return nil, nil, err
		}

		if maxKeys > 0 && len(prefixes)+len(objects) > maxKeys {
			return nil, nil, errors.New("prefixes + objects exceed maxKeys")
		}

		switch isTruncated {
		case true:
			if nextContinuationToken == "" {
				return nil, nil, errors.New("isTruncated is true but nextContinuationToken is empty")
			}
		case false:
			if nextContinuationToken != "" {
				return nil, nil, errors.New("isTruncated is false but nextContinuationToken is not empty")
			}
		}

		for _, p := range prefixes {
			gotPrefixes[p] = struct{}{}
		}

		for _, o := range objects {
			gotObjects[o.Name] = struct{}{}
		}

		marker, more = nextContinuationToken, isTruncated
	}

	return gotPrefixes, gotObjects, nil
}

func testListObjectsLimits(t *testing.T, listObjects listObjectsFunc) {
	runTest(t, func(t *testing.T, ctx context.Context, _ minio.ObjectLayer, project *uplink.Project) {
		testBucketInfo, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		for _, key := range []string{
			"p",
			"p/a/d",
			"p/b/d",
			"p/c/d",
		} {
			_, err := createFile(ctx, project, testBucketInfo.Name, key, nil, nil)
			require.NoError(t, err)
		}

		// Reconfigure the layer so we can hit MaxKeysLimit and
		// MaxKeysExhaustiveLimit limits.
		c := miniogw.S3CompatibilityConfig{
			IncludeCustomMetadataListing: true,
			MaxKeysLimit:                 2,
			MaxKeysExhaustiveLimit:       2,
		}

		l, err := miniogw.NewStorjGateway(c).NewGatewayLayer(auth.Credentials{})
		require.NoError(t, err)

		for _, tt := range []struct {
			prefix    string
			delimiter string

			lenPres int
			lenObjs int
		}{
			{
				prefix:    "",
				delimiter: "",
				lenPres:   0,
				lenObjs:   1,
			},
			{
				prefix:    "p/",
				delimiter: "/",
				lenPres:   1,
				lenObjs:   0,
			},
			{
				prefix:    "p",
				delimiter: "/",
				lenPres:   0,
				lenObjs:   1,
			},
		} {
			pres, objs, marker, nextMarker, truncated, err := listObjects(ctx, l, testBucketInfo.Name, tt.prefix, "", tt.delimiter, maxKeysLimit)
			require.NoError(t, err)

			assert.Len(t, pres, tt.lenPres)
			assert.Len(t, objs, tt.lenObjs)
			assert.Empty(t, marker)
			assert.NotEmpty(t, nextMarker)
			assert.True(t, truncated)
		}

		_, _, _, _, _, err = listObjects(ctx, l, testBucketInfo.Name, "p/a", "p/b/d", "/d", 1)
		require.ErrorIs(t, err, miniogw.ErrTooManyItemsToList, "MaxKeysExhaustiveLimit")

		require.NoError(t, l.Shutdown(ctx))
	})
}

// TestListObjectsFullyCompatible tests whether exhaustive listing's results
// are, e.g., lexicographically ordered. It also tests cases when incorrectly
// implemented obvious exhaustive listing optimizations (in the context of
// libuplink API) might return wrong results. It cannot test whether such
// optimizations exist (there are other ways to check this).
func TestListObjectsFullyCompatible(t *testing.T) {
	t.Parallel()

	bucketName := testrand.BucketName()

	const (
		prefix  = "moHaq/" // "moHaq" is "prefix" in Klingon.
		prefix2 = "ab/cd/"
	)

	var paths, prefixed []string

	for i := 0; i < 100; i++ {
		p := testrand.Path()

		if i%2 == 0 {
			p = prefix + prefix2 + p

			if len(p) > 1024 {
				p = p[:1024]
			}

			prefixed = append(prefixed, p)
		}

		paths = append(paths, p)
	}

	sort.Strings(paths)
	sort.Strings(prefixed)

	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
		Reconfigure: testplanet.Reconfigure{
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.DefaultPathCipher = storj.EncNull
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		// Establish new context with *uplink.Project for the gateway to pick up.
		ctxWithProject := miniogw.WithUplinkProject(ctx, project)

		s3Compatibility := miniogw.S3CompatibilityConfig{
			IncludeCustomMetadataListing: true,
			MaxKeysLimit:                 51, // +1 to list 50 (see storj.io/gateway/miniogw limitResults())
			MaxKeysExhaustiveLimit:       100,
			FullyCompatibleListing:       true, // this is what's important here
		}

		layer, err := miniogw.NewStorjGateway(s3Compatibility).NewGatewayLayer(auth.Credentials{})
		require.NoError(t, err)

		defer func() { require.NoError(t, layer.Shutdown(ctxWithProject)) }()

		_, err = project.EnsureBucket(ctxWithProject, bucketName)
		require.NoError(t, err)

		for _, p := range paths {
			_, err = createFile(ctxWithProject, project, bucketName, p, []byte{'t', 'e', 's', 't'}, nil)
			require.NoError(t, err)
		}

		{
			const name = "mustn't trigger optimizations: all"

			var objects []minio.ObjectInfo

			for isTruncated, nextMarker := true, ""; isTruncated; {
				result, err := layer.ListObjects(ctxWithProject, bucketName, "", nextMarker, "", 100)
				require.NoError(t, err, name)

				isTruncated, nextMarker = result.IsTruncated, result.NextMarker

				assert.Empty(t, result.Prefixes, name)

				objects = append(objects, result.Objects...)
			}

			require.Len(t, objects, len(paths), name)

			for i, p := range paths {
				assert.Equal(t, p, objects[i].Name, name, i)
			}
		}
		{
			const name = "mustn't trigger optimizations: all (limit check)"

			result, err := layer.ListObjects(ctxWithProject, bucketName, "", "", "", 100)
			require.NoError(t, err, name)

			assert.True(t, result.IsTruncated, name)
			assert.Equal(t, result.NextMarker, paths[49], name)
			assert.Empty(t, result.Prefixes, name)

			require.Len(t, result.Objects, len(paths[:50]), name)

			for i, p := range paths[:50] {
				assert.Equal(t, p, result.Objects[i].Name, name, i)
			}
		}
		{
			const name = "mustn't trigger optimizations: prefix without trailing forward slash"

			var objects []minio.ObjectInfo

			for isTruncated, nextMarker := true, ""; isTruncated; {
				result, err := layer.ListObjects(ctxWithProject, bucketName, strings.TrimSuffix(prefix, "/"), nextMarker, "", 10)
				require.NoError(t, err, name)

				isTruncated, nextMarker = result.IsTruncated, result.NextMarker

				assert.Empty(t, result.Prefixes, name)

				objects = append(objects, result.Objects...)
			}

			require.Len(t, objects, len(prefixed), name)

			for i, p := range prefixed {
				assert.Equal(t, p, objects[i].Name, name, i)
			}
		}
		{
			const name = "mustn't trigger optimizations: prefix without trailing forward slash contains forward slashes + forward slash delimiter"

			// Set marker to something before prefix, so we don't trigger our
			// optimization for non-forward-slash-terminated prefixes. If we
			// triggered it, we are still fully compatible, but we specifically
			// want to test the fallback to the exhaustive listing.
			result, err := layer.ListObjects(ctxWithProject, bucketName, strings.TrimSuffix(prefix+prefix2, "/"), string(prefix[0]), "/", 100)
			require.NoError(t, err, name)

			assert.False(t, result.IsTruncated, name)
			assert.Empty(t, result.NextMarker, name)
			assert.Empty(t, result.Objects, name)

			require.Len(t, result.Prefixes, 1, name)

			assert.Equal(t, prefix+prefix2, result.Prefixes[0], name)
		}
		{
			const name = "mustn't trigger optimizations: arbitrary delimiter"

			var (
				objects  []minio.ObjectInfo
				prefixes []string
			)

			for isTruncated, nextMarker := true, ""; isTruncated; {
				result, err := layer.ListObjects(ctxWithProject, bucketName, "", nextMarker, prefix2, 100)
				require.NoError(t, err, name)

				isTruncated, nextMarker = result.IsTruncated, result.NextMarker

				objects = append(objects, result.Objects...)
				prefixes = append(prefixes, result.Prefixes...)
			}

			require.Len(t, prefixes, 1, name)

			assert.Equal(t, prefix+prefix2, prefixes[0], name)

			var withoutPrefix []string

			for _, p := range paths {
				if !strings.HasPrefix(p, prefix+prefix2) {
					withoutPrefix = append(withoutPrefix, p)
				}
			}

			require.Len(t, objects, len(withoutPrefix), name)

			for i, p := range withoutPrefix {
				assert.Equal(t, p, objects[i].Name, name, i)
			}
		}
		{
			const name = "mustn't trigger optimizations: prefix without trailing forward slash + arbitrary delimiter"

			result, err := layer.ListObjects(ctxWithProject, bucketName, strings.TrimSuffix(prefix, "/"), "", prefix2, 100)
			require.NoError(t, err, name)

			assert.False(t, result.IsTruncated, name)
			assert.Empty(t, result.NextMarker, name)
			assert.Empty(t, result.Objects, name)

			require.Len(t, result.Prefixes, 1, name)

			assert.Equal(t, prefix+prefix2, result.Prefixes[0], name)
		}
		{
			const name = "partially optimized: prefix"

			result, err := layer.ListObjects(ctxWithProject, bucketName, prefix, "", "", 100)
			require.NoError(t, err, name)

			assert.False(t, result.IsTruncated, name)
			assert.Empty(t, result.NextMarker, name)
			assert.Empty(t, result.Prefixes, name)

			require.Len(t, result.Objects, len(prefixed), name)

			for i, p := range prefixed {
				assert.Equal(t, p, result.Objects[i].Name, name, i)
			}
		}
		{
			const name = "partially optimized: prefix without trailing forward slash + forward slash delimiter"

			// Set marker to something before prefix, so we don't trigger our
			// optimization for non-forward-slash-terminated prefixes. If we
			// triggered it, we are still fully compatible, but we specifically
			// want to test the fallback to the exhaustive listing.
			result, err := layer.ListObjects(ctxWithProject, bucketName, strings.TrimSuffix(prefix, "/"), string(prefix[0]), "/", 100)
			require.NoError(t, err, name)

			assert.False(t, result.IsTruncated, name)
			assert.Empty(t, result.NextMarker, name)
			assert.Empty(t, result.Objects, name)

			require.Len(t, result.Prefixes, 1, name)

			assert.Equal(t, prefix, result.Prefixes[0], name)
		}
		{
			const name = "partially optimized: prefix + arbitrary delimiter"

			result, err := layer.ListObjects(ctxWithProject, bucketName, prefix, "", strings.SplitAfterN(prefix2, "/", 2)[1], 100)
			require.NoError(t, err, name)

			assert.False(t, result.IsTruncated, name)
			assert.Empty(t, result.NextMarker, name)
			assert.Empty(t, result.Objects, name)

			require.Len(t, result.Prefixes, 1, name)

			assert.Equal(t, prefix+prefix2, result.Prefixes[0], name)
		}
		{
			const name = "optimized: prefix + forward slash delimiter"

			result, err := layer.ListObjects(ctxWithProject, bucketName, prefix, "", "/", 100)
			require.NoError(t, err, name)

			assert.False(t, result.IsTruncated, name)
			assert.Empty(t, result.NextMarker, name)
			assert.Empty(t, result.Objects, name)

			require.Len(t, result.Prefixes, 1, name)

			assert.Equal(t, prefix+strings.SplitAfterN(prefix2, "/", 2)[0], result.Prefixes[0], name)
		}
	})
}

func TestListMultipartUploads(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Check the error when listing an object from a bucket with empty name
		uploads, err := layer.ListMultipartUploads(ctx, "", "", "", "", "", 1)
		assert.Equal(t, minio.BucketNameInvalid{}, err)
		assert.Empty(t, uploads)

		// Check the error when listing objects from non-existing bucket
		uploads, err = layer.ListMultipartUploads(ctx, testBucket, "", "", "", "", 1)
		assert.Equal(t, minio.BucketNotFound{Bucket: testBucket}, err)
		assert.Empty(t, uploads)

		// Create the bucket using the Uplink API
		_, err = project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		keyPaths := []string{
			"a", "aa", "b", "bb", "c",
			"a/xa", "a/xaa", "a/xb", "a/xbb", "a/xc",
			"b/ya", "b/yaa", "b/yb", "b/ybb", "b/yc",
			"i", "i/i", "ii", "j", "j/i", "k", "kk", "l",
			"m/i", "mm", "n/i", "oo",
		}

		keys := make(map[string]map[string]string, len(keyPaths))

		metadata := map[string]string{
			"content-type": "text/plain",
			"key1":         "value1",
			"key2":         "value2",
		}
		for _, key := range keyPaths {
			upload, err := layer.NewMultipartUpload(ctx, testBucket, key, minio.ObjectOptions{
				UserDefined: metadata,
			})
			require.NoError(t, err)
			require.NotEmpty(t, upload)
			keys[key] = metadata
		}

		for i, tt := range []struct {
			name       string
			prefix     string
			marker     string
			delimiter  string
			maxUploads int
			more       bool
			prefixes   []string
			uploads    []string
		}{
			{
				name:       "Basic non-recursive",
				delimiter:  "/",
				maxUploads: maxUploadsLimit,
				prefixes:   []string{"a/", "b/", "i/", "j/", "m/", "n/"},
				uploads:    []string{"a", "aa", "b", "bb", "c", "i", "ii", "j", "k", "kk", "l", "mm", "oo"},
			}, {
				name:       "Basic non-recursive with non-existing mark",
				marker:     "`",
				delimiter:  "/",
				maxUploads: maxUploadsLimit,
				prefixes:   []string{"a/", "b/", "i/", "j/", "m/", "n/"},
				uploads:    []string{"a", "aa", "b", "bb", "c", "i", "ii", "j", "k", "kk", "l", "mm", "oo"},
			}, {
				name:       "Basic non-recursive with existing mark",
				marker:     "b",
				delimiter:  "/",
				maxUploads: maxUploadsLimit,
				prefixes:   []string{"b/", "i/", "j/", "m/", "n/"},
				uploads:    []string{"bb", "c", "i", "ii", "j", "k", "kk", "l", "mm", "oo"},
			}, {
				name:       "Basic non-recursive with last mark",
				marker:     "oo",
				delimiter:  "/",
				maxUploads: maxUploadsLimit,
			}, {
				name:       "Basic non-recursive with past last mark",
				marker:     "ooa",
				delimiter:  "/",
				maxUploads: maxUploadsLimit,
			}, {
				name:       "Basic non-recursive with max uploads limit of 0",
				marker:     "",
				delimiter:  "/",
				maxUploads: 0,
				more:       false,
				prefixes:   nil,
				uploads:    nil,
			}, {
				name:       "Basic non-recursive with max uploads limit of 0 and marker",
				marker:     "b",
				delimiter:  "/",
				maxUploads: 0,
				more:       false,
				prefixes:   nil,
				uploads:    nil,
			}, {
				name:       "Basic non-recursive with max uploads limit of 1",
				delimiter:  "/",
				maxUploads: 1,
				more:       true,
				uploads:    []string{"a"},
			}, {
				name:       "Basic non-recursive with max uploads limit of 1 with non-existing mark",
				marker:     "`",
				delimiter:  "/",
				maxUploads: 1,
				more:       true,
				uploads:    []string{"a"},
			}, {
				name:       "Basic non-recursive with max uploads limit of 1 with existing mark",
				marker:     "aa",
				delimiter:  "/",
				maxUploads: 1,
				more:       true,
				uploads:    []string{"b"},
			}, {
				name:       "Basic non-recursive with max uploads limit of 1 with last mark",
				marker:     "oo",
				delimiter:  "/",
				maxUploads: 1,
			}, {
				name:       "Basic non-recursive with max uploads limit of 1 past last mark",
				marker:     "ooa",
				delimiter:  "/",
				maxUploads: 1,
			}, {
				name:       "Basic non-recursive with max uploads limit of 2",
				delimiter:  "/",
				maxUploads: 2,
				more:       true,
				prefixes:   []string{"a/"},
				uploads:    []string{"a"},
			}, {
				name:       "Basic non-recursive with max uploads limit of 2 with non-existing mark",
				marker:     "`",
				delimiter:  "/",
				maxUploads: 2,
				more:       true,
				prefixes:   []string{"a/"},
				uploads:    []string{"a"},
			}, {
				name:       "Basic non-recursive with max uploads limit of 2 with existing mark",
				marker:     "aa",
				delimiter:  "/",
				maxUploads: 2,
				more:       true,
				prefixes:   []string{"b/"},
				uploads:    []string{"b"},
			}, {
				name:       "Basic non-recursive with max uploads limit of 2 with mark right before the end",
				marker:     "nm",
				delimiter:  "/",
				maxUploads: 2,
				uploads:    []string{"oo"},
			}, {
				name:       "Basic non-recursive with max uploads limit of 2 with last mark",
				marker:     "oo",
				delimiter:  "/",
				maxUploads: 2,
			}, {
				name:       "Basic non-recursive with max uploads limit of 2 past last mark",
				marker:     "ooa",
				delimiter:  "/",
				maxUploads: 2,
			}, {
				name:       "Prefix non-recursive",
				prefix:     "a/",
				delimiter:  "/",
				maxUploads: maxUploadsLimit,
				uploads:    []string{"xa", "xaa", "xb", "xbb", "xc"},
			}, {
				name:       "Prefix non-recursive with mark",
				prefix:     "a/",
				marker:     "xb",
				delimiter:  "/",
				maxUploads: maxUploadsLimit,
				uploads:    []string{"xbb", "xc"},
			}, {
				name:       "Prefix non-recursive with mark and max keys",
				prefix:     "a/",
				marker:     "xaa",
				delimiter:  "/",
				maxUploads: 2,
				more:       true,
				uploads:    []string{"xb", "xbb"},
			},
		} {
			errTag := fmt.Sprintf("%d. %+v", i, tt)

			result, err := layer.ListMultipartUploads(ctx, testBucket, tt.prefix, tt.marker, "", tt.delimiter, tt.maxUploads)
			require.NoError(t, err, errTag)
			assert.Equal(t, tt.more, result.IsTruncated, errTag)
			assert.Equal(t, tt.marker, result.KeyMarker, errTag)
			assert.Equal(t, tt.prefixes, result.CommonPrefixes, errTag)
			require.Equal(t, len(tt.uploads), len(result.Uploads), errTag)
			for i, uploadInfo := range result.Uploads {
				metadata, found := keys[uploadInfo.Object]
				if assert.True(t, found) {
					if tt.prefix != "" && strings.HasSuffix(tt.prefix, "/") {
						assert.Equal(t, tt.prefix+tt.uploads[i], uploadInfo.Object, errTag)
					} else {
						assert.Equal(t, tt.uploads[i], uploadInfo.Object, errTag)
					}
					assert.Equal(t, testBucket, uploadInfo.Bucket, errTag)
					assert.Equal(t, metadata, uploadInfo.UserDefined, errTag)
				}
			}
		}
	})
}

func TestNewMultipartUpload(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		bucket, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)
		require.Equal(t, bucket.Name, testBucket)

		listParts, err := layer.ListMultipartUploads(ctx, testBucket, "", "", "", "", 1)
		require.NoError(t, err)
		require.Empty(t, listParts.Uploads)

		_, err = layer.NewMultipartUpload(ctx, testBucket, testFile, minio.ObjectOptions{})
		require.NoError(t, err)
		_, err = layer.NewMultipartUpload(ctx, testBucket, testFile2, minio.ObjectOptions{})
		require.NoError(t, err)

		listParts, err = layer.ListMultipartUploads(ctx, testBucket, "", "", "", "", 2)
		require.NoError(t, err)
		require.Len(t, listParts.Uploads, 2)
	})
}

func TestCopyObjectPart(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		_, err := layer.CopyObjectPart(ctx, "srcBucket", "srcObject", "destBucket", "destObject", "uploadID", 0, 0, 10, minio.ObjectInfo{}, minio.ObjectOptions{}, minio.ObjectOptions{})
		require.EqualError(t, err, minio.NotImplemented{}.Error())
	})
}

func TestStorageClassSupport(t *testing.T) {
	t.Parallel()

	const (
		apiPutObjectStorageClass          = "PutObject (storage class)"
		apiNewMultipartUploadStorageClass = "NewMultipartUpload (storage class)"
		apiCopyObjectStorageClass         = "CopyObject (storage class)"
	)

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		for _, class := range []string{
			storageclass.RRS,
			storageclass.STANDARD,
			storageclass.DMA,
		} {
			userDefined := map[string]string{xhttp.AmzStorageClass: class}
			opts := minio.ObjectOptions{UserDefined: userDefined}
			info := minio.ObjectInfo{UserDefined: userDefined}

			_, errPutObject := layer.PutObject(ctx, "test-a", "o", nil, opts)
			_, errNewMultipartUpload := layer.NewMultipartUpload(ctx, "test-a", "o", opts)
			_, errCopyObject := layer.CopyObject(ctx, "test-a", "so", "test-b", "do", info, minio.ObjectOptions{}, minio.ObjectOptions{})

			if class != storageclass.STANDARD {
				assert.ErrorIs(t, errPutObject, minio.NotImplemented{Message: apiPutObjectStorageClass})
				assert.ErrorIs(t, errNewMultipartUpload, minio.NotImplemented{Message: apiNewMultipartUploadStorageClass})
				assert.ErrorIs(t, errCopyObject, minio.NotImplemented{Message: apiCopyObjectStorageClass})
			} else {
				assert.NotErrorIs(t, errPutObject, minio.NotImplemented{Message: apiPutObjectStorageClass})
				assert.NotErrorIs(t, errNewMultipartUpload, minio.NotImplemented{Message: apiNewMultipartUploadStorageClass})
				assert.NotErrorIs(t, errCopyObject, minio.NotImplemented{Message: apiCopyObjectStorageClass})
			}
		}
	})
}

func TestPutObjectPart(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		bucket, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)
		require.Equal(t, bucket.Name, testBucket)

		listInfo, err := layer.ListMultipartUploads(ctx, testBucket, "", "", "", "", 1)
		require.NoError(t, err)
		require.Empty(t, listInfo.Uploads)

		uploadID, err := layer.NewMultipartUpload(ctx, testBucket, testFile, minio.ObjectOptions{})
		require.NoError(t, err)

		totalPartsCount := 3
		for i := 1; i <= totalPartsCount; i++ {
			info, err := layer.PutObjectPart(ctx, testBucket, testFile, uploadID, i, newMinioPutObjReader(t), minio.ObjectOptions{})
			require.NoError(t, err)
			require.Equal(t, i, info.PartNumber)
		}

		listParts, err := layer.ListObjectParts(ctx, testBucket, testFile, uploadID, 0, totalPartsCount, minio.ObjectOptions{})
		require.NoError(t, err)
		require.Len(t, listParts.Parts, totalPartsCount)
		require.Equal(t, testBucket, listParts.Bucket)
		require.Equal(t, testFile, listParts.Object)
		require.Equal(t, uploadID, listParts.UploadID)

		require.Equal(t, listParts.Parts[0].PartNumber, 1)
		require.Equal(t, listParts.Parts[1].PartNumber, 2)
		require.Equal(t, listParts.Parts[2].PartNumber, 3)
	})
}

func TestPutObjectPartZeroBytesOnlyPart(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		bucket, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		uploadID, err := layer.NewMultipartUpload(ctx, bucket.Name, testFile, minio.ObjectOptions{})
		require.NoError(t, err)

		defer func() {
			if err = layer.AbortMultipartUpload(ctx, bucket.Name, testFile, uploadID, minio.ObjectOptions{}); err != nil {
				assert.ErrorIs(t, err, minio.InvalidUploadID{Bucket: bucket.Name, Object: testFile, UploadID: uploadID})
			}
		}()

		h, err := hash.NewReader(
			bytes.NewReader(make([]byte, 0)),
			0,
			"d41d8cd98f00b204e9800998ecf8427e",
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			0)
		require.NoError(t, err)

		r := minio.NewPutObjReader(h)

		opts := minio.ObjectOptions{
			UserDefined: make(map[string]string),
		}

		part, err := layer.PutObjectPart(ctx, bucket.Name, testFile, uploadID, 1, r, opts)
		require.NoError(t, err)

		assert.Zero(t, part.Size)
		assert.Zero(t, part.ActualSize)

		parts := []minio.CompletePart{
			{
				PartNumber: part.PartNumber,
				ETag:       part.ETag,
			},
		}

		obj, err := layer.CompleteMultipartUpload(ctx, bucket.Name, testFile, uploadID, parts, opts)
		require.NoError(t, err)

		assert.Zero(t, obj.Size)

		downloaded, err := project.DownloadObject(ctx, obj.Bucket, obj.Name, nil)
		require.NoError(t, err)

		_, err = downloaded.Read(make([]byte, 1))
		assert.ErrorIs(t, err, io.EOF)

		assert.Zero(t, downloaded.Info().System.ContentLength)

		require.NoError(t, downloaded.Close())
	})
}

func TestPutObjectPartZeroBytesLastPart(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		bucket, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		uploadID, err := layer.NewMultipartUpload(ctx, bucket.Name, testFile, minio.ObjectOptions{})
		require.NoError(t, err)

		defer func() {
			if err = layer.AbortMultipartUpload(ctx, bucket.Name, testFile, uploadID, minio.ObjectOptions{}); err != nil {
				assert.ErrorIs(t, err, minio.InvalidUploadID{Bucket: bucket.Name, Object: testFile, UploadID: uploadID})
			}
		}()

		const (
			nonZeroContent          = "test"
			nonZeroContentLen       = int64(4)
			nonZeroContentMD5Hex    = "098f6bcd4621d373cade4e832627b4f6"
			nonZeroContentSHA256Hex = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
		)

		var parts []minio.CompletePart

		// Upload two non-zero parts:

		for i := 0; i < 2; i++ {
			h, err := hash.NewReader(
				bytes.NewReader([]byte(nonZeroContent)),
				nonZeroContentLen,
				nonZeroContentMD5Hex,
				nonZeroContentSHA256Hex,
				nonZeroContentLen)
			require.NoError(t, err)

			r := minio.NewPutObjReader(h)

			opts := minio.ObjectOptions{
				UserDefined: make(map[string]string),
			}

			part, err := layer.PutObjectPart(ctx, bucket.Name, testFile, uploadID, i+1, r, opts)
			require.NoError(t, err)

			assert.Equal(t, nonZeroContentLen, part.Size)
			assert.Equal(t, nonZeroContentLen, part.ActualSize)

			parts = append(parts, minio.CompletePart{PartNumber: part.PartNumber, ETag: part.ETag})
		}

		// Upload one (last) zero-byte part:

		h, err := hash.NewReader(
			bytes.NewReader(make([]byte, 0)),
			0,
			"d41d8cd98f00b204e9800998ecf8427e",
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			0)
		require.NoError(t, err)

		r := minio.NewPutObjReader(h)

		opts := minio.ObjectOptions{
			UserDefined: make(map[string]string),
		}

		part, err := layer.PutObjectPart(ctx, bucket.Name, testFile, uploadID, 3, r, opts)
		require.NoError(t, err)

		assert.Zero(t, part.Size)
		assert.Zero(t, part.ActualSize)

		parts = append(parts, minio.CompletePart{PartNumber: part.PartNumber, ETag: part.ETag})

		obj, err := layer.CompleteMultipartUpload(ctx, bucket.Name, testFile, uploadID, parts, opts)
		require.NoError(t, err)

		// The uplink library contains unresolved TODO for returning real
		// objects after committing.
		//
		// TODO(amwolff): enable this check after mentioned TODO is completed.
		//
		// assert.Equal(t, 2*nonZeroContentLen, obj.Size)

		// Verify state:

		downloaded, err := project.DownloadObject(ctx, obj.Bucket, obj.Name, nil)
		require.NoError(t, err)

		defer func() { require.NoError(t, downloaded.Close()) }()

		buf := new(bytes.Buffer)

		_, err = io.Copy(buf, downloaded)
		require.NoError(t, err)

		assert.Equal(t, nonZeroContent+nonZeroContent, buf.String())

		assert.Equal(t, 2*nonZeroContentLen, downloaded.Info().System.ContentLength)
	})
}

// TestPutObjectPartSegmentSize ensures that completing multipart upload after
// uploading parts of segment size will not return an error. This has happened
// before because of bad encryption/decryption of ETag in libuplink, so this
// test mainly assures it doesn't happen again.
//
// Related fix: https://review.dev.storj.io/c/storj/uplink/+/5710
func TestPutObjectPartSegmentSize(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		bucket, err := project.CreateBucket(ctx, testrand.BucketName())
		require.NoError(t, err)

		object := testrand.Path()

		uploadID, err := layer.NewMultipartUpload(ctx, bucket.Name, object, minio.ObjectOptions{})
		require.NoError(t, err)

		defer func() {
			if err = layer.AbortMultipartUpload(ctx, bucket.Name, object, uploadID, minio.ObjectOptions{}); err != nil {
				assert.ErrorIs(t, err, minio.InvalidUploadID{Bucket: bucket.Name, Object: object, UploadID: uploadID})
			}
		}()

		var parts []minio.CompletePart

		for i, s := range []memory.Size{segmentSize, memory.KiB} { // 641 KiB file
			data := testrand.Bytes(s)

			h, err := hash.NewReader(bytes.NewReader(data), s.Int64(), md5Hex(data), sha256Hex(data), s.Int64())
			require.NoError(t, err)

			r := minio.NewPutObjReader(h)

			opts := minio.ObjectOptions{
				UserDefined: make(map[string]string),
			}

			part, err := layer.PutObjectPart(ctx, bucket.Name, object, uploadID, i+1, r, opts)
			require.NoError(t, err)

			parts = append(parts, minio.CompletePart{PartNumber: part.PartNumber, ETag: part.ETag})
		}

		opts := minio.ObjectOptions{
			UserDefined: make(map[string]string),
		}

		obj, err := layer.CompleteMultipartUpload(ctx, bucket.Name, object, uploadID, parts, opts)
		require.NoError(t, err)

		downloaded, err := project.DownloadObject(ctx, obj.Bucket, obj.Name, nil)
		require.NoError(t, err)

		defer func() { require.NoError(t, downloaded.Close()) }()

		buf := new(bytes.Buffer)

		_, err = io.Copy(buf, downloaded)
		require.NoError(t, err)

		expectedSize := segmentSize + memory.KiB

		assert.Equal(t, expectedSize, memory.Size(len(buf.Bytes())))
		assert.Equal(t, expectedSize, memory.Size(downloaded.Info().System.ContentLength))
	})
}

func TestGetMultipartInfo(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Check the error when using an empty bucket name
		multipartInfo, err := layer.GetMultipartInfo(ctx, "", "object", "uploadid", minio.ObjectOptions{})
		require.Error(t, err)
		assert.Equal(t, minio.BucketNameInvalid{}, err)
		assert.Empty(t, multipartInfo)

		multipartInfo, err = layer.GetMultipartInfo(ctx, testBucket, "", "uploadid", minio.ObjectOptions{})
		require.Error(t, err)
		assert.Equal(t, minio.ObjectNameInvalid{}, err)
		assert.Empty(t, multipartInfo)

		multipartInfo, err = layer.GetMultipartInfo(ctx, testBucket, "object", "", minio.ObjectOptions{})
		require.Error(t, err)
		assert.Equal(t, minio.InvalidUploadID{}, err)
		assert.Empty(t, multipartInfo)

		// Check the error when getting MultipartInfo from non-existing bucket
		multipartInfo, err = layer.GetMultipartInfo(ctx, testBucket, "object", "uploadid", minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNotFound{Bucket: testBucket}, err)
		assert.Empty(t, multipartInfo)

		// Create the bucket using the Uplink API
		_, err = project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		now := time.Now()
		// TODO when we can have two multipart uploads for the same object key, make tests for this case
		upload, err := layer.NewMultipartUpload(ctx, testBucket, "multipart-upload", minio.ObjectOptions{})
		require.NoError(t, err)
		require.NotEmpty(t, upload)

		// Check the error when getting MultipartInfo from non-existing object
		multipartInfo, err = layer.GetMultipartInfo(ctx, testBucket, "object", upload, minio.ObjectOptions{})
		assert.Equal(t, minio.ObjectNotFound{Bucket: testBucket, Object: "object"}, err)
		assert.Empty(t, multipartInfo)

		multipartInfo, err = layer.GetMultipartInfo(ctx, testBucket, "multipart-upload", upload, minio.ObjectOptions{})
		require.NoError(t, err)

		require.Equal(t, testBucket, multipartInfo.Bucket)
		require.Equal(t, "multipart-upload", multipartInfo.Object)
		require.Equal(t, upload, multipartInfo.UploadID)
		require.WithinDuration(t, now, multipartInfo.Initiated, time.Minute)
	})
}

func TestListObjectParts(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Check the error when listing parts from a bucket with empty name
		parts, err := layer.ListObjectParts(ctx, "", "", "", 0, 1, minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNameInvalid{}, err)
		assert.Empty(t, parts)

		// Check the error when listing parts of an object with empty key
		parts, err = layer.ListObjectParts(ctx, testBucket, "", "", 0, 1, minio.ObjectOptions{})
		assert.Equal(t, minio.ObjectNameInvalid{Bucket: testBucket}, err)
		assert.Empty(t, parts)

		// Check the error when listing parts of a multipart upload is empty upload ID
		parts, err = layer.ListObjectParts(ctx, testBucket, testFile, "", 0, 1, minio.ObjectOptions{})
		assert.Equal(t, minio.InvalidUploadID{Bucket: testBucket, Object: testFile}, err)
		assert.Empty(t, parts)

		// TODO: This fails because InvalidUploadID is returned instead of BucketNotFound. Check if this is a bug.
		// Check the error when listing parts from non-existing bucket
		// parts, err = layer.ListObjectParts(ctx, TestBucket, TestFile, "uploadid", 0, 1, minio.ObjectOptions{})
		// assert.Equal(t, minio.BucketNotFound{Bucket: TestBucket}, err)
		// assert.Empty(t, parts)

		bucket, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)
		require.Equal(t, bucket.Name, testBucket)

		listInfo, err := layer.ListMultipartUploads(ctx, testBucket, "", "", "", "", 1)
		require.NoError(t, err)
		require.Empty(t, listInfo.Uploads)

		uploadID, err := layer.NewMultipartUpload(ctx, testBucket, testFile, minio.ObjectOptions{})
		require.NoError(t, err)

		now := time.Now()
		totalPartsCount := 3
		minioReaders := make([]*minio.PutObjReader, 3)
		for i := 0; i < totalPartsCount; i++ {
			minioReaders[i] = newMinioPutObjReader(t)
			info, err := layer.PutObjectPart(ctx, testBucket, testFile, uploadID, i+1, minioReaders[i], minio.ObjectOptions{})
			require.NoError(t, err)
			assert.Equal(t, i+1, info.PartNumber)
			assert.Equal(t, minioReaders[i].Size(), info.Size)
			assert.Equal(t, minioReaders[i].ActualSize(), info.ActualSize)
			assert.Equal(t, minioReaders[i].MD5CurrentHexString(), info.ETag)
		}

		listParts, err := layer.ListObjectParts(ctx, testBucket, testFile, uploadID, 0, totalPartsCount, minio.ObjectOptions{})
		require.NoError(t, err)
		require.Equal(t, testBucket, listParts.Bucket)
		require.Equal(t, testFile, listParts.Object)
		require.Equal(t, uploadID, listParts.UploadID)
		require.Len(t, listParts.Parts, totalPartsCount)
		for i := 0; i < totalPartsCount; i++ {
			assert.Equal(t, i+1, listParts.Parts[i].PartNumber)
			assert.Equal(t, minioReaders[i].Size(), listParts.Parts[i].Size)
			assert.Equal(t, minioReaders[i].ActualSize(), listParts.Parts[i].ActualSize)
			assert.WithinDuration(t, now, listParts.Parts[i].LastModified, 5*time.Second)
			assert.Equal(t, minioReaders[i].MD5CurrentHexString(), listParts.Parts[i].ETag)
		}

		// try batch of two
		listParts, err = layer.ListObjectParts(ctx, testBucket, testFile, uploadID, 0, 2, minio.ObjectOptions{})
		require.NoError(t, err)
		require.Equal(t, testBucket, listParts.Bucket)
		require.Equal(t, testFile, listParts.Object)
		require.Equal(t, uploadID, listParts.UploadID)
		require.Equal(t, 0, listParts.PartNumberMarker)
		require.Equal(t, 0+2, listParts.NextPartNumberMarker)
		require.Len(t, listParts.Parts, 2)
		for i := 0; i < 2; i++ {
			assert.Equal(t, i+1, listParts.Parts[i].PartNumber)
			assert.Equal(t, minioReaders[i].Size(), listParts.Parts[i].Size)
			assert.Equal(t, minioReaders[i].ActualSize(), listParts.Parts[i].ActualSize)
			assert.WithinDuration(t, now, listParts.Parts[i].LastModified, 5*time.Second)
			assert.Equal(t, minioReaders[i].MD5CurrentHexString(), listParts.Parts[i].ETag)
		}

		// try batch of remaining
		listParts, err = layer.ListObjectParts(ctx, testBucket, testFile, uploadID, 2, 1000, minio.ObjectOptions{})
		require.NoError(t, err)
		require.Equal(t, testBucket, listParts.Bucket)
		require.Equal(t, testFile, listParts.Object)
		require.Equal(t, uploadID, listParts.UploadID)
		require.Equal(t, 2, listParts.PartNumberMarker)
		require.Equal(t, totalPartsCount, listParts.NextPartNumberMarker)
		require.Len(t, listParts.Parts, totalPartsCount-2)
		for i := 0; i < totalPartsCount-2; i++ {
			assert.Equal(t, i+1+2, listParts.Parts[i].PartNumber)
			assert.Equal(t, minioReaders[i].Size(), listParts.Parts[i].Size)
			assert.Equal(t, minioReaders[i].ActualSize(), listParts.Parts[i].ActualSize)
			assert.WithinDuration(t, now, listParts.Parts[i].LastModified, 5*time.Second)
			assert.Equal(t, minioReaders[i].MD5CurrentHexString(), listParts.Parts[i].ETag)
		}
	})
}

func TestAbortMultipartUpload(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// invalid upload
		err := layer.AbortMultipartUpload(ctx, testBucket, testFile, "uploadID", minio.ObjectOptions{})
		require.Error(t, err)

		bucket, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)
		require.Equal(t, bucket.Name, testBucket)

		uploadID, err := layer.NewMultipartUpload(ctx, testBucket, testFile, minio.ObjectOptions{})
		require.NoError(t, err)

		err = layer.AbortMultipartUpload(ctx, testBucket, testFile, uploadID, minio.ObjectOptions{})
		require.NoError(t, err)
	})
}

func TestCompleteMultipartUpload(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		_, err := layer.CompleteMultipartUpload(ctx, "bucket", "object", "invalid-upload", nil, minio.ObjectOptions{})
		require.Error(t, err)

		bucket, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)
		require.Equal(t, bucket.Name, testBucket)

		listInfo, err := layer.ListMultipartUploads(ctx, testBucket, "", "", "", "", 1)
		require.NoError(t, err)
		require.Empty(t, listInfo.Uploads)

		metadata := map[string]string{
			"content-type":         "text/plain",
			xhttp.AmzObjectTagging: "key1=value1&key2=value2",
		}

		uploadID, err := layer.NewMultipartUpload(ctx, testBucket, testFile, minio.ObjectOptions{UserDefined: metadata})
		require.NoError(t, err)

		totalPartsCount := 3
		completeParts := make([]minio.CompletePart, 0, totalPartsCount)
		for i := 1; i <= totalPartsCount; i++ {
			info, err := layer.PutObjectPart(ctx, testBucket, testFile, uploadID, i, newMinioPutObjReader(t), minio.ObjectOptions{})
			require.NoError(t, err)
			require.Equal(t, i, info.PartNumber)
			completeParts = append(completeParts, minio.CompletePart{
				ETag:       info.ETag,
				PartNumber: i,
			})
		}

		expectedMetadata := map[string]string{
			"content-type": "text/plain",
			"s3:tags":      "key1=value1&key2=value2",
		}

		_, err = layer.CompleteMultipartUpload(ctx, testBucket, testFile, uploadID, completeParts, minio.ObjectOptions{})
		require.NoError(t, err)

		obj, err := layer.ListObjects(ctx, testBucket, testFile, "", "", 2)
		require.NoError(t, err)
		require.Len(t, obj.Objects, 1)
		require.Equal(t, testBucket, obj.Objects[0].Bucket)
		require.Equal(t, testFile, obj.Objects[0].Name)

		expectedMetadata["s3:etag"] = obj.Objects[0].ETag

		require.Equal(t, expectedMetadata, obj.Objects[0].UserDefined)
	})
}

func TestCompleteMultipartUploadPartNumberETagChecks(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		for _, tt := range [...]struct {
			name                string
			expectedPartNumbers []int
			receivedPartNumbers []int
			receivedETags       []string
			err                 error
		}{
			{
				name:                "ok",
				expectedPartNumbers: []int{1, 2},
				receivedPartNumbers: []int{1, 2},
				receivedETags:       []string{"098f6bcd4621d373cade4e832627b4f6", "098f6bcd4621d373cade4e832627b4f6"},
				err:                 nil,
			},
			{
				name:                "more expected than received parts I",
				expectedPartNumbers: []int{1, 2},
				receivedPartNumbers: []int{1},
				receivedETags:       []string{"098f6bcd4621d373cade4e832627b4f6"},
				err:                 minio.InvalidPart{PartNumber: 2, ExpETag: "098f6bcd4621d373cade4e832627b4f6", GotETag: ""},
			},
			{
				name:                "more expected than received parts II",
				expectedPartNumbers: []int{1},
				receivedPartNumbers: nil,
				receivedETags:       nil,
				err:                 minio.InvalidPart{PartNumber: 1, ExpETag: "098f6bcd4621d373cade4e832627b4f6", GotETag: ""},
			},
			{
				name:                "invalid part number",
				expectedPartNumbers: []int{1, 2},
				receivedPartNumbers: []int{8, 9},
				receivedETags:       []string{"098f6bcd4621d373cade4e832627b4f6", "098f6bcd4621d373cade4e832627b4f6"},
				err:                 minio.InvalidPart{PartNumber: 8, ExpETag: "", GotETag: "098f6bcd4621d373cade4e832627b4f6"},
			},
			{
				name:                "invalid etag",
				expectedPartNumbers: []int{1, 2, 3, 4, 5},
				receivedPartNumbers: []int{1, 2, 3, 4, 5},
				receivedETags:       []string{"shine", "on", "you", "crazy", "etag"},
				err:                 minio.InvalidPart{PartNumber: 1, ExpETag: "098f6bcd4621d373cade4e832627b4f6", GotETag: "shine"},
			},
			{
				name:                "more received than expected parts I",
				expectedPartNumbers: []int{1},
				receivedPartNumbers: []int{1, 2},
				receivedETags:       []string{"098f6bcd4621d373cade4e832627b4f6", "098f6bcd4621d373cade4e832627b4f6"},
				err:                 minio.InvalidPart{PartNumber: 2, ExpETag: "", GotETag: "098f6bcd4621d373cade4e832627b4f6"},
			},
			{
				name:                "more received than expected parts II",
				expectedPartNumbers: []int{},
				receivedPartNumbers: []int{1},
				receivedETags:       []string{"098f6bcd4621d373cade4e832627b4f6"},
				err:                 minio.InvalidPart{PartNumber: 1, ExpETag: "", GotETag: "098f6bcd4621d373cade4e832627b4f6"},
			},
			{
				name:                "all over the place I",
				expectedPartNumbers: []int{1, 2},
				receivedPartNumbers: []int{3, 4, 99},
				receivedETags:       []string{"acerola", "apple", "apricots", "avocado"},
				err:                 minio.InvalidPart{PartNumber: 3, ExpETag: "", GotETag: "acerola"},
			},
			{
				name:                "all over the place II",
				expectedPartNumbers: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
				receivedPartNumbers: []int{1, 2, 9000},
				receivedETags:       []string{"", "", "3"},
				err:                 minio.InvalidPart{PartNumber: 1, ExpETag: "098f6bcd4621d373cade4e832627b4f6", GotETag: ""},
			},
			{
				name:                "all over the place III",
				expectedPartNumbers: []int{9000, 2, 1},
				receivedPartNumbers: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
				receivedETags:       []string{"098f6bcd4621d373cade4e832627b4f6", "2", ""},
				err:                 minio.InvalidPart{PartNumber: 2, ExpETag: "098f6bcd4621d373cade4e832627b4f6", GotETag: "2"},
			},
		} {
			bucket, err := project.CreateBucket(ctx, testrand.BucketName())
			require.NoError(t, err, tt.name)
			object := testrand.Path()
			uploadID, err := layer.NewMultipartUpload(ctx, bucket.Name, object, minio.ObjectOptions{})
			require.NoError(t, err, tt.name)
			defer func(name string) {
				if err = layer.AbortMultipartUpload(ctx, bucket.Name, object, uploadID, minio.ObjectOptions{}); err != nil {
					assert.ErrorIs(t, err, minio.InvalidUploadID{Bucket: bucket.Name, Object: object, UploadID: uploadID}, name)
				}
			}(tt.name)

			for _, n := range tt.expectedPartNumbers {
				_, err = layer.PutObjectPart(ctx, bucket.Name, object, uploadID, n, newMinioPutObjReader(t), minio.ObjectOptions{})
				require.NoError(t, err, tt.name)
			}

			var uploadedParts []minio.CompletePart
			for i, n := range tt.receivedPartNumbers {
				p := minio.CompletePart{
					PartNumber: n,
				}
				if len(tt.receivedETags) > i {
					p.ETag = tt.receivedETags[i]
				}
				uploadedParts = append(uploadedParts, p)
			}

			_, err = layer.CompleteMultipartUpload(ctx, bucket.Name, object, uploadID, uploadedParts, minio.ObjectOptions{})
			if tt.err != nil {
				assert.ErrorIs(t, err, tt.err, tt.name)
			} else {
				require.NoError(t, err, tt.name)
			}
		}
	})
}

func TestCompleteMultipartUploadSizeCheck(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, _ minio.ObjectLayer, project *uplink.Project) {
		// Reconfigure the layer so we can hit MinPartSize limit.
		c := miniogw.S3CompatibilityConfig{MinPartSize: 50 * memory.KiB.Int64()}
		l, err := miniogw.NewStorjGateway(c).NewGatewayLayer(auth.Credentials{})
		require.NoError(t, err)

		defer func() { require.NoError(t, l.Shutdown(ctx)) }()

		for _, tt := range [...]struct {
			name  string
			sizes []memory.Size
			err   error
		}{
			{
				name:  "all parts meet or are above the size threshold",
				sizes: []memory.Size{50 * memory.KiB, 51 * memory.KiB, 52 * memory.KiB},
				err:   nil,
			},
			{
				name:  "the last part is below the size threshold",
				sizes: []memory.Size{51 * memory.KiB, 52 * memory.KiB, 1 * memory.KiB},
				err:   nil,
			},
			{
				name:  "the middle part is below the size threshold",
				sizes: []memory.Size{51 * memory.KiB, 0 * memory.KiB, 53 * memory.KiB},
				// In tests, we rewrite this 0 KiB part with a part of size 7
				// (still under the threshold) and the following ETag.
				err: minio.PartTooSmall{PartNumber: 2, PartSize: 7, PartETag: "d69b751558e2033dd8e63fa124676d5a"},
			},
			{
				name:  "only one part and meets the size threshold",
				sizes: []memory.Size{50 * memory.KiB},
				err:   nil,
			},
			{
				name:  "only one part and is above the size threshold",
				sizes: []memory.Size{1123 * memory.KiB},
				err:   nil,
			},
			{
				name:  "only one part and is below the size threshold",
				sizes: []memory.Size{25 * memory.KiB},
				err:   nil,
			},
		} {
			bucket, err := project.CreateBucket(ctx, testrand.BucketName())
			require.NoError(t, err, tt.name)
			object := testrand.Path()
			uploadID, err := l.NewMultipartUpload(ctx, bucket.Name, object, minio.ObjectOptions{})
			require.NoError(t, err, tt.name)
			defer func(name string) {
				if err = l.AbortMultipartUpload(ctx, bucket.Name, object, uploadID, minio.ObjectOptions{}); err != nil {
					assert.ErrorIs(t, err, minio.InvalidUploadID{Bucket: bucket.Name, Object: object, UploadID: uploadID}, name)
				}
			}(tt.name)

			var uploadedParts []minio.CompletePart
			for i, n := range tt.sizes {
				var b []byte
				if n == 0 {
					b = []byte("3.14159")
				} else {
					b = testrand.Bytes(n)
				}
				h, err := hash.NewReader(bytes.NewReader(b), int64(len(b)), md5Hex(b), sha256Hex(b), int64(len(b)))
				require.NoError(t, err, tt.name)
				r := minio.NewPutObjReader(h)
				p, err := l.PutObjectPart(ctx, bucket.Name, object, uploadID, i+1, r, minio.ObjectOptions{})
				require.NoError(t, err, tt.name)
				uploadedParts = append(uploadedParts, minio.CompletePart{PartNumber: p.PartNumber, ETag: p.ETag})
			}

			_, err = l.CompleteMultipartUpload(ctx, bucket.Name, object, uploadID, uploadedParts, minio.ObjectOptions{})
			if tt.err != nil {
				assert.ErrorIs(t, err, tt.err, tt.name)
			} else {
				require.NoError(t, err, tt.name)
			}
		}
	})
}

func TestDeleteObjectWithNoReadOrListPermission(t *testing.T) {
	t.Parallel()

	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.DefaultPathCipher = storj.EncNull
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		access := planet.Uplinks[0].Access[planet.Satellites[0].ID()]

		// Create the bucket using the Uplink API
		testBucketInfo, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		// Create the object using the Uplink API
		_, err = createFile(ctx, project, testBucketInfo.Name, testFile, nil, nil)
		require.NoError(t, err)

		// Restrict the access grant to deletes only
		restrictedAccess, err := access.Share(uplink.Permission{AllowDelete: true})
		require.NoError(t, err)

		projectWithRestrictedAccess, err := uplink.OpenProject(ctx, restrictedAccess)
		require.NoError(t, err)

		defer func() { require.NoError(t, projectWithRestrictedAccess.Close()) }()

		// Establish new context with *uplink.Project for the gateway to pick up.
		ctxWithProject := miniogw.WithUplinkProject(ctx, projectWithRestrictedAccess)

		s3Compatibility := miniogw.S3CompatibilityConfig{
			IncludeCustomMetadataListing: true,
			MaxKeysLimit:                 maxKeysLimit,
			MaxKeysExhaustiveLimit:       100000,
		}

		layer, err := miniogw.NewStorjGateway(s3Compatibility).NewGatewayLayer(auth.Credentials{})
		require.NoError(t, err)

		defer func() { require.NoError(t, layer.Shutdown(ctxWithProject)) }()

		// Delete the object info using the Minio API
		deleted, err := layer.DeleteObject(ctxWithProject, testBucket, testFile, minio.ObjectOptions{})
		require.NoError(t, err)

		require.Equal(t, testBucket, deleted.Bucket)
		require.Empty(t, deleted.Name)

		// Check that the object is deleted using the Uplink API
		_, err = project.StatObject(ctx, testBucketInfo.Name, testFile)
		require.ErrorIs(t, err, uplink.ErrObjectNotFound)
	})
}

func TestListObjectVersions(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		_, err := layer.ListObjectVersions(ctx, "bucket", "prefix", "marker", "versionMarker", "delimiter", 0)
		require.EqualError(t, err, minio.NotImplemented{}.Error())
	})
}

// TODO(artur): TestPutObjectTags should check all PutObjectTags's return
// values.
func TestPutObjectTags(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Check the error when putting object tags from a bucket with empty name
		_, err := layer.PutObjectTags(ctx, "", "", "key1=value1", minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNameInvalid{}, err)

		// Check the error when putting object tags with a non-existent bucket
		_, err = layer.PutObjectTags(ctx, testBucket, testFile, "key1=value1", minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNotFound{Bucket: testBucket}, err)

		// Create the bucket using the Uplink API
		testBucketInfo, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		// Check the error when putting object tags for an object with empty name
		_, err = layer.PutObjectTags(ctx, testBucket, "", "key1=value1", minio.ObjectOptions{})
		assert.Equal(t, minio.ObjectNameInvalid{Bucket: testBucket}, err)

		// Check the error when putting object tags with a non-existing object
		_, err = layer.PutObjectTags(ctx, testBucket, testFile, "key1=value1", minio.ObjectOptions{})
		assert.Equal(t, minio.ObjectNotFound{Bucket: testBucket, Object: testFile}, err)

		// These are the object tags we want to put
		objectTags := "key3=value3&key4=value4"

		// This is the tag map expected from GetObjectTags
		expectedObjectTags := map[string]string{
			"key3": "value3",
			"key4": "value4",
		}

		// Create the object using the Uplink API
		_, err = createFile(ctx, project, testBucketInfo.Name, testFile, []byte("test"), nil)
		require.NoError(t, err)

		_, err = layer.PutObjectTags(ctx, testBucket, testFile, objectTags, minio.ObjectOptions{})
		require.NoError(t, err)

		ts, err := layer.GetObjectTags(ctx, testBucket, testFile, minio.ObjectOptions{})
		require.NoError(t, err)
		assert.Equal(t, expectedObjectTags, ts.ToMap())

		// Test that sending an empty tag set is effectively the same as deleting them
		_, err = layer.PutObjectTags(ctx, testBucket, testFile, "", minio.ObjectOptions{})
		require.NoError(t, err)

		ts, err = layer.GetObjectTags(ctx, testBucket, testFile, minio.ObjectOptions{})
		require.NoError(t, err)
		assert.Empty(t, ts.ToMap())
	})
}

func TestGetObjectTags(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Check the error when getting object tags from a bucket with empty name
		_, err := layer.GetObjectTags(ctx, "", "", minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNameInvalid{}, err)

		// Check the error when getting object tags with a non-existent bucket
		_, err = layer.GetObjectTags(ctx, testBucket, testFile, minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNotFound{Bucket: testBucket}, err)

		// Create the bucket using the Uplink API
		testBucketInfo, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		// Check the error when getting object tags for an object with empty name
		_, err = layer.GetObjectTags(ctx, testBucket, "", minio.ObjectOptions{})
		assert.Equal(t, minio.ObjectNameInvalid{Bucket: testBucket}, err)

		// Check the error when getting object tags with a non-existent object
		_, err = layer.GetObjectTags(ctx, testBucket, testFile, minio.ObjectOptions{})
		assert.Equal(t, minio.ObjectNotFound{Bucket: testBucket, Object: testFile}, err)

		// This is the custom metadata for the object.
		metadata := map[string]string{
			"content-type": "text/plain",
			"s3:tags":      "key1=value1&key2=value2",
		}

		// These are the expected object tags from that metadata.
		expected := map[string]string{
			"key1": "value1",
			"key2": "value2",
		}

		// Create the object using the Uplink API
		_, err = createFile(ctx, project, testBucketInfo.Name, testFile, []byte("test"), metadata)
		require.NoError(t, err)

		ts, err := layer.GetObjectTags(ctx, testBucket, testFile, minio.ObjectOptions{})
		require.NoError(t, err)
		assert.Equal(t, expected, ts.ToMap())

		metadataNoObjectTags := map[string]string{
			"content-type": "text/plain",
		}

		_, err = createFile(ctx, project, testBucketInfo.Name, testFile, []byte("test"), metadataNoObjectTags)
		require.NoError(t, err)

		ts, err = layer.GetObjectTags(ctx, testBucket, testFile, minio.ObjectOptions{})
		require.NoError(t, err)
		assert.Empty(t, ts.ToMap())

		metadataEmptyObjectTags := map[string]string{
			"content-type": "text/plain",
			"s3:tags":      "",
		}

		_, err = createFile(ctx, project, testBucketInfo.Name, testFile, []byte("test"), metadataEmptyObjectTags)
		require.NoError(t, err)

		ts, err = layer.GetObjectTags(ctx, testBucket, testFile, minio.ObjectOptions{})
		require.NoError(t, err)
		assert.Empty(t, ts.ToMap())
	})
}

// TODO(artur): TestDeleteObjectTags should check all DeleteObjectTags's return
// values.
func TestDeleteObjectTags(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Check the error when deleting object tags from a bucket with empty name
		_, err := layer.DeleteObjectTags(ctx, "", "", minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNameInvalid{}, err)

		// Check the error when deleting object tags with a non-existent bucket
		_, err = layer.DeleteObjectTags(ctx, testBucket, testFile, minio.ObjectOptions{})
		assert.Equal(t, minio.BucketNotFound{Bucket: testBucket}, err)

		// Create the bucket using the Uplink API
		testBucketInfo, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		// Check the error when deleting object tags for an object with empty name
		_, err = layer.DeleteObjectTags(ctx, testBucket, "", minio.ObjectOptions{})
		assert.Equal(t, minio.ObjectNameInvalid{Bucket: testBucket}, err)

		// Check the error when deleting object tags for a non-existing object
		_, err = layer.DeleteObjectTags(ctx, testBucket, testFile, minio.ObjectOptions{})
		assert.Equal(t, minio.ObjectNotFound{Bucket: testBucket, Object: testFile}, err)

		metadata := map[string]string{
			"s3:tags": "key5=value5&key6=value6",
		}

		// Create the object using the Uplink API
		_, err = createFile(ctx, project, testBucketInfo.Name, testFile, []byte("test"), metadata)
		require.NoError(t, err)

		_, err = layer.DeleteObjectTags(ctx, testBucket, testFile, minio.ObjectOptions{})
		require.NoError(t, err)

		ts, err := layer.GetObjectTags(ctx, testBucket, testFile, minio.ObjectOptions{})
		require.NoError(t, err)
		assert.Empty(t, ts.ToMap())
	})
}

func TestObjectNameTooLong(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, _ *uplink.Project) {
		bucket := testrand.BucketName()
		object := string(testrand.Bytes(memory.KiB + 1))

		_, err := layer.PutObject(ctx, bucket, object, nil, minio.ObjectOptions{})
		require.ErrorIs(t, err, minio.ObjectNameTooLong{Bucket: bucket, Object: object})

		object = string(testrand.Bytes(10 * memory.KiB))

		_, err = layer.NewMultipartUpload(ctx, bucket, object, minio.ObjectOptions{})
		require.ErrorIs(t, err, minio.ObjectNameTooLong{Bucket: bucket, Object: object})

		object = string(testrand.Bytes(memory.MiB))

		_, err = layer.CopyObject(ctx, "", "", bucket, object, minio.ObjectInfo{}, minio.ObjectOptions{}, minio.ObjectOptions{})
		require.ErrorIs(t, err, minio.ObjectNameTooLong{Bucket: bucket, Object: object})
	})
}

func TestBucketNameInvalid(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		for _, bucket := range []string{
			"",
			"test.",
			"-test",
			"test-",
			"!test",
			"Test",
			"a",
			".",
			"1.1.1.1",
			string(testrand.Bytes(memory.KiB + 1)),
		} {
			require.ErrorIs(t, layer.MakeBucketWithLocation(ctx, bucket, minio.BucketOptions{}),
				minio.BucketNameInvalid{Bucket: bucket}, bucket,
				bucket)

			require.ErrorIs(t, layer.DeleteBucket(ctx, bucket, false),
				minio.BucketNameInvalid{Bucket: bucket},
				bucket)

			_, err := layer.ListObjects(ctx, bucket, "", "", "", 1000)
			require.ErrorIs(t, err, minio.BucketNameInvalid{Bucket: bucket}, bucket)

			_, err = layer.ListObjectsV2(ctx, bucket, "", "", "", 1000, false, "")
			require.ErrorIs(t, err, minio.BucketNameInvalid{Bucket: bucket}, bucket)

			_, err = layer.GetObjectNInfo(ctx, bucket, "test", nil, nil, 0, minio.ObjectOptions{})
			require.ErrorIs(t, err, minio.BucketNameInvalid{Bucket: bucket}, bucket)

			_, err = layer.GetObjectInfo(ctx, bucket, "test", minio.ObjectOptions{})
			require.ErrorIs(t, err, minio.BucketNameInvalid{Bucket: bucket}, bucket)

			_, err = layer.PutObject(ctx, bucket, "test", nil, minio.ObjectOptions{})
			require.ErrorIs(t, err, minio.BucketNameInvalid{Bucket: bucket}, bucket)

			_, err = layer.CopyObject(ctx, bucket, "test", "test", "test", minio.ObjectInfo{}, minio.ObjectOptions{}, minio.ObjectOptions{})
			require.ErrorIs(t, err, minio.BucketNameInvalid{Bucket: bucket}, bucket)

			_, err = layer.CopyObject(ctx, "test", "test", bucket, "test", minio.ObjectInfo{}, minio.ObjectOptions{}, minio.ObjectOptions{})
			require.ErrorIs(t, err, minio.BucketNameInvalid{Bucket: bucket}, bucket)

			_, err = layer.DeleteObject(ctx, bucket, "test", minio.ObjectOptions{})
			require.ErrorIs(t, err, minio.BucketNameInvalid{Bucket: bucket}, bucket)

			_, err = layer.PutObjectTags(ctx, bucket, "test", "", minio.ObjectOptions{})
			require.ErrorIs(t, err, minio.BucketNameInvalid{Bucket: bucket}, bucket)

			_, err = layer.GetObjectTags(ctx, bucket, "test", minio.ObjectOptions{})
			require.ErrorIs(t, err, minio.BucketNameInvalid{Bucket: bucket}, bucket)

			_, err = layer.DeleteObjectTags(ctx, bucket, "test", minio.ObjectOptions{})
			require.ErrorIs(t, err, minio.BucketNameInvalid{Bucket: bucket}, bucket)

			_, err = layer.ListMultipartUploads(ctx, bucket, "", "", "", "", 1000)
			require.ErrorIs(t, err, minio.BucketNameInvalid{Bucket: bucket}, bucket)

			_, err = layer.NewMultipartUpload(ctx, bucket, "test", minio.ObjectOptions{})
			require.ErrorIs(t, err, minio.BucketNameInvalid{Bucket: bucket}, bucket)

			_, err = layer.GetMultipartInfo(ctx, bucket, "test", "", minio.ObjectOptions{})
			require.ErrorIs(t, err, minio.BucketNameInvalid{Bucket: bucket}, bucket)

			_, err = layer.ListObjectParts(ctx, bucket, "test", "", 0, 1000, minio.ObjectOptions{})
			require.ErrorIs(t, err, minio.BucketNameInvalid{Bucket: bucket}, bucket)

			require.ErrorIs(t, layer.AbortMultipartUpload(ctx, bucket, "test", "", minio.ObjectOptions{}),
				minio.BucketNameInvalid{Bucket: bucket},
				bucket)

			_, err = layer.CompleteMultipartUpload(ctx, bucket, "test", "", []minio.CompletePart{}, minio.ObjectOptions{})
			require.ErrorIs(t, err, minio.BucketNameInvalid{Bucket: bucket}, bucket)
		}
	})
}

func TestObjectTTL(t *testing.T) {
	t.Parallel()

	runTest(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		bucket := testrand.BucketName()

		_, err := project.CreateBucket(ctx, bucket)
		require.NoError(t, err)

		for _, tt := range []struct {
			ttl         string
			expectedTTL time.Time
			expectedErr error
		}{
			{
				ttl:         "none",
				expectedTTL: time.Time{},
				expectedErr: nil,
			},
			{
				ttl:         "",
				expectedTTL: time.Time{},
				expectedErr: nil,
			},
			{
				ttl:         "+2h",
				expectedTTL: time.Now().Add(2 * time.Hour).UTC(),
				expectedErr: nil,
			},
			{
				ttl:         time.Unix(2147483647, 0).UTC().Format(time.RFC3339),
				expectedTTL: time.Unix(2147483647, 0).UTC(),
				expectedErr: nil,
			},
			{
				ttl:         "-2h",
				expectedTTL: time.Time{},
				expectedErr: miniogw.ErrInvalidTTL,
			},
			{
				ttl:         "now",
				expectedTTL: time.Time{},
				expectedErr: miniogw.ErrInvalidTTL,
			},
			{
				ttl:         "+0xc0ffeeµs",
				expectedTTL: time.Time{},
				expectedErr: miniogw.ErrInvalidTTL,
			},
		} {
			testObjectTTL(ctx, t, layer, project, bucket, "X-Amz-Meta-Object-Expires", tt.ttl, tt.expectedTTL, tt.expectedErr)
			testObjectTTL(ctx, t, layer, project, bucket, "X-Minio-Meta-Object-Expires", tt.ttl, tt.expectedTTL, tt.expectedErr)
			testObjectTTL(ctx, t, layer, project, bucket, "X-Amz-Meta-Storj-Expires", tt.ttl, tt.expectedTTL, tt.expectedErr)
			testObjectTTL(ctx, t, layer, project, bucket, "X-Minio-Meta-Storj-Expires", tt.ttl, tt.expectedTTL, tt.expectedErr)
			testMultipartObjectTTL(ctx, t, layer, project, bucket, "X-Amz-Meta-Object-Expires", tt.ttl, tt.expectedTTL, tt.expectedErr)
			testMultipartObjectTTL(ctx, t, layer, project, bucket, "X-Minio-Meta-Object-Expires", tt.ttl, tt.expectedTTL, tt.expectedErr)
			testMultipartObjectTTL(ctx, t, layer, project, bucket, "X-Amz-Meta-Storj-Expires", tt.ttl, tt.expectedTTL, tt.expectedErr)
			testMultipartObjectTTL(ctx, t, layer, project, bucket, "X-Minio-Meta-Storj-Expires", tt.ttl, tt.expectedTTL, tt.expectedErr)
		}
	})
}

func testObjectTTL(
	ctx context.Context,
	t *testing.T,
	layer minio.ObjectLayer,
	project *uplink.Project,
	bucket, ttlKey, ttl string,
	expectedTTL time.Time,
	expectedErr error,
) {
	object := testrand.Path()

	_, err := layer.PutObject(ctx, bucket, object, nil, minio.ObjectOptions{
		UserDefined: map[string]string{
			ttlKey: ttl,
		},
	})
	if expectedErr != nil {
		require.ErrorIs(t, err, expectedErr)
		return
	}
	require.NoError(t, err)

	downloaded, err := project.DownloadObject(ctx, bucket, object, nil)
	require.NoError(t, err)

	assert.WithinDuration(t, expectedTTL, downloaded.Info().System.Expires, time.Minute)

	require.NoError(t, downloaded.Close())
}

func testMultipartObjectTTL(
	ctx context.Context,
	t *testing.T,
	layer minio.ObjectLayer,
	project *uplink.Project,
	bucket, ttlKey, ttl string,
	expectedTTL time.Time,
	expectedErr error,
) {
	object := testrand.Path()

	uploadID, err := layer.NewMultipartUpload(ctx, bucket, object, minio.ObjectOptions{
		UserDefined: map[string]string{
			ttlKey: ttl,
		},
	})
	if expectedErr != nil {
		require.ErrorIs(t, err, expectedErr)
		return
	}
	require.NoError(t, err)

	// Call GetMultipartInfo to check the pending upload is discoverable correctly.
	_, err = layer.GetMultipartInfo(ctx, bucket, object, uploadID, minio.ObjectOptions{})
	require.NoError(t, err)

	_, err = layer.CompleteMultipartUpload(ctx, bucket, object, uploadID, nil, minio.ObjectOptions{})
	require.NoError(t, err)

	downloaded, err := project.DownloadObject(ctx, bucket, object, nil)
	require.NoError(t, err)

	assert.WithinDuration(t, expectedTTL, downloaded.Info().System.Expires, time.Minute)

	require.NoError(t, downloaded.Close())
}

// md5Hex returns MD5 hash in hex encoding of given data.
func md5Hex(data []byte) string {
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:])
}

// sha256Hex returns SHA-256 hash in hex encoding of given data.
func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestProjectUsageLimit(t *testing.T) {
	t.Parallel()

	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.DefaultPathCipher = storj.EncNull
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satDB := planet.Satellites[0].DB
		acctDB := satDB.ProjectAccounting()

		now := time.Now()

		// set custom bandwidth limit for project 512 Kb
		bandwidthLimit := 500 * memory.KiB
		err := acctDB.UpdateProjectBandwidthLimit(ctx, planet.Uplinks[0].Projects[0].ID, bandwidthLimit)
		require.NoError(t, err)

		dataSize := 100 * memory.KiB
		data := testrand.Bytes(dataSize)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		// Establish new context with *uplink.Project for the gateway to pick up.
		ctxWithProject := miniogw.WithUplinkProject(ctx, project)

		s3Compatibility := miniogw.S3CompatibilityConfig{
			IncludeCustomMetadataListing: true,
			MaxKeysLimit:                 maxKeysLimit,
			MaxKeysExhaustiveLimit:       100000,
		}

		layer, err := miniogw.NewStorjGateway(s3Compatibility).NewGatewayLayer(auth.Credentials{})
		require.NoError(t, err)

		defer func() { require.NoError(t, layer.Shutdown(ctxWithProject)) }()

		// Create a bucket with the Minio API
		err = layer.MakeBucketWithLocation(ctxWithProject, "testbucket", minio.BucketOptions{})
		require.NoError(t, err)

		hashReader, err := hash.NewReader(bytes.NewReader(data), int64(dataSize), md5Hex(data), sha256Hex(data), int64(dataSize))
		require.NoError(t, err)
		putObjectReader := minio.NewPutObjReader(hashReader)

		info, err := layer.PutObject(ctxWithProject, "testbucket", "test/path1", putObjectReader, minio.ObjectOptions{UserDefined: make(map[string]string)})
		require.NoError(t, err)
		assert.Equal(t, "test/path1", info.Name)
		assert.Equal(t, "testbucket", info.Bucket)
		assert.False(t, info.IsDir)
		assert.True(t, time.Since(info.ModTime) < 1*time.Minute)
		assert.NotEmpty(t, info.ETag)

		time.Sleep(10 * time.Second)
		// We'll be able to download 5X before reach the limit.
		for i := 0; i < 5; i++ {
			err = minio.GetObject(ctxWithProject, layer, "testbucket", "test/path1", 0, -1, io.Discard, "", minio.ObjectOptions{})
			require.NoError(t, err)
		}

		// An extra download should return 'Exceeded Usage Limit' error
		err = minio.GetObject(ctxWithProject, layer, "testbucket", "test/path1", 0, -1, io.Discard, "", minio.ObjectOptions{})
		require.Error(t, err)
		require.ErrorIs(t, err, miniogw.ErrBandwidthLimitExceeded)

		// Simulate new billing cycle (next month)
		planet.Satellites[0].API.Accounting.ProjectUsage.SetNow(func() time.Time {
			return time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		})

		// Should not return an error since it's a new month
		err = minio.GetObject(ctxWithProject, layer, "testbucket", "test/path1", 0, -1, io.Discard, "", minio.ObjectOptions{})
		require.NoError(t, err)
	})
}

// TestSlowDown tests whether uplink.ErrTooManyRequests error converts to
// ErrSlowDown correctly.
func TestSlowDown(t *testing.T) {
	t.Parallel()

	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.RateLimiter.Rate = 0
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		// Establish new context with *uplink.Project for the gateway to pick up.
		ctxWithProject := miniogw.WithUplinkProject(ctx, project)

		s3Compatibility := miniogw.S3CompatibilityConfig{
			IncludeCustomMetadataListing: true,
			MaxKeysLimit:                 1000,
			MaxKeysExhaustiveLimit:       100000,
		}

		layer, err := miniogw.NewStorjGateway(s3Compatibility).NewGatewayLayer(auth.Credentials{})
		require.NoError(t, err)

		defer func() { require.NoError(t, layer.Shutdown(ctxWithProject)) }()

		require.ErrorIs(t, layer.MakeBucketWithLocation(ctxWithProject, testrand.BucketName(), minio.BucketOptions{}), miniogw.ErrSlowDown)
	})
}

func runTest(t *testing.T, test func(*testing.T, context.Context, minio.ObjectLayer, *uplink.Project)) {
	runTestWithPathCipher(t, storj.EncNull, test)
}

func runTestWithPathCipher(t *testing.T, pathCipher storj.CipherSuite, test func(*testing.T, context.Context, minio.ObjectLayer, *uplink.Project)) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.MaxSegmentSize = segmentSize
			},
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.DefaultPathCipher = pathCipher
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		// Establish new context with *uplink.Project for the gateway to pick up.
		ctxWithProject := miniogw.WithUplinkProject(ctx, project)

		s3Compatibility := miniogw.S3CompatibilityConfig{
			IncludeCustomMetadataListing: true,
			MaxKeysLimit:                 maxKeysLimit,
			MaxKeysExhaustiveLimit:       100000,
			MaxUploadsLimit:              maxUploadsLimit,
		}

		layer, err := miniogw.NewStorjGateway(s3Compatibility).NewGatewayLayer(auth.Credentials{})
		require.NoError(t, err)

		defer func() { require.NoError(t, layer.Shutdown(ctxWithProject)) }()

		test(t, ctxWithProject, layer, project)
	})
}

func createFile(ctx context.Context, project *uplink.Project, bucket, key string, data []byte, metadata map[string]string) (*uplink.Object, error) {
	upload, err := project.UploadObject(ctx, bucket, key, nil)
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(upload, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}

	err = upload.SetCustomMetadata(ctx, metadata)
	if err != nil {
		return nil, err
	}

	err = upload.Commit()
	if err != nil {
		return nil, err
	}

	return upload.Info(), nil
}

func newMinioPutObjReader(t *testing.T) *minio.PutObjReader {
	hashReader, err := hash.NewReader(
		bytes.NewReader([]byte("test")),
		int64(len("test")),
		"098f6bcd4621d373cade4e832627b4f6",
		"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
		int64(len("test")))
	require.NoError(t, err)

	return minio.NewPutObjReader(hashReader)
}
