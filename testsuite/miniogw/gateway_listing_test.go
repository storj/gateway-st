// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/gateway/miniogw"
	minio "storj.io/minio/cmd"
	"storj.io/minio/pkg/auth"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
)

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

		testListObjects(t, f, true)
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

		testListObjectsLimits(t, f, true)
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

		testListObjects(t, f, true)
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

		testListObjectsLimits(t, f, true)
	})
}

func TestListObjectVersions(t *testing.T) {
	t.Parallel()

	f := func(ctx context.Context, layer minio.ObjectLayer, bucket, prefix, marker, delimiter string, maxKeys int) ([]string, []minio.ObjectInfo, string, string, bool, error) {
		list, err := layer.ListObjectVersions(ctx, bucket, prefix, marker, "", delimiter, maxKeys)
		if err != nil {
			return nil, nil, "", "", false, err
		}
		return list.Prefixes, list.Objects, marker, list.NextMarker, list.IsTruncated, nil
	}

	t.Run("once", func(t *testing.T) {
		t.Parallel()

		testListObjects(t, f, false)
	})
	t.Run("loop", func(t *testing.T) {
		t.Parallel()

		testListObjectsLoop(t, f)
	})
	t.Run("stat", func(t *testing.T) {
		t.Parallel()

		testListObjectsStatLoop(t, f)
	})

	// NOTE(artur): testListObjectsArbitraryPrefixDelimiter &&
	// testListObjectsArbitraryPrefixDelimiter are going to fail for
	// ListObjectVersions because it doesn't support arbitrary
	// delimiters.

	t.Run("limits", func(t *testing.T) {
		t.Parallel()

		testListObjectsLimits(t, f, false)
	})
}

func testListObjects(t *testing.T, listObjects listObjectsFunc, testListSingleOptimization bool) {
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
			name                          string
			prefix                        string
			marker                        string
			delimiter                     string
			maxKeys                       int
			more                          bool
			prefixes                      []string
			objects                       []string
			listSingleOptimizationRelated bool
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
				marker:    "a/xb",
				delimiter: "/",
				maxKeys:   maxKeysLimit,
				objects:   []string{"xbb", "xc"},
			}, {
				name:      "Prefix non-recursive with mark and max keys",
				prefix:    "a/",
				marker:    "a/xaa",
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
				name:                          "list as stat, recursive, object, prefix, and object-with-prefix exist",
				prefix:                        "i",
				more:                          true,
				maxKeys:                       maxKeysLimit,
				prefixes:                      nil,
				objects:                       []string{"i"},
				listSingleOptimizationRelated: true,
			}, {
				name:                          "list as stat, nonrecursive, object, prefix, and object-with-prefix exist",
				prefix:                        "i",
				delimiter:                     "/",
				maxKeys:                       maxKeysLimit,
				more:                          true,
				prefixes:                      []string{"i/"},
				objects:                       []string{"i"},
				listSingleOptimizationRelated: true,
			}, {
				name:                          "list as stat, recursive, object and prefix exist, no object-with-prefix",
				prefix:                        "j",
				maxKeys:                       maxKeysLimit,
				more:                          true,
				prefixes:                      nil,
				objects:                       []string{"j"},
				listSingleOptimizationRelated: true,
			}, {
				name:                          "list as stat, nonrecursive, object and prefix exist, no object-with-prefix",
				prefix:                        "j",
				delimiter:                     "/",
				maxKeys:                       maxKeysLimit,
				more:                          true,
				prefixes:                      []string{"j/"},
				objects:                       []string{"j"},
				listSingleOptimizationRelated: true,
			}, {
				name:                          "list as stat, recursive, object and object-with-prefix exist, no prefix",
				prefix:                        "k",
				maxKeys:                       maxKeysLimit,
				more:                          true,
				prefixes:                      nil,
				objects:                       []string{"k"},
				listSingleOptimizationRelated: true,
			}, {
				name:                          "list as stat, nonrecursive, object and object-with-prefix exist, no prefix",
				prefix:                        "k",
				delimiter:                     "/",
				maxKeys:                       maxKeysLimit,
				more:                          true,
				prefixes:                      nil,
				objects:                       []string{"k"},
				listSingleOptimizationRelated: true,
			}, {
				name:                          "list as stat, recursive, object exists, no object-with-prefix or prefix",
				prefix:                        "l",
				maxKeys:                       maxKeysLimit,
				more:                          true,
				prefixes:                      nil,
				objects:                       []string{"l"},
				listSingleOptimizationRelated: true,
			}, {
				name:                          "list as stat, nonrecursive, object exists, no object-with-prefix or prefix",
				prefix:                        "l",
				delimiter:                     "/",
				maxKeys:                       maxKeysLimit,
				more:                          true,
				prefixes:                      nil,
				objects:                       []string{"l"},
				listSingleOptimizationRelated: true,
			}, {
				name:     "list as stat, recursive, prefix, and object-with-prefix exist, no object (fallback to exhaustive)",
				prefix:   "m",
				maxKeys:  maxKeysLimit,
				prefixes: nil,
				objects:  []string{"m/i", "mm"},
			}, {
				name:                          "list as stat, nonrecursive, prefix, and object-with-prefix exist, no object",
				prefix:                        "m",
				delimiter:                     "/",
				maxKeys:                       maxKeysLimit,
				more:                          true,
				prefixes:                      []string{"m/"},
				objects:                       nil,
				listSingleOptimizationRelated: true,
			}, {
				name:     "list as stat, recursive, prefix exists, no object-with-prefix, no object (fallback to exhaustive)",
				prefix:   "n",
				maxKeys:  maxKeysLimit,
				prefixes: nil,
				objects:  []string{"n/i"},
			}, {
				name:                          "list as stat, nonrecursive, prefix exists, no object-with-prefix, no object",
				prefix:                        "n",
				delimiter:                     "/",
				maxKeys:                       maxKeysLimit,
				more:                          true,
				prefixes:                      []string{"n/"},
				objects:                       nil,
				listSingleOptimizationRelated: true,
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
			if !testListSingleOptimization && tt.listSingleOptimizationRelated {
				continue
			}

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

func testListObjectsLimits(t *testing.T, listObjects listObjectsFunc, testListExhaustiveOptimization bool) {
	runTest(t, func(t *testing.T, ctx context.Context, _ minio.ObjectLayer, project *uplink.Project) {
		testBucketInfo, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		for _, key := range []string{
			"p",
			"p/a/d",
			"p/b/d",
			"p/c/d",
			"q",
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
				lenObjs:   2,
			},
			{
				prefix:    "p/",
				delimiter: "/",
				lenPres:   2,
				lenObjs:   0,
			},
			{
				prefix:    "p",
				delimiter: "/",
				lenPres:   1,
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

		if testListExhaustiveOptimization {
			_, _, _, _, _, err = listObjects(ctx, l, testBucketInfo.Name, "p/a", "p/b/d", "/d", 1)
			require.ErrorIs(t, err, miniogw.ErrTooManyItemsToList, "MaxKeysExhaustiveLimit")
		}

		require.NoError(t, l.Shutdown(ctx))
	})
}

// testListObjectsCompatibility contains the common test logic for both
// fully compatible and fast arbitrary prefix tests
func testListObjectsCompatibility(t *testing.T, s3Compatibility miniogw.S3CompatibilityConfig) {
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
		ctxWithProject := miniogw.WithCredentials(ctx, project, miniogw.CredentialsInfo{Access: planet.Uplinks[0].Access[planet.Satellites[0].ID()]})

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

		// Only run the arbitrary delimiter test for fully compatible mode
		if s3Compatibility.FullyCompatibleListing {
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

		// Only run the arbitrary delimiter test for fully compatible mode
		if s3Compatibility.FullyCompatibleListing {
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

		// Only run the arbitrary delimiter test for fully compatible mode
		if s3Compatibility.FullyCompatibleListing {
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

		{
			const name = "marker before prefix"

			result, err := layer.ListObjects(ctxWithProject, bucketName, prefix, "aaaa", "/", 100)
			require.NoError(t, err, name)

			assert.False(t, result.IsTruncated, name)
			assert.Empty(t, result.NextMarker, name)
			assert.Empty(t, result.Objects, name)

			require.Len(t, result.Prefixes, 1, name)

			assert.Equal(t, prefix+strings.SplitAfterN(prefix2, "/", 2)[0], result.Prefixes[0], name)
		}

		{
			const name = "marker after prefix"

			result, err := layer.ListObjects(ctxWithProject, bucketName, prefix, "zzzz", "/", 100)
			require.NoError(t, err, name)

			assert.False(t, result.IsTruncated, name)
			assert.Empty(t, result.NextMarker, name)
			assert.Empty(t, result.Objects, name)
			assert.Empty(t, result.Prefixes, name)
		}

		{
			const name = "marker contains prefix"

			result, err := layer.ListObjects(ctxWithProject, bucketName, prefix+prefix2, prefixed[len(prefixed)-2], "", 100)
			require.NoError(t, err, name)

			assert.False(t, result.IsTruncated, name)
			assert.Empty(t, result.NextMarker, name)
			assert.Empty(t, result.Prefixes, name)

			require.Len(t, result.Objects, 1, name)
			assert.Equal(t, prefixed[len(prefixed)-1], result.Objects[0].Name, name)
		}
	})
}

// TestListObjectsFullyCompatible tests whether exhaustive listing's results
// are, e.g., lexicographically ordered. It also tests cases when incorrectly
// implemented obvious exhaustive listing optimizations (in the context of
// libuplink API) might return wrong results. It cannot test whether such
// optimizations exist (there are other ways to check this).
func TestListObjectsFullyCompatible(t *testing.T) {
	t.Parallel()

	s3Compatibility := miniogw.S3CompatibilityConfig{
		IncludeCustomMetadataListing: true,
		MaxKeysLimit:                 50,
		MaxKeysExhaustiveLimit:       100,
		FullyCompatibleListing:       true,
	}

	testListObjectsCompatibility(t, s3Compatibility)
}

// TestListObjectsFastArbitraryPrefixes disables exhaustive listing and path
// encryption to test arbitrary prefix support
func TestListObjectsFastArbitraryPrefixes(t *testing.T) {
	t.Parallel()

	s3Compatibility := miniogw.S3CompatibilityConfig{
		IncludeCustomMetadataListing: true,
		MaxKeysLimit:                 50,
		MaxKeysExhaustiveLimit:       0,
		FullyCompatibleListing:       false,
	}

	testListObjectsCompatibility(t, s3Compatibility)
}

func TestListObjectsFastArbitraryDelimiter(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.DefaultPathCipher = storj.EncNull
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		const (
			slashDelimiter     = "/"
			arbitraryDelimiter = "###"
		)

		sat := planet.Satellites[0]
		up := planet.Uplinks[0]

		project, err := up.OpenProject(ctx, sat)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		// Establish new context with *uplink.Project for the gateway to pick up.
		ctxWithProject := miniogw.WithCredentials(ctx, project, miniogw.CredentialsInfo{Access: up.Access[sat.ID()]})

		layer, err := miniogw.NewStorjGateway(miniogw.S3CompatibilityConfig{
			MaxKeysLimit: 50,
			// Reduce the maximum number of exhaustive listing keys to 0 to ensure that
			// we receive an error whenever exhaustive listing occurs.
			MaxKeysExhaustiveLimit: 0,
			FullyCompatibleListing: false,
		}).NewGatewayLayer(auth.Credentials{})
		require.NoError(t, err)

		defer func() { require.NoError(t, layer.Shutdown(ctxWithProject)) }()

		bucketName := testrand.BucketName()
		_, err = project.EnsureBucket(ctxWithProject, bucketName)
		require.NoError(t, err)

		for _, objectKey := range []string{
			"abc" + arbitraryDelimiter,
			"abc" + arbitraryDelimiter + "def",
			"abc" + arbitraryDelimiter + "def" + arbitraryDelimiter + "ghi",
			"abc" + slashDelimiter + "def",
			"xyz" + arbitraryDelimiter + "uvw",
		} {
			_, err = createFile(ctxWithProject, project, bucketName, objectKey, testrand.Bytes(16), nil)
			require.NoError(t, err)
		}

		objectNames := func(objInfos []minio.ObjectInfo) []string {
			var names []string
			for _, info := range objInfos {
				names = append(names, info.Name)
			}
			return names
		}

		t.Run("Root", func(t *testing.T) {
			result, err := layer.ListObjects(ctxWithProject, bucketName, "", "", arbitraryDelimiter, 5)
			require.NoError(t, err)

			require.Equal(t, []string{"abc" + slashDelimiter + "def"}, objectNames(result.Objects))
			require.Equal(t, []string{
				"abc" + arbitraryDelimiter,
				"xyz" + arbitraryDelimiter,
			}, result.Prefixes)
			require.Empty(t, result.NextMarker)
			require.False(t, result.IsTruncated)
		})

		t.Run("1 level deep", func(t *testing.T) {
			result, err := layer.ListObjects(ctxWithProject, bucketName, "abc"+arbitraryDelimiter, "", arbitraryDelimiter, 5)
			require.NoError(t, err)

			require.Equal(t, []string{
				"abc" + arbitraryDelimiter,
				"abc" + arbitraryDelimiter + "def",
			}, objectNames(result.Objects))
			require.Equal(t, []string{"abc" + arbitraryDelimiter + "def" + arbitraryDelimiter}, result.Prefixes)
			require.Empty(t, result.NextMarker)
			require.False(t, result.IsTruncated)
		})

		t.Run("2 levels deep", func(t *testing.T) {
			result, err := layer.ListObjects(ctxWithProject, bucketName, "abc"+arbitraryDelimiter+"def"+arbitraryDelimiter, "", arbitraryDelimiter, 5)
			require.NoError(t, err)

			require.Equal(t, []string{"abc" + arbitraryDelimiter + "def" + arbitraryDelimiter + "ghi"}, objectNames(result.Objects))
			require.Empty(t, result.Prefixes)
			require.Empty(t, result.NextMarker)
			require.False(t, result.IsTruncated)
		})

		t.Run("Prefix suffixed with partial delimiter", func(t *testing.T) {
			partialDelimiter := arbitraryDelimiter[:len(arbitraryDelimiter)-1]

			result, err := layer.ListObjects(ctxWithProject, bucketName, "abc"+partialDelimiter, "", arbitraryDelimiter, 5)
			require.NoError(t, err)

			require.Equal(t, []string{
				"abc" + arbitraryDelimiter,
				"abc" + arbitraryDelimiter + "def",
			}, objectNames(result.Objects))
			require.Equal(t, []string{"abc" + arbitraryDelimiter + "def" + arbitraryDelimiter}, result.Prefixes)
			require.Empty(t, result.NextMarker)
			require.False(t, result.IsTruncated)
		})
	})
}
