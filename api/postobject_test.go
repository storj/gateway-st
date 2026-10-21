// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/minio/cmd"
)

// postObjectLayer is a fakeObjectLayer that stores PutObject calls.
type postObjectLayer struct {
	*fakeObjectLayer

	bucket, object string
	data           []byte
	metadata       map[string]string
}

func (l *postObjectLayer) PutObject(ctx context.Context, bucket, object string, data *cmd.PutObjReader, opts cmd.ObjectOptions) (cmd.ObjectInfo, error) {
	b, err := io.ReadAll(data)
	if err != nil {
		return cmd.ObjectInfo{}, err
	}
	l.bucket, l.object, l.data, l.metadata = bucket, object, b, opts.UserDefined
	return cmd.ObjectInfo{Bucket: bucket, Name: object, ETag: "etag"}, nil
}

// postObject sends a SigV4-signed browser POST upload. The signing fields are added to both the
// form and the policy conditions.
func postObject(t *testing.T, objectAPI cmd.ObjectLayer, target string, conditions []any, fields [][2]string, file string) response {
	t.Helper()

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	credential := testAccessKeyID + "/" + amzDate[:8] + "/us-east-1/s3/aws4_request"
	fields = append(fields,
		[2]string{"x-amz-algorithm", "AWS4-HMAC-SHA256"},
		[2]string{"x-amz-credential", credential},
		[2]string{"x-amz-date", amzDate},
	)
	for _, f := range fields[len(fields)-3:] {
		conditions = append(conditions, map[string]string{f[0]: f[1]})
	}

	policyJSON, err := json.Marshal(map[string]any{
		"expiration": now.Add(time.Hour).Format(time.RFC3339Nano),
		"conditions": conditions,
	})
	require.NoError(t, err)
	policy := base64.StdEncoding.EncodeToString(policyJSON)

	key := []byte("AWS4" + testSecretAccessKey)
	for _, part := range []string{amzDate[:8], "us-east-1", "s3", "aws4_request"} {
		key = hmacSHA256(key, part)
	}
	fields = append(fields,
		[2]string{"policy", policy},
		[2]string{"x-amz-signature", hex.EncodeToString(hmacSHA256(key, policy))},
	)

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	for _, f := range fields {
		require.NoError(t, mw.WriteField(f[0], f[1]))
	}
	fw, err := mw.CreateFormFile("file", "photo.jpg")
	require.NoError(t, err)
	_, err = fw.Write([]byte(file))
	require.NoError(t, err)
	require.NoError(t, mw.Close())

	return serve(t, objectAPI, http.MethodPost, target, http.Header{"Content-Type": {mw.FormDataContentType()}}, body.Bytes())
}

func TestPostObjectRedirect(t *testing.T) {
	upload := func(redirect string) response {
		return postObject(t, &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}, "/bucket",
			[]any{
				map[string]string{"bucket": "bucket"},
				map[string]string{"key": "photo.jpg"},
				map[string]string{"success_action_redirect": redirect},
				map[string]string{"success_action_status": "201"},
			},
			[][2]string{
				{"bucket", "bucket"},
				{"key", "photo.jpg"},
				{"success_action_redirect", redirect},
				{"success_action_status", "201"},
			}, "data")
	}

	resp := upload("https://example.com/done?a=b")
	require.Equal(t, http.StatusSeeOther, resp.StatusCode, resp.Body)
	require.Equal(t, `https://example.com/done?a=b&bucket=bucket&etag=%22etag%22&key=photo.jpg`, resp.Header.Get("Location"))

	// An invalid redirect is ignored in favor of success_action_status.
	resp = upload("not-a-url")
	require.Equal(t, http.StatusCreated, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Key>photo.jpg</Key>")
	require.Contains(t, resp.Body, `<PostResponse xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
}

func TestPostObjectFilenameKey(t *testing.T) {
	objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
	resp := postObject(t, objectAPI, "/bucket",
		[]any{map[string]string{"bucket": "bucket"}, []string{"eq", "$key", "uploads/photo.jpg"}},
		[][2]string{{"key", "uploads/${filename}"}}, "data")
	require.Equal(t, http.StatusNoContent, resp.StatusCode, resp.Body)
	require.Equal(t, "uploads/photo.jpg", objectAPI.object)
}

func TestPostObjectContentLengthRange(t *testing.T) {
	upload := func(file string, ranges ...[]any) response {
		conditions := []any{map[string]string{"bucket": "bucket"}, map[string]string{"key": "photo.jpg"}}
		for _, r := range ranges {
			conditions = append(conditions, append([]any{"content-length-range"}, r...))
		}
		return postObject(t, &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}, "/bucket",
			conditions, [][2]string{{"key", "photo.jpg"}}, file)
	}

	resp := upload("data", []any{0, 4})
	require.Equal(t, http.StatusNoContent, resp.StatusCode, resp.Body)

	resp = upload("data!", []any{0, 4})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>EntityTooLarge</Code>")

	// Multiple ranges intersect; a later wider range doesn't override an earlier one.
	resp = upload(strings.Repeat("x", 30), []any{10, 20}, []any{0, 100})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>EntityTooLarge</Code>")

	// A maximum beyond the range of int64 doesn't limit the size.
	resp = upload("data", []any{0, 1e30})
	require.Equal(t, http.StatusNoContent, resp.StatusCode, resp.Body)
}

func TestPostObjectMetadata(t *testing.T) {
	upload := func(objectAPI cmd.ObjectLayer, fields ...[2]string) response {
		conditions := []any{map[string]string{"bucket": "bucket"}}
		for _, f := range fields {
			conditions = append(conditions, map[string]string{f[0]: f[1]})
		}
		return postObject(t, objectAPI, "/bucket", conditions, fields, "data")
	}

	objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
	resp := upload(objectAPI, [2]string{"key", "photo.jpg"}, [2]string{"Content-Encoding", "aws-chunked,gzip"})
	require.Equal(t, http.StatusNoContent, resp.StatusCode, resp.Body)
	require.Equal(t, "binary/octet-stream", objectAPI.metadata["Content-Type"])
	require.Equal(t, "gzip", objectAPI.metadata["Content-Encoding"])

	objectAPI = &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
	resp = upload(objectAPI, [2]string{"key", "photo.jpg"}, [2]string{"x-amz-storage-class", "ONEZONE_IA"})
	require.Equal(t, http.StatusNoContent, resp.StatusCode, resp.Body)
	require.NotContains(t, objectAPI.metadata, "X-Amz-Storage-Class")

	resp = upload(&postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}, [2]string{"key", "photo.jpg"}, [2]string{"x-amz-storage-class", "FOO"})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>InvalidStorageClass</Code>")
}

func TestPostObjectChecksum(t *testing.T) {
	// The CRC32 of "data".
	const crc32 = "rfPzYw=="

	upload := func(file string, conditions ...any) response {
		return postObject(t, &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}, "/bucket",
			append([]any{map[string]string{"bucket": "bucket"}, map[string]string{"key": "photo.jpg"}}, conditions...),
			[][2]string{{"key", "photo.jpg"}, {"x-amz-checksum-crc32", crc32}}, file)
	}

	resp := upload("data", map[string]string{"x-amz-checksum-crc32": crc32})
	require.Equal(t, http.StatusNoContent, resp.StatusCode, resp.Body)

	resp = upload("date", map[string]string{"x-amz-checksum-crc32": crc32})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>BadDigest</Code>")

	// Checksum fields need a policy condition like any other field.
	resp = upload("data")
	require.Equal(t, http.StatusForbidden, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "x-amz-checksum-crc32")
}

func TestPostObjectPolicyOnlyAuthenticatesPostObject(t *testing.T) {
	// The policy's expiration and conditions are checked only by PostObject, so a signed form
	// must not authenticate other POST operations.
	for _, target := range []string{"/bucket?delete", "/bucket/key?uploads", "/bucket/key?uploadId=id"} {
		resp := postObject(t, &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}, target,
			[]any{map[string]string{"bucket": "bucket"}}, nil,
			`<Delete><Object><Key>key</Key></Object></Delete>`)
		require.Equal(t, http.StatusForbidden, resp.StatusCode, target)
		require.Contains(t, resp.Body, "<Code>AccessDenied</Code>", target)
	}
}

func TestPostObjectEncryptionField(t *testing.T) {
	objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
	resp := postObject(t, objectAPI, "/bucket",
		[]any{
			map[string]string{"bucket": "bucket"},
			map[string]string{"key": "photo.jpg"},
			map[string]string{"x-amz-server-side-encryption": "aws:kms"},
		},
		[][2]string{{"key", "photo.jpg"}, {"x-amz-server-side-encryption", "aws:kms"}}, "data")
	require.Equal(t, http.StatusNotImplemented, resp.StatusCode, resp.Body)
	require.Empty(t, objectAPI.object)
}
