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
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/gateway/api/internal/fsobjects"
	"storj.io/gateway/api/objectlayer"
	"storj.io/minio/cmd"
)

// postObjectLayer is a fakeObjectLayer that stores PutObject calls.
type postObjectLayer struct {
	*fakeObjectLayer

	bucket, object string
	data           []byte
	metadata       map[string]string
}

func (l *postObjectLayer) PutObject(ctx context.Context, bucket, object string, data *cmd.PutObjReader, opts objectlayer.ObjectOptions) (objectlayer.ObjectInfo, error) {
	b, err := io.ReadAll(data)
	if err != nil {
		return objectlayer.ObjectInfo{}, err
	}
	l.bucket, l.object, l.data, l.metadata = bucket, object, b, opts.UserDefined
	return objectlayer.ObjectInfo{Bucket: bucket, Name: object, ETag: "etag"}, nil
}

// postObject sends a SigV4-signed browser POST upload. The signing fields are added to both the
// form and the policy conditions.
func postObject(t *testing.T, objectAPI objectlayer.ObjectLayer, target string, conditions []any, fields [][2]string, file string) response {
	t.Helper()
	return postObjectAt(t, objectAPI, target, conditions, fields, file, time.Now())
}

// postObjectAt is like postObject, but signs the upload as of now.
func postObjectAt(t *testing.T, objectAPI objectlayer.ObjectLayer, target string, conditions []any, fields [][2]string, file string, now time.Time) response {
	t.Helper()
	return postObjectWithHeader(t, objectAPI, target, conditions, fields, file, now, nil)
}

// postObjectWithHeader is like postObjectAt, but sends header with the request too.
func postObjectWithHeader(t *testing.T, objectAPI objectlayer.ObjectLayer, target string, conditions []any, fields [][2]string, file string, now time.Time, header http.Header) response {
	t.Helper()

	now = now.UTC()
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

	header = header.Clone()
	if header == nil {
		header = http.Header{}
	}
	header.Set("Content-Type", mw.FormDataContentType())
	return serve(t, objectAPI, http.MethodPost, target, header, body.Bytes())
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
	upload := func(objectAPI objectlayer.ObjectLayer, fields ...[2]string) response {
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
	for _, tt := range []struct {
		value  string
		status int
	}{
		{"AES256", http.StatusNoContent},
		{"AES128", http.StatusBadRequest},
		{"aws:kms", http.StatusNotImplemented},
	} {
		objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
		resp := postObject(t, objectAPI, "/bucket",
			[]any{
				map[string]string{"bucket": "bucket"},
				map[string]string{"key": "photo.jpg"},
				map[string]string{"x-amz-server-side-encryption": tt.value},
			},
			[][2]string{{"key", "photo.jpg"}, {"x-amz-server-side-encryption", tt.value}}, "data")
		require.Equal(t, tt.status, resp.StatusCode, "%s: %s", tt.value, resp.Body)
		require.Empty(t, resp.Header.Get("X-Amz-Server-Side-Encryption"), tt.value)
		if tt.status != http.StatusNoContent {
			require.Empty(t, objectAPI.object, tt.value)
		}
	}
}

func TestPostObjectInvalidKey(t *testing.T) {
	for _, tt := range []struct {
		key, code string
	}{
		{"", "InvalidRequest"},
		{"\xff\xfe", "InvalidURI"},
	} {
		objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
		resp := postObject(t, objectAPI, "/bucket",
			[]any{map[string]string{"bucket": "bucket"}, []string{"starts-with", "$key", ""}},
			[][2]string{{"key", tt.key}}, "data")
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, "<Code>"+tt.code+"</Code>")
		require.Empty(t, objectAPI.data)
	}
}

func TestPostObjectV2MissingSignature(t *testing.T) {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	require.NoError(t, mw.WriteField("key", "photo.jpg"))
	require.NoError(t, mw.WriteField("AWSAccessKeyId", testAccessKeyID))
	require.NoError(t, mw.WriteField("policy", base64.StdEncoding.EncodeToString([]byte(`{"conditions":[]}`))))
	fw, err := mw.CreateFormFile("file", "photo.jpg")
	require.NoError(t, err)
	_, err = fw.Write([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, mw.Close())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/bucket", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rec := httptest.NewRecorder()
	newTestRouter(&postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}).ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "<Code>InvalidArgument</Code>")
}

func TestPostObjectQueryLikeS3(t *testing.T) {
	for _, target := range []string{"/bucket?foo", "/bucket?foo=bar", "/bucket?versionId=x", "/bucket?x-id=PutObject"} {
		objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
		resp := postObject(t, objectAPI, target,
			[]any{map[string]string{"bucket": "bucket"}, map[string]string{"key": "k"}},
			[][2]string{{"key", "k"}}, "data")
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, target)
		require.Contains(t, resp.Body, "<Code>InvalidArgument</Code><Message>Query String Parameters not allowed on POST requests.</Message>", target)
		require.Nil(t, objectAPI.data, target)
	}
}

// TestPostObjectChecksums checks that, like S3, a POST upload stores the checksum of its
// x-amz-checksum-* field, or else a default one, and returns it with any success status.
func TestPostObjectChecksums(t *testing.T) {
	objectAPI := fsobjects.New(t.TempDir())
	require.Equal(t, http.StatusOK, serve(t, objectAPI, http.MethodPut, "/bucket", nil, nil).StatusCode)

	upload := func(key string, fields ...[2]string) response {
		conditions := []any{map[string]string{"bucket": "bucket"}, map[string]string{"key": key}}
		for _, field := range fields {
			conditions = append(conditions, map[string]string{field[0]: field[1]})
		}
		return postObject(t, objectAPI, "/bucket", conditions, append([][2]string{{"key", key}}, fields...), "hello world")
	}

	for _, tc := range []struct {
		name, key        string
		fields           [][2]string
		status           int
		header, checksum string
	}{
		{"crc32", "crc32", [][2]string{{"x-amz-checksum-crc32", "DUoRhQ=="}}, http.StatusNoContent, "X-Amz-Checksum-Crc32", "DUoRhQ=="},
		{"none", "none", nil, http.StatusNoContent, "X-Amz-Checksum-Crc64nvme", "jSnVw/bqjr4="},
		{"crc32 status 201", "crc32-201", [][2]string{{"x-amz-checksum-crc32", "DUoRhQ=="}, {"success_action_status", "201"}}, http.StatusCreated, "X-Amz-Checksum-Crc32", "DUoRhQ=="},
		{"none redirect", "none-redirect", [][2]string{{"success_action_redirect", "https://example.com/done"}}, http.StatusSeeOther, "X-Amz-Checksum-Crc64nvme", "jSnVw/bqjr4="},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := upload(tc.key, tc.fields...)
			require.Equal(t, tc.status, resp.StatusCode, resp.Body)
			require.Equal(t, tc.checksum, resp.Header.Get(tc.header))
			require.Equal(t, "FULL_OBJECT", resp.Header.Get("X-Amz-Checksum-Type"))
			// S3 doesn't add the checksum to the PostResponse document.
			require.NotContains(t, resp.Body, "Checksum")

			head := serve(t, objectAPI, http.MethodHead, "/bucket/"+tc.key, http.Header{"X-Amz-Checksum-Mode": {"ENABLED"}}, nil)
			require.Equal(t, http.StatusOK, head.StatusCode)
			require.Equal(t, tc.checksum, head.Header.Get(tc.header))
		})
	}
}

// TestPostObjectChecksumFields checks that, like S3, the checksum fields of a POST upload
// are checked after its policy, and that a Content-MD5 header is ignored.
func TestPostObjectChecksumFields(t *testing.T) {
	objectAPI := fsobjects.New(t.TempDir())
	require.Equal(t, http.StatusOK, serve(t, objectAPI, http.MethodPut, "/bucket", nil, nil).StatusCode)

	upload := func(fields [][2]string, conditions []map[string]string, header http.Header) response {
		policy := []any{map[string]string{"bucket": "bucket"}, map[string]string{"key": "key"}}
		for _, condition := range conditions {
			policy = append(policy, condition)
		}
		return postObjectWithHeader(t, objectAPI, "/bucket", policy, append([][2]string{{"key", "key"}}, fields...), "hello world", time.Now(), header)
	}

	for _, tc := range []struct {
		name       string
		fields     [][2]string
		conditions []map[string]string
		header     http.Header
		status     int
		message    string
	}{
		{
			name:       "malformed checksum",
			fields:     [][2]string{{"x-amz-checksum-crc32", "bad"}},
			conditions: []map[string]string{{"x-amz-checksum-crc32": "bad"}},
			status:     http.StatusBadRequest,
			message:    "Value for x-amz-checksum-crc32 header is invalid.",
		},
		{
			name:    "malformed checksum without condition",
			fields:  [][2]string{{"x-amz-checksum-crc32", "bad"}},
			status:  http.StatusForbidden,
			message: "Invalid according to Policy: Extra input fields: x-amz-checksum-crc32",
		},
		{
			name:       "malformed checksum with failing condition",
			fields:     [][2]string{{"x-amz-checksum-crc32", "bad"}, {"x-amz-meta-a", "b"}},
			conditions: []map[string]string{{"x-amz-checksum-crc32": "bad"}, {"x-amz-meta-a": "c"}},
			status:     http.StatusForbidden,
			message:    "Invalid according to Policy: Policy Condition failed",
		},
		{
			name:    "unknown algorithm without condition",
			fields:  [][2]string{{"x-amz-checksum-foo", "AAAA"}},
			status:  http.StatusForbidden,
			message: "Invalid according to Policy: Extra input fields: x-amz-checksum-foo",
		},
		{
			name:   "malformed Content-MD5",
			header: http.Header{"Content-Md5": {"bad"}},
			status: http.StatusNoContent,
		},
		{
			name:   "wrong Content-MD5",
			header: http.Header{"Content-Md5": {"XUFAKrxLKna5cZ2REBfFkg=="}},
			status: http.StatusNoContent,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := upload(tc.fields, tc.conditions, tc.header)
			require.Equal(t, tc.status, resp.StatusCode, resp.Body)
			require.Contains(t, resp.Body, tc.message)
		})
	}
}
