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
}

func (l *postObjectLayer) PutObject(ctx context.Context, bucket, object string, data *cmd.PutObjReader, opts cmd.ObjectOptions) (cmd.ObjectInfo, error) {
	b, err := io.ReadAll(data)
	if err != nil {
		return cmd.ObjectInfo{}, err
	}
	l.bucket, l.object, l.data = bucket, object, b
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
}

func TestPostObjectFilenameKey(t *testing.T) {
	objectAPI := &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
	resp := postObject(t, objectAPI, "/bucket",
		[]any{[]string{"eq", "$key", "uploads/photo.jpg"}},
		[][2]string{{"key", "uploads/${filename}"}}, "data")
	require.Equal(t, http.StatusNoContent, resp.StatusCode, resp.Body)
	require.Equal(t, "uploads/photo.jpg", objectAPI.object)
}
