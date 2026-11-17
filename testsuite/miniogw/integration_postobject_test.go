// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"io"
	"mime/multipart"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
)

func TestPostObject(t *testing.T) {
	runIntegrationTest(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
	}, func(ctx *testcontext.Context, t *testing.T, planet *testplanet.Planet, gw *testGateway) {
		client := gw.client(t)
		presigner := s3.NewPresignClient(client)

		bucket := testrand.BucketName()
		require.NoError(t, createBucket(ctx, client, bucket, false, false))

		// presign returns a form for key, signed by a policy with conditions that expires after
		// expires. The SDK adds conditions for the signing fields, the bucket and, unless conditions
		// already restrict it, the key.
		presign := func(t *testing.T, key string, expires time.Duration, conditions ...any) *s3.PresignedPostRequest {
			req, err := presigner.PresignPostObject(ctx, &s3.PutObjectInput{Bucket: new(bucket), Key: new(key)}, func(o *s3.PresignPostOptions) {
				o.Expires = expires
				o.Conditions = conditions
			})
			require.NoError(t, err)
			return req
		}

		requireObject := func(t *testing.T, key string, data []byte) *s3.GetObjectOutput {
			got, err := getObject(ctx, client, bucket, key, "")
			require.NoError(t, err)
			body, err := io.ReadAll(got.Body)
			require.NoError(t, errs.Combine(err, got.Body.Close()))
			require.Equal(t, data, body)
			return got
		}

		requireNoObject := func(t *testing.T, key string) {
			_, err := getObject(ctx, client, bucket, key, "")
			requireS3Error(t, err, http.StatusNotFound, "NoSuchKey")
		}

		t.Run("upload", func(t *testing.T) {
			// Enough data for a remote segment.
			key, data := "upload", testrand.BytesInt(5000)
			fields := map[string]string{"Content-Type": "image/jpeg", "x-amz-meta-camera": "front-door"}
			form := presign(t, key, time.Hour, map[string]string{"Content-Type": "image/jpeg"}, map[string]string{"x-amz-meta-camera": "front-door"})

			resp := postObject(ctx, t, form, fields, "clip.jpg", data)
			require.Equal(t, http.StatusNoContent, resp.status)
			etag := quotedMD5(data)
			require.Equal(t, etag, resp.header.Get("ETag"))

			got := requireObject(t, key, data)
			require.Equal(t, "image/jpeg", *got.ContentType)
			require.Equal(t, map[string]string{"camera": "front-door"}, got.Metadata)
			require.Equal(t, etag, *got.ETag)
		})

		t.Run("success_action_status", func(t *testing.T) {
			key, data := "created", testrand.BytesInt(100)
			form := presign(t, key, time.Hour, map[string]string{"success_action_status": "201"})

			resp := postObject(ctx, t, form, map[string]string{"success_action_status": "201"}, "clip.jpg", data)
			require.Equal(t, http.StatusCreated, resp.status)

			var created struct {
				Bucket, Key, ETag string
			}
			require.NoError(t, xml.Unmarshal(resp.body, &created))
			require.Equal(t, bucket, created.Bucket)
			require.Equal(t, key, created.Key)
			require.Equal(t, quotedMD5(data), created.ETag)
			requireObject(t, key, data)
		})

		t.Run("filename", func(t *testing.T) {
			data := testrand.BytesInt(100)
			form := presign(t, "uploads/${filename}", time.Hour, []any{"starts-with", "$key", "uploads/"})

			resp := postObject(ctx, t, form, nil, "clip.jpg", data)
			require.Equal(t, http.StatusNoContent, resp.status)
			requireObject(t, "uploads/clip.jpg", data)
		})

		t.Run("content-length-range", func(t *testing.T) {
			form := presign(t, "sized", time.Hour, []any{"content-length-range", 10, 100})

			// The maximum is enforced while reading the file and the minimum after the upload, which
			// must then be aborted.
			resp := postObject(ctx, t, form, nil, "clip.jpg", testrand.BytesInt(101))
			requirePostError(t, resp, http.StatusBadRequest, "EntityTooLarge")
			requireNoObject(t, "sized")

			resp = postObject(ctx, t, form, nil, "clip.jpg", testrand.BytesInt(9))
			requirePostError(t, resp, http.StatusBadRequest, "EntityTooSmall")
			requireNoObject(t, "sized")

			data := testrand.BytesInt(100)
			resp = postObject(ctx, t, form, nil, "clip.jpg", data)
			require.Equal(t, http.StatusNoContent, resp.status)
			requireObject(t, "sized", data)
		})

		t.Run("signature mismatch", func(t *testing.T) {
			form := presign(t, "bad-signature", time.Hour)
			signature := []byte(form.Values["X-Amz-Signature"])
			require.NotEmpty(t, signature)
			signature[0] ^= 1
			form.Values["X-Amz-Signature"] = string(signature)

			resp := postObject(ctx, t, form, nil, "clip.jpg", testrand.BytesInt(100))
			requirePostError(t, resp, http.StatusForbidden, "SignatureDoesNotMatch")
			requireNoObject(t, "bad-signature")
		})

		t.Run("expired policy", func(t *testing.T) {
			form := presign(t, "expired", -time.Minute)

			resp := postObject(ctx, t, form, nil, "clip.jpg", testrand.BytesInt(100))
			requirePostError(t, resp, http.StatusForbidden, "AccessDenied")
			requireNoObject(t, "expired")
		})

		t.Run("key outside policy", func(t *testing.T) {
			form := presign(t, "signed-key", time.Hour)
			form.Values["key"] = "other-key"

			resp := postObject(ctx, t, form, nil, "clip.jpg", testrand.BytesInt(100))
			requirePostError(t, resp, http.StatusForbidden, "AccessDenied")
			requireNoObject(t, "other-key")
		})

		t.Run("field outside policy", func(t *testing.T) {
			form := presign(t, "extra-field", time.Hour)

			resp := postObject(ctx, t, form, map[string]string{"x-amz-meta-camera": "front-door"}, "clip.jpg", testrand.BytesInt(100))
			requirePostError(t, resp, http.StatusForbidden, "AccessDenied")
			requireNoObject(t, "extra-field")
		})
	})
}

// postObject sends a browser POST upload of file, named filename, with the presigned form's fields
// and fields.
func postObject(ctx context.Context, t *testing.T, form *s3.PresignedPostRequest, fields map[string]string, filename string, file []byte) postResponse {
	t.Helper()

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	for _, values := range []map[string]string{form.Values, fields} {
		for name, value := range values {
			require.NoError(t, mw.WriteField(name, value))
		}
	}
	// S3 ignores the fields after the file.
	fw, err := mw.CreateFormFile("file", filename)
	require.NoError(t, err)
	_, err = fw.Write(file)
	require.NoError(t, err)
	require.NoError(t, mw.Close())

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, form.URL, &body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, errs.Combine(err, resp.Body.Close()))
	return postResponse{status: resp.StatusCode, header: resp.Header, body: respBody}
}

// postResponse is a POST upload's response with its body read, so that no unclosed
// *http.Response escapes postObject.
type postResponse struct {
	status int
	header http.Header
	body   []byte
}

// requirePostError requires a POST upload's response to be the S3 error code with status.
func requirePostError(t *testing.T, resp postResponse, status int, code string) {
	t.Helper()

	require.Equal(t, status, resp.status, string(resp.body))
	var s3Err struct{ Code string }
	require.NoError(t, xml.Unmarshal(resp.body, &s3Err), string(resp.body))
	require.Equal(t, code, s3Err.Code)
}

// quotedMD5 returns the ETag of an object with data uploaded in one request.
func quotedMD5(data []byte) string {
	sum := md5.Sum(data)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}
