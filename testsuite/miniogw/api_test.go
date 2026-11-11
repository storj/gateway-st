// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/common/base58"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
)

func TestExperimentalAPI(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		gw := newTestGateway(t, ctx, planet, binaryGateway)
		defer gw.close(t)

		accessKey, secretKey := base58.Encode(testrand.BytesInt(20)), base58.Encode(testrand.BytesInt(20))
		address := gw.serveBinary(t, planet.Uplinks[0].Access[planet.Satellites[0].ID()], accessKey, secretKey, "--experimental-api")
		client := createS3Client(address, accessKey, secretKey)

		bucket, key, data := "bucket", "key", testrand.BytesInt(100)
		require.NoError(t, createBucket(ctx, client, bucket, false, false))
		_, err := putObject(ctx, client, bucket, key, bytes.NewReader(data))
		require.NoError(t, err)

		got, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: new(bucket), Key: new(key)})
		require.NoError(t, err)
		body, err := io.ReadAll(got.Body)
		require.NoError(t, errs.Combine(err, got.Body.Close()))
		require.Equal(t, data, body)

		// Only --website lets unsigned requests read.
		resp, err := httpGet(ctx, "http://"+address+"/"+bucket+"/"+key)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusForbidden, resp.StatusCode)

		websiteKey, websiteSecret := base58.Encode(testrand.BytesInt(20)), base58.Encode(testrand.BytesInt(20))
		website := gw.serveBinary(t, planet.Uplinks[0].Access[planet.Satellites[0].ID()], websiteKey, websiteSecret, "--experimental-api", "--website")
		resp, err = httpGet(ctx, "http://"+website+"/"+bucket+"/"+key)
		require.NoError(t, err)
		body, err = io.ReadAll(resp.Body)
		require.NoError(t, errs.Combine(err, resp.Body.Close()))
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, data, body)

		html := []byte("<html></html>")
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      new(bucket),
			Key:         new("index.html"),
			Body:        bytes.NewReader(html),
			ContentType: new("text/html"),
		})
		require.NoError(t, err)
		resp, err = httpGet(ctx, "http://"+website+"/"+bucket+"/index.html")
		require.NoError(t, err)
		body, err = io.ReadAll(resp.Body)
		require.NoError(t, errs.Combine(err, resp.Body.Close()))
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, "text/html", resp.Header.Get("Content-Type"))
		require.Equal(t, html, body)

		// Unsigned requests still can't choose the response's Content-Type, like in S3.
		resp, err = httpGet(ctx, "http://"+website+"/"+bucket+"/"+key+"?response-content-type=text%2Fhtml")
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)

		// Like minio's handlers, it answers CORS requests from any origin by default.
		preflight, err := http.NewRequestWithContext(ctx, http.MethodOptions, "http://"+website+"/"+bucket+"/"+key, nil)
		require.NoError(t, err)
		preflight.Header.Set("Origin", "https://example.com")
		preflight.Header.Set("Access-Control-Request-Method", http.MethodGet)
		resp, err = http.DefaultClient.Do(preflight)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Less(t, resp.StatusCode, 300)
		require.Equal(t, "https://example.com", resp.Header.Get("Access-Control-Allow-Origin"))

		get, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+website+"/"+bucket+"/"+key, nil)
		require.NoError(t, err)
		get.Header.Set("Origin", "https://example.com")
		resp, err = http.DefaultClient.Do(get)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, "https://example.com", resp.Header.Get("Access-Control-Allow-Origin"))

		// minio's handlers refuse DeleteObjects without Content-MD5, which aws-sdk-go-v2 replaces
		// with a checksum header, so this only succeeds when the api package serves it.
		_, err = client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: new(bucket),
			Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: new(key)}}},
		})
		require.NoError(t, err)
	})
}
