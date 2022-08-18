// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v6"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"storj.io/common/base58"
	"storj.io/common/memory"
	"storj.io/common/processgroup"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/gateway/internal/minioclient"
	"storj.io/storj/private/testplanet"
)

func TestUploadDownload(t *testing.T) {
	var counter int64
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		NonParallel: true,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		oldAccess := planet.Uplinks[0].Access[planet.Satellites[0].ID()]

		access, err := oldAccess.Serialize()
		require.NoError(t, err)

		index := atomic.AddInt64(&counter, 1)
		// TODO: make address not hardcoded the address selection here
		// may conflict with some automatically bound address.
		gatewayAddr := fmt.Sprintf("127.0.0.1:1100%d", index)

		gatewayAccessKey := base58.Encode(testrand.BytesInt(20))
		gatewaySecretKey := base58.Encode(testrand.BytesInt(20))

		gatewayExe := ctx.CompileAt("../..", "storj.io/gateway")

		client, err := minioclient.NewMinio(minioclient.Config{
			S3Gateway:     gatewayAddr,
			Satellite:     planet.Satellites[0].Addr(),
			AccessKey:     gatewayAccessKey,
			SecretKey:     gatewaySecretKey,
			APIKey:        planet.Uplinks[0].APIKey[planet.Satellites[0].ID()].Serialize(),
			EncryptionKey: "fake-encryption-key",
			NoSSL:         true,
		})
		require.NoError(t, err)

		gateway, err := startGateway(t, ctx, client, gatewayExe, access, gatewayAddr, gatewayAccessKey, gatewaySecretKey)
		require.NoError(t, err)
		defer func() { processgroup.Kill(gateway) }()

		{ // normal upload
			bucket := "bucket"

			err = client.MakeBucket(bucket, "")
			require.NoError(t, err)

			// generate enough data for a remote segment
			data := testrand.BytesInt(5000)
			objectName := "testdata"

			rawClient, ok := client.(*minioclient.Minio)
			require.True(t, ok)

			expectedMetadata := map[string]string{
				"foo": "bar",
			}
			err = rawClient.Upload(bucket, objectName, data, expectedMetadata)
			require.NoError(t, err)

			object, err := rawClient.API.StatObjectWithContext(ctx, bucket, objectName, minio.StatObjectOptions{})
			require.NoError(t, err)
			// TODO figure out why it returns "Foo:bar", instead "foo:bar"
			require.EqualValues(t, map[string]string{
				"Foo": "bar",
			}, object.UserMetadata)

			buffer := make([]byte, len(data))

			bytes, err := client.Download(bucket, objectName, buffer)
			require.NoError(t, err)

			require.Equal(t, data, bytes)

			{ // try to access the content as static website - expect forbidden error
				response, err := httpGet(ctx, fmt.Sprintf("http://%s/%s", gatewayAddr, bucket))
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				require.NoError(t, response.Body.Close())

				response, err = httpGet(ctx, fmt.Sprintf("http://%s/%s/%s", gatewayAddr, bucket, objectName))
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				require.NoError(t, response.Body.Close())
			}

			{ // restart the gateway with the --website flag and try again - expect success
				err = stopGateway(gateway, gatewayAddr)
				require.NoError(t, err)
				gateway, err = startGateway(t, ctx, client, gatewayExe, access, gatewayAddr, gatewayAccessKey, gatewaySecretKey, "--website")
				require.NoError(t, err)

				response, err := httpGet(ctx, fmt.Sprintf("http://%s/%s", gatewayAddr, bucket))
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				require.NoError(t, response.Body.Close())

				response, err = httpGet(ctx, fmt.Sprintf("http://%s/%s/%s", gatewayAddr, bucket, objectName))
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, response.StatusCode)
				readData, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				require.Equal(t, data, readData)
				require.NoError(t, response.Body.Close())
			}
		}

		{ // multipart upload
			bucket := "bucket-multipart"

			err = client.MakeBucket(bucket, "")
			require.NoError(t, err)

			// minimum single part size is 5mib
			size := 8 * memory.MiB
			data := testrand.Bytes(size)
			objectName := "testdata"
			partSize := 5 * memory.MiB

			part1MD5 := md5.Sum(data[:partSize])
			part2MD5 := md5.Sum(data[partSize:])
			parts := append([]byte{}, part1MD5[:]...)
			parts = append(parts, part2MD5[:]...)
			partsMD5 := md5.Sum(parts)
			expectedETag := hex.EncodeToString(partsMD5[:]) + "-2"

			rawClient, ok := client.(*minioclient.Minio)
			require.True(t, ok)

			expectedMetadata := map[string]string{
				"foo": "bar",
			}
			err = rawClient.UploadMultipart(bucket, objectName, data, partSize.Int(), 0, expectedMetadata)
			require.NoError(t, err)

			doneCh := make(chan struct{})
			defer close(doneCh)

			// TODO find out why with prefix set its hanging test
			for message := range rawClient.API.ListObjectsV2WithMetadataWithContext(ctx, bucket, "", true, doneCh) {
				require.Equal(t, objectName, message.Key)
				require.NotEmpty(t, message.ETag)

				// Minio adds a double quote to ETag, sometimes.
				// Remove the potential quote from either end.
				etag := strings.TrimPrefix(message.ETag, `"`)
				etag = strings.TrimSuffix(etag, `"`)

				require.Equal(t, expectedETag, etag)
				// returned metadata is not fully processed so lets compare only single entry
				require.Equal(t, "bar", message.UserMetadata["X-Amz-Meta-Foo"])
				break
			}

			object, err := rawClient.API.StatObjectWithContext(ctx, bucket, objectName, minio.StatObjectOptions{})
			require.NoError(t, err)
			// TODO figure out why it returns "Foo:bar", instead "foo:bar"
			require.EqualValues(t, map[string]string{
				"Foo": "bar",
			}, object.UserMetadata)

			buffer := make([]byte, len(data))
			bytes, err := client.Download(bucket, objectName, buffer)
			require.NoError(t, err)

			require.Equal(t, data, bytes)
		}
		{
			uplink := planet.Uplinks[0]
			satellite := planet.Satellites[0]
			info, err := satellite.DB.Buckets().GetBucket(ctx, []byte("bucket"), uplink.Projects[0].ID)
			require.NoError(t, err)
			require.Contains(t, string(info.UserAgent), "Zenko")
		}
	})
}

func httpGet(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

func startGateway(t *testing.T, ctx *testcontext.Context, client minioclient.Client, exe, access, address, accessKey, secretKey string, moreFlags ...string) (*exec.Cmd, error) {
	args := append([]string{"run",
		"--config-dir", ctx.Dir("gateway"),
		"--access", access,
		"--server.address", address,
		"--minio.access-key", accessKey,
		"--minio.secret-key", secretKey,
		"--client.user-agent", "Zenko",
	}, moreFlags...)

	gateway := exec.Command(exe, args...)

	log := zaptest.NewLogger(t)
	gateway.Stdout = logWriter{log.Named("gateway:stdout")}
	gateway.Stderr = logWriter{log.Named("gateway:stderr")}

	err := gateway.Start()
	if err != nil {
		return nil, err
	}

	err = waitToStart(client, address, 5*time.Second)
	if err != nil {
		killErr := gateway.Process.Kill()
		return nil, errs.Combine(err, killErr)
	}

	return gateway, nil
}

func stopGateway(gateway *exec.Cmd, address string) error {
	err := gateway.Process.Kill()
	if err != nil {
		return err
	}

	start := time.Now()
	maxStopWait := 5 * time.Second
	for {
		if !tryConnect(address) {
			return nil
		}

		// wait a bit before retrying to reduce load
		time.Sleep(50 * time.Millisecond)

		if time.Since(start) > maxStopWait {
			return fmt.Errorf("%s did not stop in required time %v", address, maxStopWait)
		}
	}
}

// waitToStart will monitor starting when we are able to start the process.
func waitToStart(client minioclient.Client, address string, maxStartupWait time.Duration) error {
	start := time.Now()
	for {
		_, err := client.ListBuckets()
		if err == nil {
			return nil
		}

		// wait a bit before retrying to reduce load
		time.Sleep(50 * time.Millisecond)

		if time.Since(start) > maxStartupWait {
			return fmt.Errorf("%s did not start in required time %v", address, maxStartupWait)
		}
	}
}

// tryConnect will try to connect to the process public address.
func tryConnect(address string) bool {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return false
	}
	// write empty byte slice to trigger refresh on connection
	_, _ = conn.Write([]byte{})
	// ignoring errors, because we only care about being able to connect
	_ = conn.Close()
	return true
}

type logWriter struct{ log *zap.Logger }

func (log logWriter) Write(p []byte) (n int, err error) {
	log.log.Debug(string(p))
	return len(p), nil
}
