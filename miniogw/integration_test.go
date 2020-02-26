// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/btcsuite/btcutil/base58"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"storj.io/common/memory"
	"storj.io/common/processgroup"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/gateway/internal/minioclient"
	"storj.io/storj/private/s3client"
	"storj.io/storj/private/testplanet"
)

func TestUploadDownload(t *testing.T) {
	var counter int64
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		NonParallel: true,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		log := zaptest.NewLogger(t)

		uplinkCfg := planet.Uplinks[0].GetConfig(planet.Satellites[0])
		oldAccess, err := uplinkCfg.GetAccess()
		require.NoError(t, err)

		// TODO fix this in storj/storj
		oldAccess.SatelliteAddr = planet.Satellites[0].URL().String()

		access, err := oldAccess.Serialize()
		require.NoError(t, err)

		index := atomic.AddInt64(&counter, 1)
		// TODO: make address not hardcoded the address selection here
		// may conflict with some automatically bound address.
		gatewayAddr := fmt.Sprintf("127.0.0.1:1100%d", index)

		gatewayAccessKey := base58.Encode(testrand.BytesInt(20))
		gatewaySecretKey := base58.Encode(testrand.BytesInt(20))

		gatewayExe := ctx.Compile("storj.io/gateway")
		gateway := exec.Command(gatewayExe,
			"run",
			"--config-dir", ctx.Dir("gateway"),
			"--access", access,
			"--server.address", gatewayAddr,
			"--minio.access-key", gatewayAccessKey,
			"--minio.secret-key", gatewaySecretKey,
		)
		processgroup.Setup(gateway)
		gateway.Stdout = logWriter{log.Named("gateway:stdout")}
		gateway.Stderr = logWriter{log.Named("gateway:stderr")}
		err = gateway.Start()
		require.NoError(t, err)
		defer func() { processgroup.Kill(gateway) }()

		err = waitForAddress(gatewayAddr, 5*time.Second)
		require.NoError(t, err)

		client, err := minioclient.NewMinio(s3client.Config{
			S3Gateway:     gatewayAddr,
			Satellite:     planet.Satellites[0].Addr(),
			AccessKey:     gatewayAccessKey,
			SecretKey:     gatewaySecretKey,
			APIKey:        uplinkCfg.Legacy.Client.APIKey,
			EncryptionKey: "fake-encryption-key",
			NoSSL:         true,
		})
		require.NoError(t, err)

		{ // normal upload
			bucket := "bucket"

			err = client.MakeBucket(bucket, "")
			require.NoError(t, err)

			// generate enough data for a remote segment
			data := testrand.BytesInt(5000)
			objectName := "testdata"

			err = client.Upload(bucket, objectName, data)
			require.NoError(t, err)

			buffer := make([]byte, len(data))

			bytes, err := client.Download(bucket, objectName, buffer)
			require.NoError(t, err)

			require.Equal(t, data, bytes)
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

			err = rawClient.UploadMultipart(bucket, objectName, data, partSize.Int(), 0)
			require.NoError(t, err)

			doneCh := make(chan struct{})
			defer close(doneCh)

			// TODO find out why with prefix set its hanging test
			for message := range rawClient.API.ListObjectsV2(bucket, "", true, doneCh) {
				require.Equal(t, objectName, message.Key)
				require.NotEmpty(t, message.ETag)

				// Minio adds a double quote to ETag, sometimes.
				// Remove the potential quote from either end.
				etag := strings.TrimPrefix(message.ETag, `"`)
				etag = strings.TrimSuffix(etag, `"`)

				require.Equal(t, expectedETag, etag)
				break
			}

			buffer := make([]byte, len(data))
			bytes, err := client.Download(bucket, objectName, buffer)
			require.NoError(t, err)

			require.Equal(t, data, bytes)
		}
	})
}

// waitForAddress will monitor starting when we are able to start the process.
func waitForAddress(address string, maxStartupWait time.Duration) error {
	start := time.Now()
	for {
		if tryConnect(address) {
			return nil
		}

		// wait a bit before retrying to reduce load
		time.Sleep(50 * time.Millisecond)

		if time.Since(start) > maxStartupWait {
			return fmt.Errorf("%s did not start in required time %v", address, maxStartupWait)
		}
	}
}

// tryConnect will try to connect to the process public address
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
