// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/btcsuite/btcutil/base58"
	"github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/gateway/internal/minioclient"
	"storj.io/gateway/miniogw"
	"storj.io/storj/cmd/uplink/cmd"
	"storj.io/storj/private/s3client"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
)

type config struct {
	Server miniogw.ServerConfig
	Minio  miniogw.MinioConfig
}

func TestUploadDownload(t *testing.T) {
	var counter int64
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		NonParallel: true,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		var gwCfg config

		index := atomic.AddInt64(&counter, 1)

		// TODO: make address not hardcoded the address selection here
		// may conflict with some automatically bound address.
		gwCfg.Server.Address = fmt.Sprintf("127.0.0.1:1100%d", index)

		gwCfg.Minio.AccessKey = base58.Encode(testrand.BytesInt(20))
		gwCfg.Minio.SecretKey = base58.Encode(testrand.BytesInt(20))

		uplinkCfg := planet.Uplinks[0].GetConfig(planet.Satellites[0])

		go func() {
			// TODO: this leaks the gateway server, however it shouldn't
			gwCfg := gwCfg
			uplinkCfg := uplinkCfg
			err := runGateway(ctx, zaptest.NewLogger(t), gwCfg, uplinkCfg, planet.Satellites[0])
			if err != nil {
				t.Log(err)
			}
		}()

		time.Sleep(100 * time.Millisecond)

		client, err := minioclient.NewMinio(s3client.Config{
			S3Gateway:     gwCfg.Server.Address,
			Satellite:     planet.Satellites[0].Addr(),
			AccessKey:     gwCfg.Minio.AccessKey,
			SecretKey:     gwCfg.Minio.SecretKey,
			APIKey:        uplinkCfg.Legacy.Client.APIKey,
			EncryptionKey: "fake-encryption-key",
			NoSSL:         true,
		})
		assert.NoError(t, err)

		bucket := "bucket"

		err = client.MakeBucket(bucket, "")
		assert.NoError(t, err)

		// generate enough data for a remote segment
		data := testrand.BytesInt(5000)
		objectName := "testdata"

		err = client.Upload(bucket, objectName, data)
		assert.NoError(t, err)

		buffer := make([]byte, len(data))

		bytes, err := client.Download(bucket, objectName, buffer)
		assert.NoError(t, err)

		assert.Equal(t, string(data), string(bytes))
	})
}

// runGateway creates and starts a gateway
func runGateway(ctx context.Context, log *zap.Logger, gwCfg config, uplinkCfg cmd.Config, satellite *testplanet.SatelliteSystem) (err error) {
	// set gateway flags
	flags := flag.NewFlagSet("gateway", flag.ExitOnError)
	flags.String("address", gwCfg.Server.Address, "")
	flags.String("config-dir", gwCfg.Minio.Dir, "")
	flags.Bool("quiet", true, "")

	// create *cli.Context with gateway flags
	cliCtx := cli.NewContext(cli.NewApp(), flags, nil)

	// TODO: setting the flag on flagset and cliCtx seems redundant, but output is not quiet otherwise
	err = cliCtx.Set("quiet", "true")
	if err != nil {
		return err
	}

	err = os.Setenv("MINIO_ACCESS_KEY", gwCfg.Minio.AccessKey)
	if err != nil {
		return err
	}

	err = os.Setenv("MINIO_SECRET_KEY", gwCfg.Minio.SecretKey)
	if err != nil {
		return err
	}

	oldAccess, err := uplinkCfg.GetAccess()
	if err != nil {
		return err
	}

	// TODO fix this in storj/storj
	oldAccess.SatelliteAddr = satellite.URL().String()

	serializedAccess, err := oldAccess.Serialize()
	if err != nil {
		return err
	}

	access, err := uplink.ParseAccess(serializedAccess)
	if err != nil {
		return err
	}

	project, err := uplink.OpenProject(ctx, access)
	if err != nil {
		return err
	}

	gw := miniogw.NewStorjGateway(project)

	minio.StartGateway(cliCtx, miniogw.Logging(gw, log))
	return errors.New("unexpected minio exit")
}
