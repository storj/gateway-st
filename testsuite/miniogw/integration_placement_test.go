// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite"
	"storj.io/storj/satellite/console"
	"storj.io/storj/satellite/nodeselection"
)

func TestCreateBucketLocation(t *testing.T) {
	runIntegrationTest(t, testplanet.Config{
		SatelliteCount: 1, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Placement = nodeselection.ConfigurablePlacementRule{
					PlacementRules: `40:annotation("location","Poland")`,
				}
				config.Console.Placement.SelfServeEnabled = true
				config.Console.Placement.SelfServeDetails.SetMap(map[storj.PlacementConstraint]console.PlacementDetail{
					40: {ID: 40, IdName: "Poland"},
				})
			},
		},
	}, func(ctx *testcontext.Context, t *testing.T, planet *testplanet.Planet, gw *testGateway) {
		client := gw.client(t)
		sat := planet.Satellites[0]
		projectID := planet.Uplinks[0].Projects[0].ID

		createBucket := func(bucket, location string) error {
			input := &s3.CreateBucketInput{Bucket: new(bucket)}
			if location != "" {
				input.CreateBucketConfiguration = &types.CreateBucketConfiguration{
					LocationConstraint: types.BucketLocationConstraint(location),
				}
			}
			_, err := client.CreateBucket(ctx, input)
			return err
		}
		requirePlacement := func(bucket string, want storj.PlacementConstraint) {
			placement, err := sat.API.DB.Buckets().GetBucketPlacement(ctx, []byte(bucket), projectID)
			require.NoError(t, err)
			require.Equal(t, want, placement)
		}

		require.NoError(t, createBucket("placed", "Poland"))
		requirePlacement("placed", 40)

		err := createBucket("invalid", "EU")
		require.Equal(t, http.StatusBadRequest, statusCode(err), err)
		require.Equal(t, "InvalidLocationConstraint", errorCode(err))

		require.NoError(t, sat.API.DB.Console().Projects().UpdateDefaultPlacement(ctx, projectID, storj.PlacementConstraint(1)))

		err = createBucket("conflicting", "Poland")
		require.Equal(t, http.StatusBadRequest, statusCode(err), err)
		require.Equal(t, "InvalidLocationConstraint", errorCode(err))

		require.NoError(t, createBucket("default", ""))
		requirePlacement("default", 1)
	})
}
