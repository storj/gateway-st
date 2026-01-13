// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"storj.io/common/macaroon"
	"storj.io/common/testcontext"
	"storj.io/gateway/miniogw"
	minio "storj.io/minio/cmd"
	"storj.io/minio/pkg/auth"
	"storj.io/minio/pkg/event"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite"
	"storj.io/storj/satellite/console"
	"storj.io/storj/satellite/eventing/eventingconfig"
	"storj.io/storj/satellite/kms"
	"storj.io/uplink"
)

func TestBucketNotificationConfig(t *testing.T) {
	t.Parallel()

	runTestWithEventing(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Create the bucket using the Uplink API
		_, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		// Get notification config for bucket with no configuration - should return empty
		config, err := layer.GetBucketNotificationConfig(ctx, testBucket)
		require.NoError(t, err)
		assert.Empty(t, config.QueueList)
		assert.Empty(t, config.TopicList)
		assert.Empty(t, config.LambdaList)

		// Test setting a valid Google Pub/Sub topic configuration
		topic := event.Topic{}
		topic.ID = "test-config-1"
		topic.Events = []event.Name{event.ObjectCreatedAll}
		topic.Filter = event.S3Key{
			RuleList: event.FilterRuleList{
				Rules: []event.FilterRule{
					{Name: "prefix", Value: "uploads/"},
					{Name: "suffix", Value: ".jpg"},
				},
			},
		}

		topic.ARN = event.ARN{
			ServiceType: "sns",
			TargetID: event.TargetID{
				Name: "@log",
			},
		}

		config = &event.Config{
			TopicList: []event.Topic{topic},
		}

		err = layer.SetBucketNotificationConfig(ctx, testBucket, config)
		require.NoError(t, err)

		// Verify the configuration was set
		getConfig, err := layer.GetBucketNotificationConfig(ctx, testBucket)
		require.NoError(t, err)
		require.Len(t, getConfig.TopicList, 1)
		retrievedTopic := getConfig.TopicList[0]
		assert.Equal(t, "test-config-1", retrievedTopic.ID)
		assert.Equal(t, []event.Name{event.ObjectCreatedAll}, retrievedTopic.Events)
		require.Len(t, retrievedTopic.Filter.RuleList.Rules, 2)
		assert.Contains(t, retrievedTopic.Filter.RuleList.Rules, event.FilterRule{Name: "prefix", Value: "uploads/"})
		assert.Contains(t, retrievedTopic.Filter.RuleList.Rules, event.FilterRule{Name: "suffix", Value: ".jpg"})
		assert.Equal(t, "sns", retrievedTopic.ARN.ServiceType)
		assert.Equal(t, "@log", retrievedTopic.ARN.TargetID.Name)

		// Test deleting the configuration by setting nil
		err = layer.SetBucketNotificationConfig(ctx, testBucket, nil)
		require.NoError(t, err)

		// Verify the configuration was deleted
		getConfig, err = layer.GetBucketNotificationConfig(ctx, testBucket)
		require.NoError(t, err)
		assert.Empty(t, getConfig.TopicList)

		// Test setting empty configuration (should also delete)
		err = layer.SetBucketNotificationConfig(ctx, testBucket, &event.Config{})
		require.NoError(t, err)

		getConfig, err = layer.GetBucketNotificationConfig(ctx, testBucket)
		require.NoError(t, err)
		assert.Empty(t, getConfig.TopicList)
	})
}

func TestBucketNotificationConfig_ValidationErrors(t *testing.T) {
	t.Parallel()

	runTestWithEventing(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Check the error when getting notification config from a bucket with empty name
		_, err := layer.GetBucketNotificationConfig(ctx, "")
		assert.Equal(t, minio.BucketNameInvalid{}, err)

		// Check the error when getting notification config with a non-existent bucket
		_, err = layer.GetBucketNotificationConfig(ctx, testBucket)
		assert.Equal(t, minio.BucketNotFound{Bucket: testBucket}, err)

		// Check the error when setting notification config for a bucket with empty name
		err = layer.SetBucketNotificationConfig(ctx, "", &event.Config{})
		assert.Equal(t, minio.BucketNameInvalid{}, err)

		// Check the error when setting notification config with a non-existent bucket
		err = layer.SetBucketNotificationConfig(ctx, testBucket, &event.Config{})
		assert.Equal(t, minio.BucketNotFound{Bucket: testBucket}, err)

		// Create the bucket
		_, err = project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		// Test that Queue configurations are rejected
		config := &event.Config{
			QueueList: []event.Queue{
				{},
			},
		}
		err = layer.SetBucketNotificationConfig(ctx, testBucket, config)
		assert.Error(t, err)
		assert.Equal(t, minio.NotImplemented{
			Message: "SQS queues and Lambda functions are not supported. Only Google Pub/Sub topic destinations are allowed.",
		}, err)
	})
}

func TestBucketNotificationConfig_MultipleEvents(t *testing.T) {
	t.Parallel()

	runTestWithEventing(t, func(t *testing.T, ctx context.Context, layer minio.ObjectLayer, project *uplink.Project) {
		// Create the bucket
		_, err := project.CreateBucket(ctx, testBucket)
		require.NoError(t, err)

		// Test setting a configuration with multiple event types
		topic := event.Topic{}
		topic.ID = "multi-event-config"
		topic.Events = []event.Name{
			event.ObjectCreatedPut,
			event.ObjectCreatedCopy,
			event.ObjectRemovedDelete,
		}
		topic.Filter = event.S3Key{
			RuleList: event.FilterRuleList{
				Rules: []event.FilterRule{
					{Name: "prefix", Value: "data/"},
				},
			},
		}
		topic.ARN = event.ARN{
			ServiceType: "sns",
			TargetID: event.TargetID{
				Name: "@log",
			},
		}

		config := &event.Config{
			TopicList: []event.Topic{topic},
		}

		err = layer.SetBucketNotificationConfig(ctx, testBucket, config)
		require.NoError(t, err)

		// Verify the configuration
		getConfig, err := layer.GetBucketNotificationConfig(ctx, testBucket)
		require.NoError(t, err)
		require.Len(t, getConfig.TopicList, 1)
		retrievedTopic := getConfig.TopicList[0]
		assert.Equal(t, "multi-event-config", retrievedTopic.ID)
		require.Len(t, retrievedTopic.Events, 3)
		assert.Contains(t, retrievedTopic.Events, event.ObjectCreatedPut)
		assert.Contains(t, retrievedTopic.Events, event.ObjectCreatedCopy)
		assert.Contains(t, retrievedTopic.Events, event.ObjectRemovedDelete)
		require.Len(t, retrievedTopic.Filter.RuleList.Rules, 1)
		assert.Contains(t, retrievedTopic.Filter.RuleList.Rules, event.FilterRule{Name: "prefix", Value: "data/"})
		assert.Equal(t, "sns", retrievedTopic.ARN.ServiceType)
		assert.Equal(t, "@log", retrievedTopic.ARN.TargetID.Name)

	})
}

func runTestWithEventing(t *testing.T, test func(*testing.T, context.Context, minio.ObjectLayer, *uplink.Project)) {
	enabledProjects := eventingconfig.ProjectSet{}

	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
		NonParallel:      true, // we control parallelism ourselves
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.BucketEventing.Projects = enabledProjects
				config.Console.SatelliteManagedEncryptionEnabled = true
				config.KeyManagement.KeyInfos = kms.KeyInfos{
					Values: map[int]kms.KeyInfo{
						1: {
							SecretVersion: "secretversion1", SecretChecksum: 12345,
						},
					},
				}
			},
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.APIKeyVersion = macaroon.APIKeyVersionEventing
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]

		// Create a project with path encryption disabled for bucket eventing
		userCtx, err := sat.UserContext(ctx, planet.Uplinks[0].Projects[0].Owner.ID)
		require.NoError(t, err)

		proj, err := sat.API.Console.Service.CreateProject(userCtx, console.UpsertProjectInfo{
			Name:             "no-path-encryption",
			ManagePassphrase: true,
		})
		require.NoError(t, err)

		// Enable eventing for the test project
		enabledProjects[proj.ID] = struct{}{}

		// Create API key for the project without path encryption
		_, apiKey, err := sat.API.Console.Service.CreateAPIKey(userCtx, proj.ID, "test-key", macaroon.APIKeyVersionEventing)
		require.NoError(t, err)

		access, err := uplink.RequestAccessWithPassphrase(ctx, sat.URL(), apiKey.Serialize(), "")
		require.NoError(t, err)

		project, err := uplink.OpenProject(ctx, access)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		// Establish new context with *uplink.Project for the gateway to pick up.
		ctxWithProject := miniogw.WithCredentials(ctx, project, miniogw.CredentialsInfo{})

		layer, err := miniogw.NewStorjGateway(defaultS3CompatibilityConfig).NewGatewayLayer(auth.Credentials{})
		require.NoError(t, err)

		defer func() { require.NoError(t, layer.Shutdown(ctxWithProject)) }()

		test(t, ctxWithProject, layer, project)
	})
}
