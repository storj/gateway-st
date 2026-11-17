// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
)

func TestObjectTTLHeader(t *testing.T) {
	runIntegrationTest(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		NonParallel: true,
	}, func(ctx *testcontext.Context, t *testing.T, planet *testplanet.Planet, gw *testGateway) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		client := gw.client(t)

		bucket := testrand.BucketName()
		require.NoError(t, createBucket(ctx, client, bucket, false, false))

		fixed := time.Unix(2147483647, 0).UTC()

		for _, header := range []string{
			"X-Amz-Meta-Object-Expires",
			"X-Minio-Meta-Object-Expires",
			"X-Amz-Meta-Storj-Expires",
			"X-Minio-Meta-Storj-Expires",
		} {
			for _, tt := range []struct {
				name, value  string
				expected     time.Time
				expectedCode string
			}{
				{name: "relative", value: "+2h", expected: time.Now().Add(2 * time.Hour)},
				{name: "absolute", value: fixed.Format(time.RFC3339), expected: fixed},
				{name: "none", value: "none"},
				{name: "invalid", value: "now", expectedCode: "InvalidTTL"},
			} {
				t.Run(header+"/"+tt.name, func(t *testing.T) {
					client := s3.New(client.Options(), withObjectCreationHeader(header, tt.value))

					single := "single/" + header + "/" + tt.name
					_, err := client.PutObject(ctx, &s3.PutObjectInput{
						Bucket: new(bucket),
						Key:    new(single),
						Body:   bytes.NewReader(testrand.Bytes(memory.KiB)),
					})
					multipart := "multipart/" + header + "/" + tt.name
					_, multipartErr := putObjectMultipart(ctx, client, &s3.CreateMultipartUploadInput{
						Bucket: new(bucket),
						Key:    new(multipart),
					}, testrand.Bytes(memory.KiB))

					if tt.expectedCode != "" {
						for _, err := range []error{err, multipartErr} {
							require.Error(t, err)
							assert.Equal(t, tt.expectedCode, errorCode(err))
							assert.Equal(t, http.StatusBadRequest, statusCode(err))
						}
						return
					}
					require.NoError(t, err)
					require.NoError(t, multipartErr)

					for _, key := range []string{single, multipart} {
						object, err := project.StatObject(ctx, bucket, key)
						require.NoError(t, err)
						assert.WithinDuration(t, tt.expected, object.System.Expires, time.Minute)
					}
				})
			}
		}
	})
}

// withObjectCreationHeader sets header to value on PutObject and CreateMultipartUpload requests.
// It sends the header as named, where the SDK's Metadata field always prefixes x-amz-meta-.
func withObjectCreationHeader(header, value string) func(*s3.Options) {
	return s3.WithAPIOptions(func(stack *middleware.Stack) error {
		return stack.Build.Add(middleware.BuildMiddlewareFunc("objectCreationHeader", func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (middleware.BuildOutput, middleware.Metadata, error) {
			switch awsmiddleware.GetOperationName(ctx) {
			case "PutObject", "CreateMultipartUpload":
				if req, ok := in.Request.(*smithyhttp.Request); ok {
					req.Header.Set(header, value)
				}
			}
			return next.HandleBuild(ctx, in)
		}), middleware.After)
	})
}
