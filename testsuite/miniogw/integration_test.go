// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/amwolff/awsig"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"storj.io/common/base58"
	"storj.io/common/grant"
	"storj.io/common/macaroon"
	"storj.io/common/memory"
	"storj.io/common/processgroup"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/gateway/api"
	"storj.io/gateway/api/objectlayer"
	"storj.io/gateway/miniogw"
	miniocmd "storj.io/minio/cmd"
	"storj.io/minio/pkg/auth"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
)

func TestUploadDownload(t *testing.T) {
	runIntegrationTest(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		NonParallel: true,
	}, func(ctx *testcontext.Context, t *testing.T, planet *testplanet.Planet, gw *testGateway) {
		client := gw.client(t)
		var err error

		{ // normal upload
			bucket := "bucket"

			require.NoError(t, createBucket(ctx, client, bucket, false, false))

			// generate enough data for a remote segment
			data := testrand.BytesInt(5000)
			objectName := "testdata"

			expectedMetadata := map[string]string{
				"foo": "bar",
			}
			_, err = client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:   new(bucket),
				Key:      new(objectName),
				Body:     bytes.NewReader(data),
				Metadata: expectedMetadata,
			})
			require.NoError(t, err)

			object, err := headObject(ctx, client, bucket, objectName, "")
			require.NoError(t, err)
			// TODO figure out why it returns "Foo:bar", instead "foo:bar"
			require.EqualValues(t, map[string]string{
				"Foo": "bar",
			}, userMetadata(object.ResultMetadata))

			bytes, err := getObjectData(ctx, client, bucket, objectName, "")
			require.NoError(t, err)

			require.Equal(t, data, bytes)
		}

		{ // multipart upload
			bucket := "bucket-multipart"

			require.NoError(t, createBucket(ctx, client, bucket, false, false))

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

			expectedMetadata := map[string]string{
				"foo": "bar",
			}
			_, err = putObjectMultipart(ctx, client, &s3.CreateMultipartUploadInput{
				Bucket:   new(bucket),
				Key:      new(objectName),
				Metadata: expectedMetadata,
			}, data[:partSize], data[partSize:])
			require.NoError(t, err)

			listing, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket:    new(bucket),
				Delimiter: new("/"),
			})
			require.NoError(t, err)
			for _, message := range listing.Contents {
				require.Equal(t, objectName, aws.ToString(message.Key))
				require.NotEmpty(t, aws.ToString(message.ETag))

				// The ETag is quoted.
				// Remove the potential quote from either end.
				etag := strings.TrimPrefix(aws.ToString(message.ETag), `"`)
				etag = strings.TrimSuffix(etag, `"`)

				require.Equal(t, expectedETag, etag)
				break
			}

			object, err := headObject(ctx, client, bucket, objectName, "")
			require.NoError(t, err)
			// TODO figure out why it returns "Foo:bar", instead "foo:bar"
			require.EqualValues(t, map[string]string{
				"Foo": "bar",
			}, userMetadata(object.ResultMetadata))

			bytes, err := getObjectData(ctx, client, bucket, objectName, "")
			require.NoError(t, err)

			require.Equal(t, data, bytes)
		}
	})
}

// TestWebsite tests the gateway binary's static website hosting, and that it sets its user agent on
// the buckets it creates.
func TestWebsite(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		NonParallel: true,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		gw := newTestGateway(t, ctx, planet, binaryGateway)
		defer gw.close(t)

		access := planet.Uplinks[0].Access[planet.Satellites[0].ID()]
		address, accessKey, secretKey := gw.serve(t, access)
		websiteAddress := gw.serveBinary(t, access, accessKey, secretKey, "--website")

		client := createS3Client(address, accessKey, secretKey)

		bucket, objectName := "bucket", "testdata"
		require.NoError(t, createBucket(ctx, client, bucket, false, false))
		// generate enough data for a remote segment
		data := testrand.BytesInt(5000)
		_, err := putObject(ctx, client, bucket, objectName, bytes.NewReader(data))
		require.NoError(t, err)

		// Without --website, the content can't be accessed as a static website.
		for _, path := range []string{bucket, bucket + "/" + objectName} {
			response, err := httpGet(ctx, fmt.Sprintf("http://%s/%s", address, path))
			require.NoError(t, err)
			require.Equal(t, http.StatusForbidden, response.StatusCode)
			require.NoError(t, response.Body.Close())
		}

		response, err := httpGet(ctx, fmt.Sprintf("http://%s/%s", websiteAddress, bucket))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.NoError(t, response.Body.Close())

		response, err = httpGet(ctx, fmt.Sprintf("http://%s/%s/%s", websiteAddress, bucket, objectName))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode)
		readData, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		require.Equal(t, data, readData)
		require.NoError(t, response.Body.Close())

		info, err := planet.Satellites[0].DB.Buckets().GetBucket(ctx, []byte(bucket), planet.Uplinks[0].Projects[0].ID)
		require.NoError(t, err)
		require.Contains(t, string(info.UserAgent), "Zenko")
	})
}

// gatewayKind is an S3 gateway implementation that the integration tests run against.
type gatewayKind string

const (
	// binaryGateway is the gateway binary: minio's handlers over miniogw.
	binaryGateway gatewayKind = "binary"
	// apiGateway is the api package over miniogw.
	apiGateway gatewayKind = "api"
)

// testGateway serves a gatewayKind for a testplanet satellite. Some of the tests that use it are
// ported from storj/edge's testsuite/server. Unlike there, they don't use authservice: each client
// gets its own gateway for the access grant it uses.
type testGateway struct {
	kind   gatewayKind
	ctx    *testcontext.Context
	planet *testplanet.Planet
	exe    string               // binaryGateway
	layer  miniocmd.ObjectLayer // apiGateway

	stops []func() error
}

// runIntegrationTest runs test against each gatewayKind, with a planet configured by planetConfig.
func runIntegrationTest(t *testing.T, planetConfig testplanet.Config, test func(ctx *testcontext.Context, t *testing.T, planet *testplanet.Planet, gw *testGateway)) {
	for _, kind := range []gatewayKind{binaryGateway, apiGateway} {
		t.Run(string(kind), func(t *testing.T) {
			testplanet.Run(t, planetConfig, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
				gw := newTestGateway(t, ctx, planet, kind)
				defer gw.close(t)

				test(ctx, t, planet, gw)
			})
		})
	}
}

// newTestGateway returns a testGateway of kind for planet. Its close must be called when the test
// returns rather than in a cleanup, because testplanet waits for the gateways' goroutines before the
// test's cleanups would run.
func newTestGateway(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, kind gatewayKind) *testGateway {
	gw := &testGateway{kind: kind, ctx: ctx, planet: planet}
	switch kind {
	case binaryGateway:
		gw.exe = ctx.CompileAt("../..", "storj.io/gateway")
	case apiGateway:
		// The gateway binary's defaults, so that both kinds of gateway behave alike.
		layer, err := miniogw.NewStorjGateway(miniogw.S3CompatibilityConfig{
			MaxKeysLimit:             1000,
			MaxKeysExhaustiveLimit:   100000,
			MaxUploadsLimit:          1000,
			MinPartSize:              5 << 20,
			DeleteObjectsConcurrency: 100,
		}).NewGatewayLayer(auth.Credentials{})
		require.NoError(t, err)
		gw.layer = layer
	}
	return gw
}

// close stops the gateways.
func (gw *testGateway) close(t *testing.T) {
	for _, stop := range gw.stops {
		require.NoError(t, stop())
	}
	if gw.layer != nil {
		require.NoError(t, gw.layer.Shutdown(gw.ctx))
	}
}

// serve starts a gateway for access and returns its address with the credentials that sign for it.
func (gw *testGateway) serve(t *testing.T, access *uplink.Access) (address, accessKey, secretKey string) {
	accessKey, secretKey = base58.Encode(testrand.BytesInt(20)), base58.Encode(testrand.BytesInt(20))
	if gw.kind == binaryGateway {
		return gw.serveBinary(t, access, accessKey, secretKey), accessKey, secretKey
	}

	project, err := gw.planet.Uplinks[0].Config.OpenProject(gw.ctx, access)
	require.NoError(t, err)

	router := mux.NewRouter()
	// minio's ObjectLayer, which miniogw's layer is returned as, doesn't declare GetBucketLocation.
	api.New(gw.layer.(objectlayer.ObjectLayer), credentialsProvider{accessKey: secretKey}, api.Config{},
		api.WithLogger(api.NewZapLogger(gw.planet.Log().Named("api")))).RegisterHandlers(router)
	// miniogw finds the project in the request's context, like edge's middleware puts it.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		router.ServeHTTP(w, r.WithContext(miniogw.WithCredentials(r.Context(), project, miniogw.CredentialsInfo{Access: access})))
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := &http.Server{Handler: handler}
	gw.ctx.Go(func() error {
		if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	gw.stops = append(gw.stops, func() error {
		return errors.Join(server.Shutdown(context.Background()), project.Close())
	})
	return listener.Addr().String(), accessKey, secretKey
}

// serveBinary starts the gateway binary with flags for access and returns its address.
func (gw *testGateway) serveBinary(t *testing.T, access *uplink.Access, accessKey, secretKey string, flags ...string) string {
	serialized, err := access.Serialize()
	require.NoError(t, err)

	address := freeAddress(t)
	client := createS3Client(address, accessKey, secretKey)

	gateway, err := startGateway(t, gw.ctx, client, gw.exe, serialized, address, accessKey, secretKey, flags...)
	require.NoError(t, err)
	gw.stops = append(gw.stops, func() error {
		processgroup.Kill(gateway)
		return nil
	})
	return address
}

// client returns an S3 client of a gateway for the uplink's access to the satellite.
func (gw *testGateway) client(t *testing.T) *s3.Client {
	return gw.clientFor(t, gw.planet.Uplinks[0].Access[gw.planet.Satellites[0].ID()])
}

// clientFor returns an S3 client of a gateway for access.
func (gw *testGateway) clientFor(t *testing.T, access *uplink.Access) *s3.Client {
	address, accessKey, secretKey := gw.serve(t, access)
	if gw.kind == binaryGateway {
		return createS3Client(address, accessKey, secretKey, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
				return stack.Build.Add(middleware.BuildMiddlewareFunc("binaryCompatibility", binaryCompatibility), middleware.After)
			})
		})
	}
	return createS3Client(address, accessKey, secretKey)
}

// binaryCompatibility works around the gateway binary's minio handlers not supporting what
// aws-sdk-go-v2 sends, so that the tests still cover the rest of the binary. The handlers of
// DeleteObjects, PutObjectRetention and PutObjectLegalHold require Content-MD5, where the SDK sends
// an x-amz-checksum-* header instead, and GetObjectAttributes reads only the first of the
// x-amz-object-attributes lines that the SDK sends.
func binaryCompatibility(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (middleware.BuildOutput, middleware.Metadata, error) {
	req, ok := in.Request.(*smithyhttp.Request)
	if !ok {
		return next.HandleBuild(ctx, in)
	}
	switch awsmiddleware.GetOperationName(ctx) {
	case "DeleteObjects", "PutObjectRetention", "PutObjectLegalHold":
		body, err := io.ReadAll(req.GetStream())
		if err != nil {
			return middleware.BuildOutput{}, middleware.Metadata{}, err
		}
		sum := md5.Sum(body)
		req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(sum[:]))
		if in.Request, err = req.SetStream(bytes.NewReader(body)); err != nil {
			return middleware.BuildOutput{}, middleware.Metadata{}, err
		}
	case "GetObjectAttributes":
		req.Header.Set("X-Amz-Object-Attributes", strings.Join(req.Header.Values("X-Amz-Object-Attributes"), ","))
	}
	return next.HandleBuild(ctx, in)
}

// userMetadata returns the user metadata of a response by the names of its x-amz-meta-* headers,
// which net/http canonicalizes, without the prefix. aws-sdk-go-v2 lowercases the names instead.
func userMetadata(metadata middleware.Metadata) map[string]string {
	resp, ok := awsmiddleware.GetRawResponse(metadata).(*smithyhttp.Response)
	if !ok {
		return nil
	}
	const prefix = "X-Amz-Meta-"
	userMetadata := map[string]string{}
	for name, values := range resp.Header {
		if strings.HasPrefix(name, prefix) {
			userMetadata[name[len(prefix):]] = values[0]
		}
	}
	return userMetadata
}

// freeAddress returns a local address that was free when it was checked.
func freeAddress(t *testing.T) string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}

// credentialsProvider maps access key IDs to secret access keys.
type credentialsProvider map[string]string

func (c credentialsProvider) Provide(_ context.Context, accessKeyID string) (string, api.AuthData, error) {
	secret, ok := c[accessKeyID]
	if !ok {
		return "", api.AuthData{}, awsig.ErrInvalidAccessKeyID
	}
	return secret, api.AuthData{AccessKeyID: accessKeyID}, nil
}

// grantAccess returns an access grant for apiKey with encAccess.
func grantAccess(t *testing.T, satelliteAddr string, apiKey *macaroon.APIKey, encAccess *grant.EncryptionAccess) *uplink.Access {
	serialized, err := (&grant.Access{SatelliteAddress: satelliteAddr, APIKey: apiKey, EncAccess: encAccess}).Serialize()
	require.NoError(t, err)
	access, err := uplink.ParseAccess(serialized)
	require.NoError(t, err)
	return access
}

func httpGet(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

func startGateway(t *testing.T, ctx *testcontext.Context, client *s3.Client, exe, access, address, accessKey, secretKey string, moreFlags ...string) (*exec.Cmd, error) {
	args := append([]string{"run",
		"--config-dir", ctx.Dir("gateway", accessKey),
		"--access", access,
		"--server.address", address,
		"--minio.access-key", accessKey,
		"--minio.secret-key", secretKey,
		"--client.user-agent", "Zenko",
	}, moreFlags...)

	gateway := exec.Command(exe, args...)
	processgroup.Setup(gateway)

	log := zaptest.NewLogger(t)
	gateway.Stdout = logWriter{log.Named("gateway:stdout")}
	gateway.Stderr = logWriter{log.Named("gateway:stderr")}

	err := gateway.Start()
	if err != nil {
		return nil, err
	}

	err = waitToStart(ctx, client, address, 5*time.Second)
	if err != nil {
		killErr := gateway.Process.Kill()
		return nil, errs.Combine(err, killErr)
	}

	return gateway, nil
}

// waitToStart will monitor starting when we are able to start the process.
func waitToStart(ctx context.Context, client *s3.Client, address string, maxStartupWait time.Duration) error {
	start := time.Now()
	for {
		_, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
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

type logWriter struct{ log *zap.Logger }

func (log logWriter) Write(p []byte) (n int, err error) {
	log.log.Debug(string(p))
	return len(p), nil
}

func createBucket(ctx context.Context, client *s3.Client, bucket string, versioningEnabled, lockEnabled bool) error {
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket:                     new(bucket),
		ObjectLockEnabledForBucket: new(lockEnabled),
	})
	if err != nil {
		return err
	}

	if versioningEnabled {
		return putBucketVersioning(ctx, client, bucket, types.BucketVersioningStatusEnabled)
	}

	return nil
}

// putBucketVersioning sets the versioning status of a bucket.
func putBucketVersioning(ctx context.Context, client *s3.Client, bucket string, status types.BucketVersioningStatus) error {
	_, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  new(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: status},
	})
	return err
}

func putObject(ctx context.Context, client *s3.Client, bucket, key string, body io.ReadSeeker) (*s3.PutObjectOutput, error) {
	return client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: new(bucket),
		Key:    new(key),
		Body:   body,
	})
}

func getObject(ctx context.Context, client *s3.Client, bucket, key, versionID string) (*s3.GetObjectOutput, error) {
	input := s3.GetObjectInput{
		Bucket: new(bucket),
		Key:    new(key),
	}
	if versionID != "" {
		input.VersionId = new(versionID)
	}
	return client.GetObject(ctx, &input)
}

func headObject(ctx context.Context, client *s3.Client, bucket, key, versionID string) (*s3.HeadObjectOutput, error) {
	input := s3.HeadObjectInput{
		Bucket: new(bucket),
		Key:    new(key),
	}
	if versionID != "" {
		input.VersionId = new(versionID)
	}
	return client.HeadObject(ctx, &input)
}

// getObjectData returns the data of a version, or of the latest version if versionID is empty.
func getObjectData(ctx context.Context, client *s3.Client, bucket, key, versionID string) (_ []byte, err error) {
	resp, err := getObject(ctx, client, bucket, key, versionID)
	if err != nil {
		return nil, err
	}
	defer func() { err = errs.Combine(err, resp.Body.Close()) }()
	return io.ReadAll(resp.Body)
}

// putObjectMultipart uploads an object in a multipart upload created with input, with a part for
// each of parts.
func putObjectMultipart(ctx context.Context, client *s3.Client, input *s3.CreateMultipartUploadInput, parts ...[]byte) (*s3.CompleteMultipartUploadOutput, error) {
	upload, err := client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return nil, err
	}

	var completed []types.CompletedPart
	for i, part := range parts {
		partNumber := int32(i + 1)
		resp, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     input.Bucket,
			Key:        input.Key,
			UploadId:   upload.UploadId,
			PartNumber: new(partNumber),
			Body:       bytes.NewReader(part),
		})
		if err != nil {
			return nil, err
		}
		completed = append(completed, types.CompletedPart{ETag: resp.ETag, PartNumber: new(partNumber)})
	}

	return client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          input.Bucket,
		Key:             input.Key,
		UploadId:        upload.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	})
}

func getObjectAttributes(ctx context.Context, client *s3.Client, bucket, key, versionID string, attributes []types.ObjectAttributes) (*s3.GetObjectAttributesOutput, error) {
	input := s3.GetObjectAttributesInput{
		Bucket:           new(bucket),
		Key:              new(key),
		ObjectAttributes: attributes,
	}
	if versionID != "" {
		input.VersionId = new(versionID)
	}
	return client.GetObjectAttributes(ctx, &input)
}

// copySource returns the x-amz-copy-source of a version, or of the latest version if versionID is
// empty. S3 refuses an empty versionId.
func copySource(bucket, key, versionID string) string {
	if versionID == "" {
		return bucket + "/" + key
	}
	return bucket + "/" + key + "?versionId=" + versionID
}

func copyObject(ctx context.Context, client *s3.Client, sourceBucket, sourceKey, sourceVersionID, destBucket, destKey string) (*s3.CopyObjectOutput, error) {
	return client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     new(destBucket),
		Key:        new(destKey),
		CopySource: new(copySource(sourceBucket, sourceKey, sourceVersionID)),
	})
}

func deleteObject(ctx context.Context, client *s3.Client, bucket, key, versionID string) error {
	input := s3.DeleteObjectInput{
		Bucket: new(bucket),
		Key:    new(key),
	}
	if versionID != "" {
		input.VersionId = new(versionID)
	}
	_, err := client.DeleteObject(ctx, &input)
	return err
}

// createS3Client returns a client of a gateway at gatewayAddr with aws-sdk-go-v2's default settings,
// so it sends what current SDKs send, such as flexible checksums instead of Content-MD5.
func createS3Client(gatewayAddr, accessKeyID, secretKey string, optFns ...func(*s3.Options)) *s3.Client {
	return s3.New(s3.Options{
		Region:       "global",
		Credentials:  credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, ""),
		BaseEndpoint: new("http://" + gatewayAddr),
		UsePathStyle: true,
	}, optFns...)
}

func errorCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}
	return ""
}

func statusCode(err error) int {
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) {
		return respErr.HTTPStatusCode()
	}
	return 0
}

func requireS3Error(t *testing.T, err error, status int, code string) {
	require.Error(t, err)
	require.Equal(t, status, statusCode(err))
	require.Equal(t, code, errorCode(err))
}

func randomVersionID() string {
	version := testrand.UUID()
	binary.BigEndian.PutUint64(version[:8], uint64(rand.Uint32()))
	return hex.EncodeToString(version.Bytes())
}
