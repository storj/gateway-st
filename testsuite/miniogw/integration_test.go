// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw_test

import (
	"context"
	"crypto/md5"
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
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/mux"
	minio "github.com/minio/minio-go/v7"
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
	"storj.io/gateway/internal/minioclient"
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
		client := gw.minioClient(t)
		var err error

		{ // normal upload
			bucket := "bucket"

			err = client.MakeBucket(ctx, bucket)
			require.NoError(t, err)

			// generate enough data for a remote segment
			data := testrand.BytesInt(5000)
			objectName := "testdata"

			rawClient, ok := client.(*minioclient.Minio)
			require.True(t, ok)

			expectedMetadata := map[string]string{
				"foo": "bar",
			}
			err = rawClient.Upload(ctx, bucket, objectName, data, expectedMetadata)
			require.NoError(t, err)

			object, err := rawClient.API.StatObject(ctx, bucket, objectName, minio.StatObjectOptions{})
			require.NoError(t, err)
			// TODO figure out why it returns "Foo:bar", instead "foo:bar"
			require.EqualValues(t, map[string]string{
				"Foo": "bar",
			}, object.UserMetadata)

			buffer := make([]byte, len(data))

			bytes, err := client.Download(ctx, bucket, objectName, buffer)
			require.NoError(t, err)

			require.Equal(t, data, bytes)
		}

		{ // multipart upload
			bucket := "bucket-multipart"

			err = client.MakeBucket(ctx, bucket)
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
			err = rawClient.UploadMultipart(ctx, bucket, objectName, data, partSize.Int(), 0, expectedMetadata)
			require.NoError(t, err)

			// TODO find out why with prefix set its hanging test
			for message := range rawClient.API.ListObjects(ctx, bucket, minio.ListObjectsOptions{WithMetadata: true}) {
				require.Equal(t, objectName, message.Key)
				require.NotEmpty(t, message.ETag)

				// Minio adds a double quote to ETag, sometimes.
				// Remove the potential quote from either end.
				etag := strings.TrimPrefix(message.ETag, `"`)
				etag = strings.TrimSuffix(etag, `"`)

				require.Equal(t, expectedETag, etag)
				break
			}

			object, err := rawClient.API.StatObject(ctx, bucket, objectName, minio.StatObjectOptions{})
			require.NoError(t, err)
			// TODO figure out why it returns "Foo:bar", instead "foo:bar"
			require.EqualValues(t, map[string]string{
				"Foo": "bar",
			}, object.UserMetadata)

			buffer := make([]byte, len(data))
			bytes, err := client.Download(ctx, bucket, objectName, buffer)
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

		client, err := minioclient.NewMinio(minioclient.Config{S3Gateway: address, AccessKey: accessKey, SecretKey: secretKey, NoSSL: true})
		require.NoError(t, err)

		bucket, objectName := "bucket", "testdata"
		require.NoError(t, client.MakeBucket(ctx, bucket))
		// generate enough data for a remote segment
		data := testrand.BytesInt(5000)
		require.NoError(t, client.Upload(ctx, bucket, objectName, data, nil))

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
	api.New(gw.layer, credentialsProvider{accessKey: secretKey}, api.Config{},
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
	client, err := minioclient.NewMinio(minioclient.Config{S3Gateway: address, AccessKey: accessKey, SecretKey: secretKey, NoSSL: true})
	require.NoError(t, err)

	gateway, err := startGateway(t, gw.ctx, client, gw.exe, serialized, address, accessKey, secretKey, flags...)
	require.NoError(t, err)
	gw.stops = append(gw.stops, func() error {
		processgroup.Kill(gateway)
		return nil
	})
	return address
}

// client returns an S3 client of a gateway for the uplink's access to the satellite.
func (gw *testGateway) client(t *testing.T) *s3.S3 {
	return gw.clientFor(t, gw.planet.Uplinks[0].Access[gw.planet.Satellites[0].ID()])
}

// clientFor returns an S3 client of a gateway for access.
func (gw *testGateway) clientFor(t *testing.T, access *uplink.Access) *s3.S3 {
	address, accessKey, secretKey := gw.serve(t, access)
	return createS3Client(t, address, accessKey, secretKey)
}

// minioClient returns a minio client of a gateway for the uplink's access to the satellite.
func (gw *testGateway) minioClient(t *testing.T) minioclient.Client {
	address, accessKey, secretKey := gw.serve(t, gw.planet.Uplinks[0].Access[gw.planet.Satellites[0].ID()])
	client, err := minioclient.NewMinio(minioclient.Config{S3Gateway: address, AccessKey: accessKey, SecretKey: secretKey, NoSSL: true})
	require.NoError(t, err)
	return client
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

func startGateway(t *testing.T, ctx *testcontext.Context, client minioclient.Client, exe, access, address, accessKey, secretKey string, moreFlags ...string) (*exec.Cmd, error) {
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
func waitToStart(ctx context.Context, client minioclient.Client, address string, maxStartupWait time.Duration) error {
	start := time.Now()
	for {
		_, err := client.ListBuckets(ctx)
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

func createBucket(ctx context.Context, client *s3.S3, bucket string, versioningEnabled, lockEnabled bool) error {
	_, err := client.CreateBucketWithContext(ctx, &s3.CreateBucketInput{
		Bucket:                     new(bucket),
		ObjectLockEnabledForBucket: new(lockEnabled),
	})
	if err != nil {
		return err
	}

	if versioningEnabled {
		_, err = client.PutBucketVersioning(&s3.PutBucketVersioningInput{
			Bucket: new(bucket),
			VersioningConfiguration: &s3.VersioningConfiguration{
				Status: new(s3.BucketVersioningStatusEnabled),
			},
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func putObject(ctx context.Context, client *s3.S3, bucket, key string, body io.ReadSeeker) (*s3.PutObjectOutput, error) {
	return client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: new(bucket),
		Key:    new(key),
		Body:   body,
	})
}

func getObject(ctx context.Context, client *s3.S3, bucket, key, versionID string) (*s3.GetObjectOutput, error) {
	input := s3.GetObjectInput{
		Bucket: new(bucket),
		Key:    new(key),
	}
	if versionID != "" {
		input.VersionId = new(versionID)
	}
	return client.GetObjectWithContext(ctx, &input)
}

func getObjectAttributes(ctx context.Context, client *s3.S3, bucket, key, versionID string, attributes []*string) (*s3.GetObjectAttributesOutput, error) {
	input := s3.GetObjectAttributesInput{
		Bucket:           new(bucket),
		Key:              new(key),
		ObjectAttributes: attributes,
	}
	if versionID != "" {
		input.VersionId = new(versionID)
	}
	return client.GetObjectAttributesWithContext(ctx, &input)
}

// copySource returns the x-amz-copy-source of a version, or of the latest version if versionID is
// empty. S3 refuses an empty versionId.
func copySource(bucket, key, versionID string) string {
	if versionID == "" {
		return bucket + "/" + key
	}
	return bucket + "/" + key + "?versionId=" + versionID
}

func copyObject(ctx context.Context, client *s3.S3, sourceBucket, sourceKey, sourceVersionID, destBucket, destKey string) (*s3.CopyObjectOutput, error) {
	return client.CopyObjectWithContext(ctx, &s3.CopyObjectInput{
		Bucket:     new(destBucket),
		Key:        new(destKey),
		CopySource: new(copySource(sourceBucket, sourceKey, sourceVersionID)),
	})
}

func deleteObject(ctx context.Context, client *s3.S3, bucket, key, versionID string) error {
	input := s3.DeleteObjectInput{
		Bucket: new(bucket),
		Key:    new(key),
	}
	if versionID != "" {
		input.VersionId = new(versionID)
	}
	_, err := client.DeleteObjectWithContext(ctx, &input)
	return err
}

func createS3Client(t *testing.T, gatewayAddr, accessKeyID, secretKey string) *s3.S3 {
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String("global"),
		Credentials:      credentials.NewStaticCredentials(accessKeyID, secretKey, ""),
		Endpoint:         aws.String("http://" + gatewayAddr),
		S3ForcePathStyle: aws.Bool(true),
	})
	require.NoError(t, err)

	return s3.New(sess)
}

func errorCode(err error) string {
	var awsErr awserr.Error
	if errors.As(err, &awsErr) {
		return awsErr.Code()
	}
	return ""
}

func statusCode(err error) int {
	var reqErr awserr.RequestFailure
	if errors.As(err, &reqErr) {
		return reqErr.StatusCode()
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
