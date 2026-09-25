// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/stretchr/testify/require"

	"storj.io/gateway/api"
	"storj.io/minio/cmd"
)

// bucketObjectLayer extends fakeObjectLayer with the bucket-level methods used by these tests.
type bucketObjectLayer struct {
	*fakeObjectLayer

	makeBucketWithLocation func(ctx context.Context, bucket string, opts cmd.BucketOptions) error
	setBucketTagging       func(ctx context.Context, bucket string, t *tags.Tags) error
	getBucketInfo          func(ctx context.Context, bucket string) (cmd.BucketInfo, error)
	listObjectsV2          func(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (cmd.ListObjectsV2Info, error)
}

func (f *bucketObjectLayer) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (cmd.ListObjectsV2Info, error) {
	return f.listObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
}

func (f *bucketObjectLayer) GetBucketInfo(ctx context.Context, bucket string) (cmd.BucketInfo, error) {
	return f.getBucketInfo(ctx, bucket)
}

func (f *bucketObjectLayer) MakeBucketWithLocation(ctx context.Context, bucket string, opts cmd.BucketOptions) error {
	return f.makeBucketWithLocation(ctx, bucket, opts)
}

func TestCreateBucketInvalidObjectLockHeader(t *testing.T) {
	var called bool
	objectAPI := &bucketObjectLayer{
		fakeObjectLayer: &fakeObjectLayer{},
		makeBucketWithLocation: func(context.Context, string, cmd.BucketOptions) error {
			called = true
			return nil
		},
	}

	header := http.Header{"X-Amz-Bucket-Object-Lock-Enabled": {"bogus"}}
	resp := serve(t, objectAPI, http.MethodPut, "/bucket", header, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>InvalidRequest</Code>")
	require.False(t, called)
}

func (f *bucketObjectLayer) SetBucketTagging(ctx context.Context, bucket string, t *tags.Tags) error {
	return f.setBucketTagging(ctx, bucket, t)
}

func TestPutBucketTaggingUnknownContentLength(t *testing.T) {
	var got *tags.Tags
	objectAPI := &bucketObjectLayer{
		fakeObjectLayer: &fakeObjectLayer{},
		setBucketTagging: func(_ context.Context, _ string, t *tags.Tags) error {
			got = t
			return nil
		},
	}

	router := mux.NewRouter()
	api.New(objectAPI, testCredentialsProvider{}, api.Config{}).RegisterHandlers(router)

	body := []byte(`<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>`)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/bucket?tagging", bytes.NewReader(body))
	req.ContentLength = -1
	signV4(req, body, time.Now())

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NotNil(t, got)
	require.Equal(t, "k=v", got.String())
}

func TestListObjectsV2(t *testing.T) {
	var gotToken string
	objectAPI := &bucketObjectLayer{
		fakeObjectLayer: &fakeObjectLayer{},
		listObjectsV2: func(_ context.Context, _, _, continuationToken, _ string, _ int, _ bool, _ string) (cmd.ListObjectsV2Info, error) {
			gotToken = continuationToken
			return cmd.ListObjectsV2Info{}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodGet, "/bucket?list-type=2", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Empty(t, gotToken)

	resp = serve(t, objectAPI, http.MethodGet, "/bucket?list-type=2&continuation-token=dG9rZW4%3D", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, "token", gotToken)

	resp = serve(t, objectAPI, http.MethodGet, "/bucket?list-type=2&continuation-token=%21", nil, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
}

func TestBucketConfigurationErrors(t *testing.T) {
	for _, tt := range []struct {
		name, target, body, code string
	}{
		{"versioning status", "/bucket?versioning", `<VersioningConfiguration><Status>Bogus</Status></VersioningConfiguration>`, "IllegalVersioningConfigurationException"},
		{"versioning element", "/bucket?versioning", `<Bogus/>`, "MalformedXML"},
		{"versioning empty", "/bucket?versioning", ``, "MalformedXML"},
		{"object lock", "/bucket?object-lock", `<ObjectLockConfiguration><ObjectLockEnabled>Bogus</ObjectLockEnabled></ObjectLockConfiguration>`, "MalformedXML"},
		{"object lock period", "/bucket?object-lock", `<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>-1</Days></DefaultRetention></Rule></ObjectLockConfiguration>`, "InvalidArgument"},
		{"notification", "/bucket?notification", `<NotificationConfiguration><QueueConfiguration><Queue>arn:minio:sqs::1:webhook</Queue><Event>s3:Bogus</Event></QueueConfiguration></NotificationConfiguration>`, "InvalidArgument"},
		{"notification without event", "/bucket?notification", `<NotificationConfiguration><QueueConfiguration><Queue>arn:gcp:pubsub::project:topic</Queue></QueueConfiguration></NotificationConfiguration>`, "MalformedXML"},
		{"versioning encoding", "/bucket?versioning", `<?xml version="1.0" encoding="bogus"?><VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`, "MalformedXML"},
		{"object lock encoding", "/bucket?object-lock", `<?xml version="1.0" encoding="bogus"?><ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>`, "MalformedXML"},
		{"notification encoding", "/bucket?notification", `<?xml version="1.0" encoding="bogus"?><NotificationConfiguration></NotificationConfiguration>`, "MalformedXML"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resp := serve(t, &fakeObjectLayer{}, http.MethodPut, tt.target, nil, []byte(tt.body))
			require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
			require.Contains(t, resp.Body, "<Code>"+tt.code+"</Code>")
		})
	}
}

func TestBackendEOFIsInternalError(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		getObjectInfo: func(context.Context, string, string, cmd.ObjectOptions) (cmd.ObjectInfo, error) {
			return cmd.ObjectInfo{}, fmt.Errorf("metainfo: %w", io.EOF)
		},
	}

	// An io.EOF from the object layer isn't an XML parsing error.
	resp := serve(t, objectAPI, http.MethodHead, "/bucket/key", nil, nil)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestDeleteObjectsLimits(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		deleteObjects: func(_ context.Context, _ string, objects []cmd.ObjectToDelete, _ cmd.ObjectOptions) ([]cmd.DeletedObject, []cmd.DeleteObjectsError, error) {
			deleted := make([]cmd.DeletedObject, len(objects))
			for i, object := range objects {
				deleted[i] = cmd.DeletedObject{ObjectName: object.ObjectName}
			}
			return deleted, nil, nil
		},
	}

	deleteRequest := func(quiet bool, n int) response {
		var body strings.Builder
		fmt.Fprintf(&body, "<Delete><Quiet>%t</Quiet>", quiet)
		for i := range n {
			fmt.Fprintf(&body, "<Object><Key>key%d</Key></Object>", i)
		}
		body.WriteString("</Delete>")
		sum := md5.Sum([]byte(body.String()))
		header := http.Header{"Content-Md5": {base64.StdEncoding.EncodeToString(sum[:])}}
		return serve(t, objectAPI, http.MethodPost, "/bucket?delete", header, []byte(body.String()))
	}

	resp := deleteRequest(false, 1)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Deleted><Key>key0</Key>")

	resp = deleteRequest(true, 1)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.NotContains(t, resp.Body, "<Deleted>")

	resp = deleteRequest(false, 1000)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)

	resp = deleteRequest(false, 1001)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>MalformedXML</Code>")
}

func TestGetBucketAcl(t *testing.T) {
	objectAPI := &bucketObjectLayer{
		fakeObjectLayer: &fakeObjectLayer{},
		getBucketInfo: func(_ context.Context, bucket string) (cmd.BucketInfo, error) {
			return cmd.BucketInfo{Name: bucket}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodGet, "/bucket?acl", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, "application/xml", resp.Header.Get("Content-Type"))
	require.Equal(t, "Storj", resp.Header.Get("Server"))
	require.True(t, strings.HasPrefix(resp.Body, `<?xml version="1.0" encoding="UTF-8"?>`), resp.Body)
	require.Contains(t, resp.Body, `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	require.Contains(t, resp.Body, "<Owner><ID>7b25a206cc747e61355f1af9395c2e1dc93664b7b64838ca859b245e20dead3c</ID><DisplayName>storj</DisplayName></Owner>")
	require.Contains(t, resp.Body, "<ID>7b25a206cc747e61355f1af9395c2e1dc93664b7b64838ca859b245e20dead3c</ID><DisplayName>storj</DisplayName></Grantee>")
}
