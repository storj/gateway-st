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
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"

	"storj.io/gateway/api"
	"storj.io/gateway/miniogw"
	"storj.io/minio/cmd"
	objectlock "storj.io/minio/pkg/bucket/object/lock"
	"storj.io/minio/pkg/bucket/versioning"
	"storj.io/minio/pkg/event"
	"storj.io/minio/pkg/tags"
)

// bucketObjectLayer extends fakeObjectLayer with the bucket-level methods used by these tests.
type bucketObjectLayer struct {
	*fakeObjectLayer

	makeBucketWithLocation func(ctx context.Context, bucket string, opts cmd.BucketOptions) error
	setBucketTagging       func(ctx context.Context, bucket string, t *tags.Tags) error
	getBucketInfo          func(ctx context.Context, bucket string) (cmd.BucketInfo, error)
	listObjects            func(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (cmd.ListObjectsInfo, error)
	listObjectVersions     func(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (cmd.ListObjectVersionsInfo, error)
	listObjectsV2          func(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (cmd.ListObjectsV2Info, error)
}

func (f *bucketObjectLayer) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (cmd.ListObjectsInfo, error) {
	return f.listObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
}

func (f *bucketObjectLayer) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (cmd.ListObjectVersionsInfo, error) {
	return f.listObjectVersions(ctx, bucket, prefix, marker, versionMarker, delimiter, maxKeys)
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
			return cmd.ListObjectsV2Info{Objects: []cmd.ObjectInfo{{Name: "key"}}}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodGet, "/bucket?list-type=2", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Empty(t, gotToken)
	require.Contains(t, resp.Body, "<Key>key</Key>")
	require.NotContains(t, resp.Body, "<Owner>")

	resp = serve(t, objectAPI, http.MethodGet, "/bucket?list-type=2&fetch-owner=true", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Owner><ID>7b25a206cc747e61355f1af9395c2e1dc93664b7b64838ca859b245e20dead3c</ID><DisplayName>storj</DisplayName></Owner>")

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

func TestListMaxKeysClamped(t *testing.T) {
	var got int
	objectAPI := &bucketObjectLayer{
		fakeObjectLayer: &fakeObjectLayer{},
		listObjects: func(_ context.Context, _, _, _, _ string, maxKeys int) (cmd.ListObjectsInfo, error) {
			got = maxKeys
			return cmd.ListObjectsInfo{}, nil
		},
		listObjectsV2: func(_ context.Context, _, _, _, _ string, maxKeys int, _ bool, _ string) (cmd.ListObjectsV2Info, error) {
			got = maxKeys
			return cmd.ListObjectsV2Info{}, nil
		},
		listObjectVersions: func(_ context.Context, _, _, _, _, _ string, maxKeys int) (cmd.ListObjectVersionsInfo, error) {
			got = maxKeys
			return cmd.ListObjectVersionsInfo{}, nil
		},
	}

	for _, target := range []string{"/bucket?max-keys=5000", "/bucket?list-type=2&max-keys=5000", "/bucket?versions&max-keys=5000"} {
		got = 0
		resp := serve(t, objectAPI, http.MethodGet, target, nil, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Equal(t, 1000, got, target)
		require.Contains(t, resp.Body, "<MaxKeys>1000</MaxKeys>", target)
	}
}

func TestListObjectVersionsDeleteMarker(t *testing.T) {
	objectAPI := &bucketObjectLayer{
		fakeObjectLayer: &fakeObjectLayer{},
		listObjectVersions: func(context.Context, string, string, string, string, string, int) (cmd.ListObjectVersionsInfo, error) {
			return cmd.ListObjectVersionsInfo{Objects: []cmd.ObjectInfo{
				{Name: "key", VersionID: "v2", IsLatest: true, DeleteMarker: true, ModTime: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)},
				{Name: "key", VersionID: "v1", ETag: "etag", Size: 5},
			}}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodGet, "/bucket?versions", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">")
	require.Contains(t, resp.Body, "<DeleteMarker><Key>key</Key><VersionId>v2</VersionId><IsLatest>true</IsLatest><LastModified>2026-01-02T03:04:05.000Z</LastModified>"+
		"<Owner><ID>7b25a206cc747e61355f1af9395c2e1dc93664b7b64838ca859b245e20dead3c</ID><DisplayName>storj</DisplayName></Owner></DeleteMarker>")
	require.Contains(t, resp.Body, "<Version><Key>key</Key><VersionId>v1</VersionId><IsLatest>false</IsLatest><LastModified>0001-01-01T00:00:00.000Z</LastModified>"+
		"<ETag>&#34;etag&#34;</ETag><Size>5</Size><StorageClass>STANDARD</StorageClass>"+
		"<Owner><ID>7b25a206cc747e61355f1af9395c2e1dc93664b7b64838ca859b245e20dead3c</ID><DisplayName>storj</DisplayName></Owner></Version>")
}

func TestListMultipartUploads(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		listMultipartUploads: func(context.Context, string, string, string, string, string, int) (cmd.ListMultipartsInfo, error) {
			return cmd.ListMultipartsInfo{
				Uploads: []cmd.MultipartInfo{{Object: "key", UploadID: "upload-id"}},
			}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodGet, "/bucket?uploads", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Upload><Key>key</Key><UploadId>upload-id</UploadId>"+
		"<Initiator><ID>7b25a206cc747e61355f1af9395c2e1dc93664b7b64838ca859b245e20dead3c</ID><DisplayName>storj</DisplayName></Initiator>"+
		"<Owner><ID>7b25a206cc747e61355f1af9395c2e1dc93664b7b64838ca859b245e20dead3c</ID><DisplayName>storj</DisplayName></Owner>"+
		"<StorageClass>STANDARD</StorageClass>")

	resp = serve(t, objectAPI, http.MethodGet, "/bucket?uploads&encoding-type=bogus", nil, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "Invalid Encoding Method specified in Request")

}

func TestListPartsLimits(t *testing.T) {
	var gotMaxParts int
	parts := []cmd.PartInfo{{PartNumber: 1, ETag: "a", Size: 1}, {PartNumber: 2, ETag: "b", Size: 1}}
	objectAPI := &fakeObjectLayer{
		listObjectParts: func(_ context.Context, _, _, _ string, _, maxParts int, _ cmd.ObjectOptions) (cmd.ListPartsInfo, error) {
			gotMaxParts = maxParts
			// Like miniogw.ListObjectParts, at most maxParts parts are returned.
			n := min(maxParts, len(parts))
			return cmd.ListPartsInfo{MaxParts: maxParts, Parts: parts[:n], IsTruncated: n < len(parts)}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodGet, "/bucket/key?uploadId=upload-id&max-parts=2147483647", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, 1000, gotMaxParts)
	require.Contains(t, resp.Body, "<MaxParts>1000</MaxParts>")

	// max-parts=0 returns no parts, like S3.
	resp = serve(t, objectAPI, http.MethodGet, "/bucket/key?uploadId=upload-id&max-parts=0", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, 0, gotMaxParts)
	require.NotContains(t, resp.Body, "<Part>")
	require.Contains(t, resp.Body, "<MaxParts>0</MaxParts><IsTruncated>true</IsTruncated>")

	for _, query := range []string{"part-number-marker=10000", "part-number-marker=20000", "part-number-marker=2147483647"} {
		resp = serve(t, objectAPI, http.MethodGet, "/bucket/key?uploadId=upload-id&"+query, nil, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, query)
	}

	for _, query := range []string{"max-parts=x", "max-parts=-1", "max-parts=2147483648", "part-number-marker=-1", "part-number-marker=2147483648", "part-number-marker=4294967297"} {
		resp = serve(t, objectAPI, http.MethodGet, "/bucket/key?uploadId=upload-id&"+query, nil, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, query)
		require.Contains(t, resp.Body, "<Code>InvalidArgument</Code>", query)
	}
}

func TestListMultipartUploadsKeyMarkerOutsidePrefix(t *testing.T) {
	var gotKeyMarker string
	objectAPI := &fakeObjectLayer{
		listMultipartUploads: func(_ context.Context, _, prefix, keyMarker, _, _ string, maxUploads int) (cmd.ListMultipartsInfo, error) {
			gotKeyMarker = keyMarker
			return cmd.ListMultipartsInfo{
				Prefix:     prefix,
				MaxUploads: maxUploads,
				Uploads:    []cmd.MultipartInfo{{Object: "p/key", UploadID: "upload-id"}},
			}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodGet, "/bucket?uploads&prefix=p/&key-marker=a", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Empty(t, gotKeyMarker)
	require.Contains(t, resp.Body, "<UploadId>upload-id</UploadId>")
	// The requested marker is echoed, not the one passed to the object layer.
	require.Contains(t, resp.Body, "<KeyMarker>a</KeyMarker>")

	resp = serve(t, objectAPI, http.MethodGet, "/bucket?uploads&prefix=p/&key-marker=q", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	// The object layer is called anyway, so that bucket and permission errors are reported.
	require.Equal(t, []string{"ListMultipartUploads", "ListMultipartUploads"}, objectAPI.Calls())
	require.Contains(t, resp.Body, "<KeyMarker>q</KeyMarker>")
	require.Contains(t, resp.Body, "<Prefix>p/</Prefix><MaxUploads>1000</MaxUploads><IsTruncated>false</IsTruncated>")
	require.NotContains(t, resp.Body, "<Upload>")

	for _, query := range []string{"max-uploads=2147483648", "max-uploads=-1"} {
		resp = serve(t, objectAPI, http.MethodGet, "/bucket?uploads&"+query, nil, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, query)
	}
}

func TestListMultipartUploadsMaxUploadsClamped(t *testing.T) {
	var got int
	objectAPI := &fakeObjectLayer{
		listMultipartUploads: func(_ context.Context, _, _, _, _, _ string, maxUploads int) (cmd.ListMultipartsInfo, error) {
			got = maxUploads
			return cmd.ListMultipartsInfo{MaxUploads: maxUploads}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodGet, "/bucket?uploads&max-uploads=5000", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, 1000, got)
	require.Contains(t, resp.Body, "<MaxUploads>1000</MaxUploads>")
}

func TestListObjectVersionsVersionIDMarker(t *testing.T) {
	var got string
	objectAPI := &bucketObjectLayer{
		fakeObjectLayer: &fakeObjectLayer{},
		listObjectVersions: func(_ context.Context, bucket, _, marker, versionMarker, _ string, _ int) (cmd.ListObjectVersionsInfo, error) {
			got = versionMarker
			if marker == "" && versionMarker != "" {
				return cmd.ListObjectVersionsInfo{}, miniogw.ErrVersionIDMarkerWithoutKeyMarker(bucket)
			}
			return cmd.ListObjectVersionsInfo{}, nil
		},
	}

	for _, marker := range []string{"garbage", "00000000-0000-0000-0000-000000000001", "%2000000000000000000000000000000001"} {
		got = ""
		resp := serve(t, objectAPI, http.MethodGet, "/bucket?versions&key-marker=key&version-id-marker="+marker, nil, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, marker)
		require.Contains(t, resp.Body, "<Code>InvalidArgument</Code>", marker)
		require.Empty(t, got, marker)
	}

	resp := serve(t, objectAPI, http.MethodGet, "/bucket?versions&key-marker=key&version-id-marker=00000000000000000000000000000001", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, "00000000000000000000000000000001", got)

	// The object layer rejects a version-id-marker without a key-marker.
	resp = serve(t, objectAPI, http.MethodGet, "/bucket?versions&version-id-marker=00000000000000000000000000000001", nil, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>InvalidArgument</Code>")
}

func TestListBuckets(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		listBuckets: func(context.Context) ([]cmd.BucketInfo, error) {
			return []cmd.BucketInfo{{Name: "b2"}, {Name: "other"}, {Name: "b1"}, {Name: "b3"}}, nil
		},
	}
	list := func(query string) response {
		resp := serve(t, objectAPI, http.MethodGet, "/?"+query, nil, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		return resp
	}
	const date = "<CreationDate>0001-01-01T00:00:00.000Z</CreationDate>"

	resp := list("")
	require.Contains(t, resp.Body, "<Name>b1</Name>"+date+"</Bucket><Bucket><Name>b2</Name>"+date+"</Bucket><Bucket><Name>b3</Name>"+date+"</Bucket><Bucket><Name>other</Name>")
	require.NotContains(t, resp.Body, "<ContinuationToken>")
	require.NotContains(t, resp.Body, "<Prefix>")

	resp = list("prefix=b&max-buckets=2")
	require.Contains(t, resp.Body, "<Buckets><Bucket><Name>b1</Name>"+date+"</Bucket><Bucket><Name>b2</Name>"+date+"</Bucket></Buckets>")
	token := base64.StdEncoding.EncodeToString([]byte("b2"))
	require.Contains(t, resp.Body, "<ContinuationToken>"+token+"</ContinuationToken><Prefix>b</Prefix>")

	resp = list("prefix=b&max-buckets=2&continuation-token=" + url.QueryEscape(token))
	require.Contains(t, resp.Body, "<Buckets><Bucket><Name>b3</Name>"+date+"</Bucket></Buckets>")
	require.NotContains(t, resp.Body, "<ContinuationToken>")

	resp = list("bucket-region=us-east-1")
	require.Contains(t, resp.Body, "<Name>b1</Name>")

	resp = list("bucket-region=eu-west-1")
	require.NotContains(t, resp.Body, "<Bucket>")

	for _, query := range []string{"max-buckets=0", "max-buckets=10001", "max-buckets=x", "continuation-token=%21"} {
		resp := serve(t, objectAPI, http.MethodGet, "/?"+query, nil, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, query)
		require.Contains(t, resp.Body, "<Code>InvalidArgument</Code>", query)
	}
}

func TestHeadBucketRegionAndPolicyStatus(t *testing.T) {
	objectAPI := &bucketObjectLayer{
		fakeObjectLayer: &fakeObjectLayer{},
		getBucketInfo: func(_ context.Context, bucket string) (cmd.BucketInfo, error) {
			return cmd.BucketInfo{Name: bucket}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodHead, "/bucket", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "us-east-1", resp.Header.Get("X-Amz-Bucket-Region"))

	resp = serve(t, objectAPI, http.MethodGet, "/bucket?policyStatus", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<IsPublic>false</IsPublic>")
}

func TestCreateBucketVirtualHostedLocation(t *testing.T) {
	objectAPI := &bucketObjectLayer{
		fakeObjectLayer: &fakeObjectLayer{},
		makeBucketWithLocation: func(context.Context, string, cmd.BucketOptions) error {
			return nil
		},
	}
	router := mux.NewRouter()
	api.New(objectAPI, testCredentialsProvider{}, api.Config{Domains: []string{"example.com"}}).RegisterHandlers(router)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPut, "http://bucket.example.com/", nil)
	signV4(req, nil, time.Now())
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "/bucket", rec.Header().Get("Location"))
}

// configObjectLayer returns fixed bucket and object configurations.
type configObjectLayer struct {
	*fakeObjectLayer
}

func (configObjectLayer) GetBucketNotificationConfig(context.Context, string) (*event.Config, error) {
	return &event.Config{}, nil
}

func (configObjectLayer) GetObjectLockConfig(context.Context, string) (*objectlock.Config, error) {
	return &objectlock.Config{ObjectLockEnabled: "Enabled"}, nil
}

func (configObjectLayer) GetBucketTagging(context.Context, string) (*tags.Tags, error) {
	return tags.NewTags(map[string]string{"k": "v"}, false)
}

func (configObjectLayer) GetBucketVersioning(context.Context, string) (*versioning.Versioning, error) {
	return &versioning.Versioning{Status: versioning.Enabled}, nil
}

func (configObjectLayer) GetObjectLegalHold(context.Context, string, string, string) (*objectlock.ObjectLegalHold, error) {
	return &objectlock.ObjectLegalHold{Status: objectlock.LegalHoldOn}, nil
}

func (configObjectLayer) GetObjectTags(context.Context, string, string, cmd.ObjectOptions) (*tags.Tags, error) {
	return tags.NewTags(map[string]string{"k": "v"}, true)
}

func (configObjectLayer) GetObjectRetention(context.Context, string, string, string) (*objectlock.ObjectRetention, error) {
	return &objectlock.ObjectRetention{Mode: objectlock.RetGovernance}, nil
}

func TestConfigurationResponseNamespace(t *testing.T) {
	for _, tt := range []struct{ target, root string }{
		{"/bucket?notification", "NotificationConfiguration"},
		{"/bucket?object-lock", "ObjectLockConfiguration"},
		{"/bucket?tagging", "Tagging"},
		{"/bucket?versioning", "VersioningConfiguration"},
		{"/bucket/key?legal-hold", "LegalHold"},
		{"/bucket/key?tagging", "Tagging"},
		{"/bucket/key?retention", "Retention"},
	} {
		resp := serve(t, configObjectLayer{&fakeObjectLayer{}}, http.MethodGet, tt.target, nil, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.True(t, strings.HasPrefix(resp.Body, `<?xml version="1.0" encoding="UTF-8"?>`+"\n<"+tt.root+` xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`), resp.Body)
		if tt.root == "Tagging" {
			require.Contains(t, resp.Body, "<TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>")
		}
	}
}
