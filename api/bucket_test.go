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

// TestCreateBucketConfigurationLikeS3 checks the responses that S3 returned for the same requests.
func TestCreateBucketConfigurationLikeS3(t *testing.T) {
	var called bool
	objectAPI := &bucketObjectLayer{
		fakeObjectLayer: &fakeObjectLayer{},
		makeBucketWithLocation: func(context.Context, string, cmd.BucketOptions) error {
			called = true
			return nil
		},
	}
	config := func(inner string) []byte {
		return []byte("<CreateBucketConfiguration>" + inner + "</CreateBucketConfiguration>")
	}

	for _, body := range [][]byte{
		config("<Foo/>"),
		config("<LocationConstraint>us-west-2</LocationConstraint><LocationConstraint>us-west-2</LocationConstraint>"),
		config("<Location><Name>use1-az4</Name><Type>AvailabilityZone</Type></Location>"),
		config("<Bucket><DataRedundancy>SingleAvailabilityZone</DataRedundancy><Type>Directory</Type></Bucket>"),
		[]byte("<BucketConfig><Foo/></BucketConfig>"),
		config("<Tags><Foo/></Tags>"),
		config("<Tags><Tag><Key>a</Key><Value>1</Value><Foo/></Tag></Tags>"),
		config("<Tags><Tag><Key>a</Key></Tag></Tags>"),
		config("<Tags><Tag><Value>1</Value></Tag></Tags>"),
		config("<Tags><Tag/></Tags>"),
		config("<Tags><Tag><Key>a</Key><Key>b</Key><Value>1</Value></Tag></Tags>"),
		config("<Tags><Tag><Key>a</Key><Value>1</Value><Value>2</Value></Tag></Tags>"),
		config("<Tags><Tag><Key>aws:x</Key><Value>1</Value></Tag></Tags><Tags/>"),
		// Text in the elements that only hold elements, but not whitespace.
		[]byte("<CreateBucketConfiguration>hi<Tags/></CreateBucketConfiguration>"),
		config("<![CDATA[hi]]>"),
		config("&amp;"),
		config("\u00a0"),
		config("<Tags>hi<Tag><Key>a</Key><Value>1</Value></Tag></Tags>"),
		config("<Tags><Tag>hi<Key>a</Key><Value>1</Value></Tag></Tags>"),
	} {
		called = false
		resp := serve(t, objectAPI, http.MethodPut, "/bucket", nil, body)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, string(body))
		require.Contains(t, resp.Body, "<Code>MalformedXML</Code>", string(body))
		require.False(t, called, string(body))
	}

	for _, body := range [][]byte{
		[]byte("<BucketConfig></BucketConfig>"),
		config(""),
		config("<Tags><Tag><Key>a</Key><Value>1</Value></Tag></Tags>"),
		config("<Tags/>"),
		config("<Tags><Tag><Key>a</Key><Value></Value></Tag></Tags>"),
		config("<Tags><Tag><Key>" + strings.Repeat("k", 128) + "</Key><Value>" + strings.Repeat("v", 256) + "</Value></Tag></Tags>"),
		config("\n\t <!-- c --><Tags> <Tag>\r\n<Key>a</Key><Value>1</Value> </Tag><![CDATA[ ]]></Tags>\n"),
	} {
		called = false
		resp := serve(t, objectAPI, http.MethodPut, "/bucket", nil, body)
		require.Equal(t, http.StatusOK, resp.StatusCode, string(body)+": "+resp.Body)
		require.True(t, called, string(body))
	}

	// S3 reports the tags of CreateBucket with its own messages for the aws: prefix, and checks every
	// key and value before the prefixes and the number of tags.
	tag := func(key, value string) string {
		return "<Tag><Key>" + key + "</Key><Value>" + value + "</Value></Tag>"
	}
	const (
		invalidKey   = "<Code>InvalidTag</Code><Message>The TagKey you have provided is invalid</Message>"
		invalidValue = "<Code>InvalidTag</Code><Message>The TagValue you have provided is invalid</Message>"
		awsPrefix    = "<Code>InvalidTag</Code><Message>User-defined tag keys can&#39;t start with &#34;aws:&#34;. This prefix is reserved for system tags. Remove &#34;aws:&#34; from your tag keys and try again.</Message>"
		awsAnyCase   = "<Code>InvalidTag</Code><Message>Invalid Tag. Tags cannot start with &#34;aws:&#34;(case insensitive) unless they are System Tags that start with &#34;aws:&#34;(case sensitive)</Message>"
		tooMany      = "<Code>BadRequest</Code><Message>Bucket tag count cannot be greater than 50</Message>"
	)
	var fiftyOne strings.Builder
	for i := range 50 {
		fiftyOne.WriteString(tag(fmt.Sprint("k", i), "v"))
	}
	for _, tt := range []struct{ name, tags, want string }{
		{"aws prefix", tag("aws:x", "1"), awsPrefix},
		{"aws prefix only", tag("aws:", "1"), awsPrefix},
		{"upper case prefix", tag("AWS:x", "1"), awsAnyCase},
		{"mixed case prefix", tag("Aws:x", "1"), awsAnyCase},
		{"empty key", tag("", "1"), invalidKey},
		{"invalid key", tag("a?", "1"), invalidKey},
		{"invalid value", tag("a", "*"), invalidValue},
		{"key of 129 characters", tag(strings.Repeat("k", 129), "1"), invalidKey},
		{"value of 257 characters", tag("a", strings.Repeat("v", 257)), invalidValue},
		{"invalid value before prefix", tag("aws:x", "*"), invalidValue},
		{"prefix, then invalid key", tag("aws:x", "1") + tag("a?", "1"), invalidKey},
		{"prefix, then invalid value", tag("aws:x", "1") + tag("a", "*"), invalidValue},
		{"upper case prefix, then invalid key", tag("AWS:x", "1") + tag("a?", "1"), awsAnyCase},
		{"empty key, then prefix", tag("", "1") + tag("aws:x", "1"), invalidKey},
		{"51 tags", fiftyOne.String() + tag("k50", "v"), tooMany},
		{"51 tags, the last invalid", fiftyOne.String() + tag("a?", "v"), invalidKey},
		{"51 tags, the first with a prefix", tag("aws:x", "v") + fiftyOne.String(), awsPrefix},
		{"51 tags, the first with an upper case prefix", tag("AWS:x", "v") + fiftyOne.String(), awsAnyCase},
	} {
		called = false
		resp := serve(t, objectAPI, http.MethodPut, "/bucket", nil, config("<Tags>"+tt.tags+"</Tags>"))
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, tt.name+": "+resp.Body)
		require.Contains(t, resp.Body, tt.want, tt.name)
		require.False(t, called, tt.name)
	}
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
	req.Header.Set("Content-Md5", base64MD5(body))
	signV4(req, body, time.Now())

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
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
	require.Contains(t, resp.Body, "<ContinuationToken>dG9rZW4=</ContinuationToken>")

	for _, token := range []string{"%21", ""} {
		resp = serve(t, objectAPI, http.MethodGet, "/bucket?list-type=2&continuation-token="+token, nil, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, "<Code>InvalidArgument</Code>")
	}
}

// putTagsLayer records the tags set on a bucket or an object.
type putTagsLayer struct {
	*fakeObjectLayer
	got *tags.Tags
}

func (l *putTagsLayer) SetBucketTagging(_ context.Context, _ string, t *tags.Tags) error {
	l.got = t
	return nil
}

func (l *putTagsLayer) PutObjectTags(_ context.Context, bucket, object, t string, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
	var err error
	l.got, err = tags.ParseObjectTags(t)
	return cmd.ObjectInfo{Bucket: bucket, Name: object}, err
}

// TestPutTaggingLikeS3 checks the responses that S3 returned for the same requests.
func TestPutTaggingLikeS3(t *testing.T) {
	objectAPI := &putTagsLayer{fakeObjectLayer: &fakeObjectLayer{}}
	put := func(target, body string) response {
		return serve(t, objectAPI, http.MethodPut, target, http.Header{"Content-Md5": {base64MD5([]byte(body))}}, []byte(body))
	}

	for _, target := range []string{"/bucket?tagging", "/bucket/key?tagging"} {
		for _, body := range []string{
			`<Tagging><TagSet><Tag><Key>a</Key></Tag></TagSet></Tagging>`,
			`<Tagging><TagSet><Tag><Value>1</Value></Tag></TagSet></Tagging>`,
			`<Tagging><TagSet><Tag><Key>a</Key><Key>b</Key><Value>1</Value></Tag></TagSet></Tagging>`,
			`<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag></TagSet><TagSet></TagSet></Tagging>`,
			`<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag></TagSet><Foo/></Tagging>`,
			`<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value><Foo/></Tag></TagSet></Tagging>`,
			// Text in the elements that only hold elements, but not whitespace.
			`<Tagging>hi<TagSet><Tag><Key>a</Key><Value>1</Value></Tag></TagSet></Tagging>`,
			`<Tagging><TagSet>hi<Tag><Key>a</Key><Value>1</Value></Tag></TagSet></Tagging>`,
			`<Tagging><TagSet><Tag>hi<Key>a</Key><Value>1</Value></Tag></TagSet></Tagging>`,
			`<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag>hi</TagSet></Tagging>`,
			`<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag></TagSet>hi</Tagging>`,
			`<Tagging><![CDATA[hi]]><TagSet></TagSet></Tagging>`,
			`<Tagging>&amp;<TagSet></TagSet></Tagging>`,
			"<Tagging>\u00a0<TagSet></TagSet></Tagging>",
		} {
			resp := put(target, body)
			require.Equal(t, http.StatusBadRequest, resp.StatusCode, body)
			require.Contains(t, resp.Body, "<Code>MalformedXML</Code>", body)
		}

		want := http.StatusOK
		if target == "/bucket?tagging" {
			want = http.StatusNoContent
		}
		// Any root element and declared encoding is accepted.
		for body, tagString := range map[string]string{
			`<Tags><TagSet><Tag><Key>a</Key><Value>1</Value></Tag></TagSet></Tags>`:                                            "a=1",
			`<?xml version="1.0" encoding="bogus"?><Tagging><TagSet><Tag><Key>b</Key><Value></Value></Tag></TagSet></Tagging>`: "b=",
			`<Tagging><TagSet></TagSet></Tagging>`:                                                                             "",
			`<Tagging></Tagging>`:                                                                                              "",
			"<Tagging>\n <!-- c --><TagSet>\t<Tag>\r\n<Key>a</Key><Value>1</Value> </Tag><![CDATA[ ]]></TagSet>\n</Tagging>":   "a=1",
		} {
			objectAPI.got = nil
			resp := put(target, body)
			require.Equal(t, want, resp.StatusCode, body+": "+resp.Body)
			require.NotNil(t, objectAPI.got, body)
			require.Equal(t, tagString, objectAPI.got.String(), body)
		}
	}

	// Unlike PutObjectTagging, PutBucketTagging requires Content-MD5 or a checksum.
	body := []byte(`<Tagging><TagSet></TagSet></Tagging>`)
	resp := serve(t, objectAPI, http.MethodPut, "/bucket?tagging", nil, body)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>InvalidRequest</Code>")
	resp = serve(t, objectAPI, http.MethodPut, "/bucket/key?tagging", nil, body)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
}

// lockConfigObjectLayer records the object lock configuration set on a bucket.
type lockConfigObjectLayer struct {
	*fakeObjectLayer
	got *objectlock.Config
}

func (l *lockConfigObjectLayer) SetObjectLockConfig(_ context.Context, _ string, config *objectlock.Config) error {
	l.got = config
	return nil
}

// TestPutObjectLockConfigurationLikeS3 checks the responses that S3 returned for the same requests.
func TestPutObjectLockConfigurationLikeS3(t *testing.T) {
	objectAPI := &lockConfigObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
	put := func(body string, header http.Header) response {
		if header == nil {
			header = http.Header{"Content-Md5": {base64MD5([]byte(body))}}
		}
		return serve(t, objectAPI, http.MethodPut, "/bucket?object-lock", header, []byte(body))
	}
	config := func(inner string) string {
		return "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>" + inner + "</ObjectLockConfiguration>"
	}
	rule := func(retention string) string {
		return config("<Rule><DefaultRetention>" + retention + "</DefaultRetention></Rule>")
	}

	for _, tt := range []struct{ name, body, code string }{
		{"empty body", "", "MissingRequestBodyError"},
		{"two Rules", config("<Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention></Rule><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention></Rule>"), "MalformedXML"},
		{"two ObjectLockEnabled", "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>", "MalformedXML"},
		{"two DefaultRetentions", config("<Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention></Rule>"), "MalformedXML"},
		{"two Days", rule("<Mode>GOVERNANCE</Mode><Days>1</Days><Days>1</Days>"), "MalformedXML"},
		{"unknown element", config("<Foo/>"), "MalformedXML"},
		{"unknown element in Rule", config("<Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention><Foo/></Rule>"), "MalformedXML"},
		{"unknown element in DefaultRetention", rule("<Mode>GOVERNANCE</Mode><Days>1</Days><Foo/>"), "MalformedXML"},
		{"empty Rule", config("<Rule></Rule>"), "MalformedXML"},
		{"empty DefaultRetention", rule(""), "MalformedXML"},
		{"Days without Mode", rule("<Days>1</Days>"), "MalformedXML"},
		{"Mode without period", rule("<Mode>GOVERNANCE</Mode>"), "InvalidRequest"},
		{"Days 0", rule("<Mode>GOVERNANCE</Mode><Days>0</Days>"), "InvalidArgument"},
		// Text in the elements that only hold elements, but not whitespace.
		{"text in root", "<ObjectLockConfiguration>hi<ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>", "MalformedXML"},
		{"text after the elements", config("hi"), "MalformedXML"},
		{"cdata in root", config("<![CDATA[hi]]>"), "MalformedXML"},
		{"entity in root", config("&amp;"), "MalformedXML"},
		{"non-breaking space in root", config("\u00a0"), "MalformedXML"},
		{"text in Rule", config("<Rule>hi<DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention></Rule>"), "MalformedXML"},
		{"text in DefaultRetention", rule("hi<Mode>GOVERNANCE</Mode><Days>1</Days>"), "MalformedXML"},
	} {
		objectAPI.got = nil
		resp := put(tt.body, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, tt.name)
		require.Contains(t, resp.Body, "<Code>"+tt.code+"</Code>", tt.name)
		require.Nil(t, objectAPI.got, tt.name)
	}

	// Any root element and declared encoding is accepted.
	for _, body := range []string{
		`<LockConfig><ObjectLockEnabled>Enabled</ObjectLockEnabled></LockConfig>`,
		"<ObjectLockConfiguration>\n\t<!-- c --><ObjectLockEnabled>Enabled</ObjectLockEnabled>\r\n<Rule> <DefaultRetention>\n<Mode>GOVERNANCE</Mode><Days>1</Days> </DefaultRetention><![CDATA[ ]]></Rule></ObjectLockConfiguration>",
		`<?xml version="1.0" encoding="bogus"?>` + rule("<Mode>COMPLIANCE</Mode><Years>1</Years>"),
	} {
		objectAPI.got = nil
		resp := put(body, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, body+": "+resp.Body)
		require.NotNil(t, objectAPI.got, body)
		require.Equal(t, "Enabled", objectAPI.got.ObjectLockEnabled, body)
	}
	require.Equal(t, objectlock.RetCompliance, objectAPI.got.Rule.DefaultRetention.Mode)
	require.Equal(t, int32(1), *objectAPI.got.Rule.DefaultRetention.Years)

	resp := put(config(""), http.Header{})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>InvalidRequest</Code>")
}

func TestBucketConfigurationErrors(t *testing.T) {
	for _, tt := range []struct {
		name, target, body, code string
	}{
		// The PutBucketVersioning cases are S3's answers.
		{"versioning status", "/bucket?versioning", `<VersioningConfiguration><Status>Bogus</Status></VersioningConfiguration>`, "MalformedXML"},
		{"versioning lowercase status", "/bucket?versioning", `<VersioningConfiguration><Status>enabled</Status></VersioningConfiguration>`, "MalformedXML"},
		{"versioning wrong root", "/bucket?versioning", `<Bogus/>`, "IllegalVersioningConfigurationException"},
		{"versioning empty status", "/bucket?versioning", `<VersioningConfiguration><Status></Status></VersioningConfiguration>`, "IllegalVersioningConfigurationException"},
		{"versioning empty", "/bucket?versioning", ``, "MissingRequestBodyError"},
		{"versioning unknown element", "/bucket?versioning", `<VersioningConfiguration><Status>Enabled</Status><Foo/></VersioningConfiguration>`, "MalformedXML"},
		{"versioning two statuses", "/bucket?versioning", `<VersioningConfiguration><Status>Enabled</Status><Status>Enabled</Status></VersioningConfiguration>`, "MalformedXML"},
		{"versioning bogus MFA delete", "/bucket?versioning", `<VersioningConfiguration><Status>Enabled</Status><MfaDelete>Maybe</MfaDelete></VersioningConfiguration>`, "MalformedXML"},
		// S3 refuses text in the elements that only hold elements, but allows whitespace.
		{"versioning text", "/bucket?versioning", `<VersioningConfiguration>hi<Status>Enabled</Status></VersioningConfiguration>`, "MalformedXML"},
		{"versioning text after", "/bucket?versioning", `<VersioningConfiguration><Status>Enabled</Status>hi</VersioningConfiguration>`, "MalformedXML"},
		{"versioning cdata", "/bucket?versioning", `<VersioningConfiguration><![CDATA[hi]]><Status>Enabled</Status></VersioningConfiguration>`, "MalformedXML"},
		{"versioning entity", "/bucket?versioning", `<VersioningConfiguration>&amp;<Status>Enabled</Status></VersioningConfiguration>`, "MalformedXML"},
		{"versioning non-breaking space", "/bucket?versioning", "<VersioningConfiguration>\u00a0<Status>Enabled</Status></VersioningConfiguration>", "MalformedXML"},
		{"versioning MFA delete without status", "/bucket?versioning", `<VersioningConfiguration><MfaDelete>Enabled</MfaDelete></VersioningConfiguration>`, "IllegalVersioningConfigurationException"},
		{"object lock", "/bucket?object-lock", `<ObjectLockConfiguration><ObjectLockEnabled>Bogus</ObjectLockEnabled></ObjectLockConfiguration>`, "MalformedXML"},
		{"object lock period", "/bucket?object-lock", `<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>-1</Days></DefaultRetention></Rule></ObjectLockConfiguration>`, "InvalidArgument"},
		{"notification", "/bucket?notification", `<NotificationConfiguration><QueueConfiguration><Queue>arn:minio:sqs::1:webhook</Queue><Event>s3:Bogus</Event></QueueConfiguration></NotificationConfiguration>`, "InvalidArgument"},
		{"notification without event", "/bucket?notification", `<NotificationConfiguration><QueueConfiguration><Queue>arn:gcp:pubsub::project:topic</Queue></QueueConfiguration></NotificationConfiguration>`, "MalformedXML"},
		{"notification empty", "/bucket?notification", ``, "MissingRequestBodyError"},
		{"notification encoding", "/bucket?notification", `<?xml version="1.0" encoding="bogus"?><NotificationConfiguration></NotificationConfiguration>`, "MalformedXML"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			header := http.Header{"Content-Md5": {base64MD5([]byte(tt.body))}}
			resp := serve(t, &fakeObjectLayer{}, http.MethodPut, tt.target, header, []byte(tt.body))
			require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
			require.Contains(t, resp.Body, "<Code>"+tt.code+"</Code>")
		})
	}
}

type versioningObjectLayer struct {
	*fakeObjectLayer
	set func(*versioning.Versioning)
}

func (l *versioningObjectLayer) SetBucketVersioning(_ context.Context, _ string, v *versioning.Versioning) error {
	l.set(v)
	return nil
}

func TestPutBucketVersioning(t *testing.T) {
	var got []*versioning.Versioning
	objectAPI := &versioningObjectLayer{fakeObjectLayer: &fakeObjectLayer{}, set: func(v *versioning.Versioning) { got = append(got, v) }}
	put := func(mfaDelete string) response {
		body := "<VersioningConfiguration><Status>Enabled</Status>" + mfaDelete + "</VersioningConfiguration>"
		return serve(t, objectAPI, http.MethodPut, "/bucket?versioning", nil, []byte(body))
	}

	// Like S3, enabling MFA delete is refused, but disabling it is accepted.
	resp := put("<MfaDelete>Enabled</MfaDelete>")
	require.Equal(t, http.StatusForbidden, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>AccessDenied</Code><Message>Mfa Authentication must be used for this request</Message>")
	require.Empty(t, got)

	for _, mfaDelete := range []string{"<MfaDelete>Disabled</MfaDelete>", "<MfaDelete></MfaDelete>", ""} {
		resp = put(mfaDelete)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	}
	require.Len(t, got, 3)
	require.Equal(t, versioning.Enabled, got[0].Status)

	// S3 ignores the declared encoding.
	resp = serve(t, objectAPI, http.MethodPut, "/bucket?versioning", nil, []byte(`<?xml version="1.0" encoding="bogus"?><VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>`))
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, versioning.Suspended, got[3].Status)

	// Whitespace, tabs and comments between the elements are accepted.
	resp = serve(t, objectAPI, http.MethodPut, "/bucket?versioning", nil, []byte("<VersioningConfiguration>\n\t <!-- c --><![CDATA[ ]]><Status>Enabled</Status>\r\n</VersioningConfiguration>"))
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, versioning.Enabled, got[4].Status)
}

func TestBackendEOFIsInternalError(t *testing.T) {
	// An io.EOF from the object layer isn't an XML parsing error, and an io.ErrUnexpectedEOF isn't
	// an incomplete request body, even as the IncompleteBody miniogw.ConvertError makes of it.
	for _, eof := range []error{io.EOF, io.ErrUnexpectedEOF} {
		objectAPI := &fakeObjectLayer{
			getObjectInfo: func(_ context.Context, bucket, object string, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
				return cmd.ObjectInfo{}, miniogw.ConvertError(fmt.Errorf("download: %w", eof), bucket, object)
			},
		}

		resp := serve(t, objectAPI, http.MethodHead, "/bucket/key", nil, nil)
		require.Equal(t, http.StatusInternalServerError, resp.StatusCode, eof)
	}
}

// copyingPutObjectLayer reads the body like miniogw's PutObject.
type copyingPutObjectLayer struct {
	*fakeObjectLayer
}

func (copyingPutObjectLayer) PutObject(_ context.Context, bucket, object string, data *cmd.PutObjReader, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
	_, err := io.Copy(io.Discard, data)
	return cmd.ObjectInfo{}, miniogw.ConvertError(err, bucket, object)
}

func TestTruncatedBodyIsIncompleteBody(t *testing.T) {
	// The chunk declares 0x40 bytes, but the body ends early.
	resp := serve(t, copyingPutObjectLayer{&fakeObjectLayer{}}, http.MethodPut, "/bucket/key", http.Header{
		"X-Amz-Content-Sha256":         {"STREAMING-UNSIGNED-PAYLOAD-TRAILER"},
		"Content-Encoding":             {"aws-chunked"},
		"X-Amz-Decoded-Content-Length": {"64"},
		"X-Amz-Trailer":                {"x-amz-checksum-crc32"},
	}, []byte("40\r\nshort"))
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>IncompleteBody</Code>")
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

// TestListMaxKeysClamped checks that at most 1000 entries are listed, while the requested max-keys
// is echoed like S3.
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
		require.Contains(t, resp.Body, "<MaxKeys>5000</MaxKeys>", target)
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

func TestListEmptyEncodingTypeLikeS3(t *testing.T) {
	for _, target := range []string{"/bucket?encoding-type=", "/bucket?list-type=2&encoding-type=", "/bucket?versions&encoding-type=", "/bucket?uploads&encoding-type="} {
		objectAPI := &fakeObjectLayer{}
		resp := serve(t, objectAPI, http.MethodGet, target, nil, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, target)
		require.Contains(t, resp.Body, "<Code>InvalidArgument</Code><Message>Invalid Encoding Method specified in Request</Message>", target)
		require.Empty(t, objectAPI.Calls(), target)
	}
}

// TestListObjectsLikeS3 checks the responses that S3 returned for the same ListObjects requests.
func TestListObjectsLikeS3(t *testing.T) {
	objectAPI := &bucketObjectLayer{
		fakeObjectLayer: &fakeObjectLayer{},
		listObjects: func(context.Context, string, string, string, string, int) (cmd.ListObjectsInfo, error) {
			return cmd.ListObjectsInfo{IsTruncated: true, NextMarker: "a/1", Objects: []cmd.ObjectInfo{{Name: "a/1"}}}, nil
		},
	}

	for target, message := range map[string]string{
		"/bucket?start-after=a":         "startAfter only supported in REST.GET.BUCKET with list-type=2",
		"/bucket?continuation-token=":   "continuation-token only supported in REST.GET.BUCKET with list-type=2",
		"/bucket?continuation-token=YQ": "continuation-token only supported in REST.GET.BUCKET with list-type=2",
		// The continuation-token is reported before start-after, and max-keys is parsed before both.
		"/bucket?continuation-token=&start-after=":    "continuation-token only supported in REST.GET.BUCKET with list-type=2",
		"/bucket?start-after=&continuation-token=":    "continuation-token only supported in REST.GET.BUCKET with list-type=2",
		"/bucket?continuation-token=a&encoding-type=": "continuation-token only supported in REST.GET.BUCKET with list-type=2",
		"/bucket?start-after=a&encoding-type=foo":     "startAfter only supported in REST.GET.BUCKET with list-type=2",
		"/bucket?continuation-token=a&max-keys=abc":   "Provided max-keys not an integer or within integer range",
		"/bucket?start-after=a&max-keys=abc":          "Provided max-keys not an integer or within integer range",
	} {
		resp := serve(t, objectAPI, http.MethodGet, target, nil, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, target)
		require.Contains(t, resp.Body, "<Code>InvalidArgument</Code><Message>"+message+"</Message>", target)
	}

	// NextMarker is only sent with a delimiter.
	resp := serve(t, objectAPI, http.MethodGet, "/bucket?max-keys=1", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.NotContains(t, resp.Body, "<NextMarker>")
	resp = serve(t, objectAPI, http.MethodGet, "/bucket?max-keys=1&delimiter=/", nil, nil)
	require.Contains(t, resp.Body, "<NextMarker>a/1</NextMarker>")
}

func TestListObjectsV2MarkerLikeS3(t *testing.T) {
	for _, target := range []string{"/bucket?list-type=2&marker=a", "/bucket?list-type=2&marker="} {
		objectAPI := &fakeObjectLayer{}
		resp := serve(t, objectAPI, http.MethodGet, target, nil, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, target)
		require.Contains(t, resp.Body, "<Code>InvalidArgument</Code><Message>Marker unsupported with REST.GET.BUCKET in list-type=2</Message>", target)
		require.Empty(t, objectAPI.Calls(), target)
	}
}

// TestListObjectsV2MarkerOrderLikeS3 checks that S3 parses max-keys before it refuses the marker.
func TestListObjectsV2MarkerOrderLikeS3(t *testing.T) {
	for target, message := range map[string]string{
		"/bucket?list-type=2&marker=&max-keys=abc":                    "Provided max-keys not an integer or within integer range",
		"/bucket?list-type=2&marker=a&max-keys=2147483648":            "Provided max-keys not an integer or within integer range",
		"/bucket?list-type=2&marker=a&max-keys=abc&encoding-type=foo": "Provided max-keys not an integer or within integer range",
	} {
		objectAPI := &fakeObjectLayer{}
		resp := serve(t, objectAPI, http.MethodGet, target, nil, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, target)
		require.Contains(t, resp.Body, "<Code>InvalidArgument</Code><Message>"+message+"</Message>", target)
		require.Empty(t, objectAPI.Calls(), target)
	}
}

// TestListParameterOrderLikeS3 checks the order in which S3 reported the problems of the same list
// requests, and their messages.
func TestListParameterOrderLikeS3(t *testing.T) {
	const (
		maxKeysValue     = "Provided max-keys not an integer or within integer range"
		maxKeysNegative  = "Argument maxKeys must be an integer between 0 and 2147483647"
		encoding         = "Invalid Encoding Method specified in Request"
		noKeyMarker      = "A version-id marker cannot be specified without a key marker."
		emptyVersionID   = "A version-id marker cannot be empty."
		invalidVersionID = "Invalid version id specified"
		maxUploadsValue  = "Provided max-uploads not an integer or within integer range"
		maxUploadsNeg    = "Argument max-uploads must be an integer between 0 and 2147483647"
		maxPartsValue    = "Provided max-parts not an integer or within integer range"
		maxPartsNeg      = "Argument max-parts must be an integer between 0 and 2147483647"
		markerValue      = "Provided part-number-marker not an integer or within integer range"
		markerNegative   = "Argument part-number-marker must be an integer between 0 and 2147483647"
	)

	for target, message := range map[string]string{
		// The encoding-type is checked before a negative max-keys, but after one that isn't an integer.
		"/bucket?max-keys=-1":                                maxKeysNegative,
		"/bucket?list-type=2&max-keys=-1":                    maxKeysNegative,
		"/bucket?versions&max-keys=-1":                       "max-keys cannot be negative",
		"/bucket?encoding-type=&max-keys=-1":                 encoding,
		"/bucket?encoding-type=foo&max-keys=-1":              encoding,
		"/bucket?max-keys=-1&encoding-type=":                 encoding,
		"/bucket?list-type=2&encoding-type=foo&max-keys=-1":  encoding,
		"/bucket?versions&encoding-type=foo&max-keys=-1":     encoding,
		"/bucket?encoding-type=&max-keys=abc":                maxKeysValue,
		"/bucket?max-keys=2147483648&encoding-type=foo":      maxKeysValue,
		"/bucket?versions&max-keys=abc&encoding-type=foo":    maxKeysValue,
		"/bucket?list-type=2&max-keys=abc&encoding-type=foo": maxKeysValue,
		"/bucket?uploads&max-uploads=-1":                     maxUploadsNeg,
		"/bucket?uploads&encoding-type=&max-uploads=-1":      encoding,
		"/bucket?uploads&encoding-type=&max-uploads=abc":     maxUploadsValue,
		"/bucket?uploads&max-uploads=2147483648":             maxUploadsValue,
		"/bucket?uploads&max-uploads=1.5":                    maxUploadsValue,
		// The V2 continuation-token is checked after the encoding-type and before a negative max-keys.
		"/bucket?list-type=2&continuation-token=&encoding-type=foo":             encoding,
		"/bucket?list-type=2&continuation-token=%21&encoding-type=foo":          encoding,
		"/bucket?list-type=2&continuation-token=&encoding-type=foo&max-keys=-1": encoding,
		"/bucket?list-type=2&continuation-token=&max-keys=-1":                   "The continuation token provided is incorrect",
		"/bucket?list-type=2&continuation-token=%21&max-keys=-1":                "The continuation token provided is incorrect",
		"/bucket?list-type=2&continuation-token=&max-keys=abc":                  maxKeysValue,
		"/bucket?list-type=2&continuation-token=&start-after=a&max-keys=-1":     "The continuation token provided is incorrect",
		// The version-id-marker is checked before the encoding-type and a negative max-keys, and
		// an empty key-marker counts.
		"/bucket?versions&encoding-type=&version-id-marker=!!":                            noKeyMarker,
		"/bucket?versions&version-id-marker=":                                             noKeyMarker,
		"/bucket?versions&version-id-marker=00000000000000000000000000000001&max-keys=-1": noKeyMarker,
		"/bucket?versions&version-id-marker=abc&max-keys=abc":                             maxKeysValue,
		"/bucket?versions&key-marker=&version-id-marker=":                                 emptyVersionID,
		"/bucket?versions&key-marker=a&version-id-marker=&encoding-type=foo":              emptyVersionID,
		"/bucket?versions&key-marker=&version-id-marker=!!":                               invalidVersionID,
		"/bucket?versions&key-marker=a&version-id-marker=!!&max-keys=-1":                  invalidVersionID,
	} {
		objectAPI := &fakeObjectLayer{}
		resp := serve(t, objectAPI, http.MethodGet, target, nil, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, target)
		require.Contains(t, resp.Body, "<Code>InvalidArgument</Code><Message>"+message+"</Message>", target)
		require.Empty(t, objectAPI.Calls(), target)
	}

	// Only the parameters of ListParts that aren't integers are checked before the upload is looked
	// up. Its encoding-type and negative limits are checked after.
	listParts := &fakeObjectLayer{
		listObjectParts: func(_ context.Context, _, _, _ string, marker, maxParts int, _ cmd.ObjectOptions) (cmd.ListPartsInfo, error) {
			require.GreaterOrEqual(t, marker, 0)
			require.GreaterOrEqual(t, maxParts, 0)
			return cmd.ListPartsInfo{}, nil
		},
	}
	for query, message := range map[string]string{
		"max-parts=abc&encoding-type=foo":          maxPartsValue,
		"max-parts=2147483648&encoding-type=foo":   maxPartsValue,
		"max-parts=-1&encoding-type=foo":           encoding,
		"max-parts=-1":                             maxPartsNeg,
		"part-number-marker=abc":                   markerValue,
		"part-number-marker=2147483648":            markerValue,
		"part-number-marker=abc&encoding-type=foo": markerValue,
		"part-number-marker=abc&max-parts=abc":     maxPartsValue,
		"part-number-marker=-1&max-parts=-1":       maxPartsNeg,
		"part-number-marker=-1":                    markerNegative,
		"encoding-type=":                           encoding,
		"encoding-type=foo&part-number-marker=-1":  encoding,
	} {
		resp := serve(t, listParts, http.MethodGet, "/bucket/key?uploadId=id&"+query, nil, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, query)
		require.Contains(t, resp.Body, "<Code>InvalidArgument</Code><Message>"+message+"</Message>", query)
	}
	missing := &fakeObjectLayer{listObjectParts: func(context.Context, string, string, string, int, int, cmd.ObjectOptions) (cmd.ListPartsInfo, error) {
		return cmd.ListPartsInfo{}, cmd.InvalidUploadID{}
	}}
	resp := serve(t, missing, http.MethodGet, "/bucket/key?uploadId=id&encoding-type=foo", nil, nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>NoSuchUpload</Code>")
	resp = serve(t, missing, http.MethodGet, "/bucket/key?uploadId=id&max-parts=abc", nil, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)

	// The limits that fit in an int32 are accepted as before, and an empty one is missing.
	resp = serve(t, listParts, http.MethodGet, "/bucket/key?uploadId=id&max-parts=2147483647&part-number-marker=2147483647", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
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

func (configObjectLayer) GetObjectInfo(context.Context, string, string, cmd.ObjectOptions) (cmd.ObjectInfo, error) {
	return cmd.ObjectInfo{UserDefined: map[string]string{"s3:tags": "k=v"}}, nil
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

func TestHeadBucketRegion(t *testing.T) {
	for _, tt := range []struct {
		err    error
		status int
		region string
	}{
		{nil, http.StatusOK, "us-east-1"},
		{cmd.PrefixAccessDenied{Bucket: "bucket"}, http.StatusForbidden, "us-east-1"},
		{cmd.BucketNotFound{Bucket: "bucket"}, http.StatusNotFound, ""},
	} {
		objectAPI := &bucketObjectLayer{
			fakeObjectLayer: &fakeObjectLayer{},
			getBucketInfo: func(_ context.Context, bucket string) (cmd.BucketInfo, error) {
				return cmd.BucketInfo{Name: bucket}, tt.err
			},
		}
		resp := serve(t, objectAPI, http.MethodHead, "/bucket", nil, nil)
		require.Equal(t, tt.status, resp.StatusCode, tt.err)
		require.Equal(t, tt.region, resp.Header.Get("X-Amz-Bucket-Region"), tt.err)
	}
}
