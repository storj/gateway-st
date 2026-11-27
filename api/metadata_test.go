// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"maps"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/amwolff/awsig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/gateway/api"
)

// TestExtractMetadataFromQuery checks that, like S3, a query only sets user-defined metadata and a
// lowercase storage class. S3 ignores headers such as Content-Type, Cache-Control and Expires in
// the query of presigned URLs too, in any casing and for CreateMultipartUpload and CopyObject as
// well as PutObject.
func TestExtractMetadataFromQuery(t *testing.T) {
	fullMetaQuery := "Cache-Control=max-age%3D60" +
		"&Content-Encoding=gzip" +
		"&Content-Disposition=inline" +
		"&Content-Language=en-US" +
		"&Content-Type=image%2Fpng" +
		"&Expires=%2B2h" +
		"&cache-control=no-store" +
		"&content-type=text%2Fhtml" +
		"&X-Amz-Storage-Class=STANDARD" +
		"&x-amz-meta-key=amzMeta" +
		"&x-minio-meta-key=minioMeta"

	userMeta := map[string]string{
		"X-Amz-Meta-Key":   "amzMeta",
		"X-Minio-Meta-Key": "minioMeta",
	}

	for _, tt := range []struct {
		name         string
		query        string
		expectedMeta map[string]string
	}{
		{
			name:         "Presigned (SigV4): only user metadata extracted",
			query:        "X-Amz-Algorithm=AWS4-HMAC-SHA256&" + fullMetaQuery,
			expectedMeta: userMeta,
		},
		{
			name:         "Presigned (SigV2): only user metadata extracted",
			query:        "AWSAccessKeyId=AKIDEXAMPLE&" + fullMetaQuery,
			expectedMeta: userMeta,
		},
		{
			name:         "Presigned (SigV2): the URL's expiry isn't metadata",
			query:        "AWSAccessKeyId=AKIDEXAMPLE&Expires=1796412512&Signature=c2lnbmF0dXJl",
			expectedMeta: map[string]string{},
		},
		{
			name:         "Not presigned: only user metadata extracted",
			query:        fullMetaQuery,
			expectedMeta: userMeta,
		},
		{
			name:         "Presigned (SigV4): lowercase storage class extracted, other casings ignored",
			query:        "X-Amz-Algorithm=AWS4-HMAC-SHA256&x-amz-storage-class=BOGUS&X-Amz-Storage-Class=STANDARD&X-AMZ-STORAGE-CLASS=STANDARD",
			expectedMeta: map[string]string{"X-Amz-Storage-Class": "BOGUS"},
		},
		{
			name:         "Not presigned: lowercase storage class extracted, other casings ignored",
			query:        "x-amz-storage-class=BOGUS&X-Amz-Storage-Class=STANDARD&X-AMZ-STORAGE-CLASS=STANDARD",
			expectedMeta: map[string]string{"X-Amz-Storage-Class": "BOGUS"},
		},
		{
			name:         "User metadata extracted in any casing",
			query:        "X-AMZ-META-FOO=bar",
			expectedMeta: map[string]string{"X-Amz-Meta-Foo": "bar"},
		},
		{
			name:         "Duplicate keys, same casing: values joined with comma",
			query:        "x-amz-meta-foo=bar&x-amz-meta-foo=baz",
			expectedMeta: map[string]string{"X-Amz-Meta-Foo": "bar,baz"},
		},
		{
			name:         "Duplicate keys, different casing: last exact casing is selected",
			query:        "x-amz-meta-FOO=bar&x-amz-meta-foo=baz",
			expectedMeta: map[string]string{"X-Amz-Meta-Foo": "baz"},
		},
		{
			name:         "Duplicate keys, different casing: values for matching casing joined with comma",
			query:        "x-amz-meta-FOO=bar&x-amz-meta-foo=baz&x-amz-meta-FOO=quux",
			expectedMeta: map[string]string{"X-Amz-Meta-Foo": "bar,quux"},
		},
		{
			name:         "Invalid value is ignored",
			query:        "x-amz-meta-foo=bar;baz&x-amz-meta-qux=quux",
			expectedMeta: map[string]string{"X-Amz-Meta-Qux": "quux"},
		},
		{
			name:         "Irrelevant keys are ignored",
			query:        "foo=bar&baz=qux",
			expectedMeta: map[string]string{},
		},
		{
			name:         "Empty query produces empty metadata",
			query:        "",
			expectedMeta: map[string]string{},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			meta := make(map[string]string)
			api.ExtractMetadataFromQuery(&url.URL{RawQuery: tt.query}, meta)
			require.Equal(t, tt.expectedMeta, meta)
		})
	}
}

func TestExtractMetadataFromHeader(t *testing.T) {
	header := http.Header{
		"Content-Type":        {"image/png"},
		"Cache-Control":       {"max-age=60"},
		"Content-Language":    {"en-US", "en-CA"},
		"Content-Encoding":    {"gzip"},
		"Content-Disposition": {"inline"},
		"Expires":             {"+2h"},
		"X-Amz-Storage-Class": {"STANDARD"},
		"X-Amz-Meta-Key":      {"amzMeta1", "amzMeta2"},
		"X-Minio-Meta-Key":    {"minioMeta1", "minioMeta2"},
	}

	expectedMeta := make(map[string]string)
	for k, values := range header {
		expectedMeta[k] = strings.Join(values, ",")
	}

	// Insert headers that should not be extracted.
	maps.Copy(header, http.Header{
		"Authorization":  {"AWS 1234567890ABCDEF:g0h1i2j3k4l5m6n7o8p9q0r1s2="},
		"Content-Length": {"123"},
	})

	extracted := make(map[string]string)
	api.ExtractMetadataFromHeader(header, extracted)
	require.Equal(t, expectedMeta, extracted)
}

func TestExtractMetadataFromPostForm(t *testing.T) {
	postForm := awsig.PostForm{
		"Content-Type":        {{Value: "image/png"}},
		"Cache-Control":       {{Value: "max-age=60"}},
		"Content-Language":    {{Value: "en-US"}, {Value: "en-CA"}},
		"Content-Encoding":    {{Value: "gzip"}},
		"Content-Disposition": {{Value: "inline"}},
		"Expires":             {{Value: "+2h"}},
		"X-Amz-Storage-Class": {{Value: "STANDARD"}},
		"X-Amz-Meta-Key":      {{Value: "amzMeta1"}, {Value: "amzMeta2"}},
		"X-Minio-Meta-Key":    {{Value: "minioMeta1"}, {Value: "minioMeta2"}},
	}

	expectedMeta := make(map[string]string)
	for k, elems := range postForm {
		values := make([]string, 0, len(elems))
		for _, elem := range elems {
			values = append(values, elem.Value)
		}
		expectedMeta[k] = strings.Join(values, ",")
	}

	// Insert fields that should not be extracted.
	maps.Copy(postForm, awsig.PostForm{
		"Authorization":  {{Value: "AWS 1234567890ABCDEF:g0h1i2j3k4l5m6n7o8p9q0r1s2="}},
		"Content-Length": {{Value: "123"}},
	})

	require.Equal(t, expectedMeta, api.ExtractMetadataFromPostForm(postForm))
}

func TestQueryKeys(t *testing.T) {
	for _, tt := range []struct {
		queryString string
		expected    []string
	}{
		{"", []string{}},
		{"a=1", []string{"a"}},
		{"b=2&a=1&c=3", []string{"b", "a", "c"}},
		{"flag", []string{"flag"}},
		{"a=1&a=2", []string{"a", "a"}},
		{"a=1&&b=2&", []string{"a", "b"}},
		{"a=1=2", []string{"a"}},
		{"a;b=1&c=2", []string{"c"}},
		{"a%20b=1", []string{"a b"}},
		{"%zz=1&ok=2", []string{"ok"}},
		{"a+b=1", []string{"a b"}},
		{"%zz=1&%gg=2", []string{}},
	} {
		assert.Equal(t, tt.expected, api.QueryKeys(tt.queryString), "Query string: %s", tt.queryString)
	}
}
