// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/minio/cmd"
)

func TestStorageErrorTranslation(t *testing.T) {
	// Every error type miniogw returns.
	for _, tt := range []struct {
		err    error
		code   string
		status int
	}{
		{cmd.BucketNameInvalid{}, "InvalidBucketName", http.StatusBadRequest},
		{cmd.BucketNotFound{}, "NoSuchBucket", http.StatusNotFound},
		{cmd.BucketNotEmpty{}, "BucketNotEmpty", http.StatusConflict},
		{cmd.BucketAlreadyExists{}, "BucketAlreadyExists", http.StatusConflict},
		{cmd.BucketTaggingNotFound{}, "NoSuchTagSet", http.StatusNotFound},
		{cmd.BucketObjectLockConfigNotFound{}, "ObjectLockConfigurationNotFoundError", http.StatusNotFound},
		{cmd.ObjectNotFound{}, "NoSuchKey", http.StatusNotFound},
		{cmd.VersionNotFound{}, "NoSuchVersion", http.StatusNotFound},
		{cmd.ObjectNameInvalid{}, "InvalidArgument", http.StatusBadRequest},
		{cmd.ObjectNameTooLong{}, "KeyTooLongError", http.StatusBadRequest},
		{cmd.ObjectTooLarge{}, "EntityTooLarge", http.StatusBadRequest},
		{cmd.ObjectTooSmall{}, "EntityTooSmall", http.StatusBadRequest},
		{cmd.PartTooSmall{}, "EntityTooSmall", http.StatusBadRequest},
		{cmd.PrefixAccessDenied{}, "AccessDenied", http.StatusForbidden},
		{cmd.MethodNotAllowed{}, "MethodNotAllowed", http.StatusMethodNotAllowed},
		{cmd.IncompleteBody{}, "IncompleteBody", http.StatusBadRequest},
		{cmd.NotImplemented{}, "NotImplemented", http.StatusNotImplemented},
		{cmd.OperationTimedOut{}, "RequestTimeout", http.StatusServiceUnavailable},
		{cmd.SlowDown{}, "SlowDown", http.StatusServiceUnavailable},
		{cmd.InvalidUploadID{}, "NoSuchUpload", http.StatusNotFound},
		{cmd.InvalidPart{}, "InvalidPart", http.StatusBadRequest},
		{cmd.InvalidRange{}, "InvalidRange", http.StatusRequestedRangeNotSatisfiable},
		{cmd.InvalidArgument{}, "InvalidArgument", http.StatusBadRequest},
	} {
		for variant, err := range storageErrors(tt.err) {
			t.Run(tt.code+"/"+variant, func(t *testing.T) {
				resp := serve(t, &fakeObjectLayer{err: err}, http.MethodGet, "/bucket/key?acl", nil, nil)
				require.Equal(t, tt.status, resp.StatusCode, resp.Body)
				require.Contains(t, resp.Body, "<Code>"+tt.code+"</Code>")
			})
		}
	}
}

func TestDeleteObjectsPerKeyStorageError(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		deleteObjects: func(context.Context, string, []cmd.ObjectToDelete, cmd.ObjectOptions) ([]cmd.DeletedObject, []cmd.DeleteObjectsError, error) {
			return nil, []cmd.DeleteObjectsError{{ObjectName: "key", Error: cmd.ObjectNotFound{}}}, nil
		},
	}
	body := []byte("<Delete><Object><Key>key</Key></Object></Delete>")
	sum := md5.Sum(body)
	header := http.Header{"Content-Md5": {base64.StdEncoding.EncodeToString(sum[:])}}

	resp := serve(t, objectAPI, http.MethodPost, "/bucket?delete", header, body)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Error><Code>NoSuchKey</Code>")
}

func TestInvalidContentMD5(t *testing.T) {
	for _, target := range []struct{ method, path string }{
		{http.MethodPut, "/bucket/key"},
		{http.MethodPut, "/bucket/key?partNumber=1&uploadId=id"},
		{http.MethodPost, "/bucket?delete"},
		{http.MethodPost, "/bucket/key?uploadId=id"},
	} {
		for name, value := range map[string]string{
			"empty":      "",
			"not base64": "not base64!",
			"15 bytes":   base64.StdEncoding.EncodeToString(make([]byte, 15)),
		} {
			t.Run(target.method+" "+target.path+"/"+name, func(t *testing.T) {
				resp := serve(t, &fakeObjectLayer{}, target.method, target.path, http.Header{"Content-Md5": {value}}, []byte("<x/>"))
				require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
				require.Contains(t, resp.Body, "<Code>InvalidDigest</Code>")
			})
		}
	}
}

func TestMalformedTaggingHeader(t *testing.T) {
	for _, tagging := range []string{"a=%zz", "a=1;b=2"} {
		for _, target := range []struct{ method, path string }{
			{http.MethodPut, "/bucket/key"},
			{http.MethodPost, "/bucket/key?uploads"},
		} {
			t.Run(target.method+" "+target.path+"/"+tagging, func(t *testing.T) {
				resp := serve(t, &fakeObjectLayer{}, target.method, target.path, http.Header{"X-Amz-Tagging": {tagging}, "Content-Length": {"1"}}, []byte("x"))
				require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
				require.Contains(t, resp.Body, "<Code>InvalidArgument</Code>")
				require.Contains(t, resp.Body, "The header &#39;x-amz-tagging&#39; shall be encoded")
			})
		}
	}
}

func TestTagValidation(t *testing.T) {
	tagXML := func(tags [][2]string) string {
		var b strings.Builder
		for _, tag := range tags {
			fmt.Fprintf(&b, "<Tag><Key>%s</Key><Value>%s</Value></Tag>", tag[0], tag[1])
		}
		return "<Tagging><TagSet>" + b.String() + "</TagSet></Tagging>"
	}
	var eleven [][2]string
	for i := range 11 {
		eleven = append(eleven, [2]string{fmt.Sprint("k", i), "v"})
	}

	for _, tc := range []struct {
		name, header  string
		tags          [][2]string
		code, message string
	}{
		{"duplicate", "a=1&a=2", nil, "InvalidArgument", "The header &#39;x-amz-tagging&#39; shall be encoded"},
		{"aws prefix", "aws%3Akey=1", [][2]string{{"aws:key", "1"}}, "InvalidTag", "Your TagKey cannot be prefixed with aws:"},
		{"invalid key", "a%3F=1", [][2]string{{"a?", "1"}}, "InvalidTag", "The TagKey you have provided is invalid"},
		{"invalid value", "a=%2A", [][2]string{{"a", "*"}}, "InvalidTag", "The TagValue you have provided is invalid"},
		{"too many", "k0=v&k1=v&k2=v&k3=v&k4=v&k5=v&k6=v&k7=v&k8=v&k9=v&k10=v", eleven, "BadRequest", "Object tags cannot be greater than 10"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			check := func(resp response) {
				t.Helper()
				require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
				require.Contains(t, resp.Body, "<Code>"+tc.code+"</Code>")
				require.Contains(t, resp.Body, tc.message)
			}
			check(serve(t, &fakeObjectLayer{}, http.MethodPut, "/bucket/key", http.Header{"X-Amz-Tagging": {tc.header}, "Content-Length": {"1"}}, []byte("x")))
			check(serve(t, &fakeObjectLayer{}, http.MethodPost, "/bucket/key?uploads", http.Header{"X-Amz-Tagging": {tc.header}}, nil))
			copyAPI := &fakeObjectLayer{getObjectInfo: func(context.Context, string, string, cmd.ObjectOptions) (cmd.ObjectInfo, error) {
				return cmd.ObjectInfo{}, nil
			}}
			check(serve(t, copyAPI, http.MethodPut, "/bucket/dst", http.Header{
				"X-Amz-Copy-Source":       {"/bucket/src"},
				"X-Amz-Tagging-Directive": {"REPLACE"},
				"X-Amz-Tagging":           {tc.header},
			}, nil))
			if tc.tags == nil {
				return
			}
			body := tagXML(tc.tags)
			check(serve(t, &fakeObjectLayer{}, http.MethodPut, "/bucket/key?tagging", nil, []byte(body)))
			check(postObject(t, &postObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}, "/bucket",
				[]any{map[string]string{"bucket": "bucket"}, map[string]string{"key": "k"}, map[string]string{"tagging": body}},
				[][2]string{{"key", "k"}, {"tagging", body}}, "data"))
		})
	}

	resp := serve(t, &fakeObjectLayer{}, http.MethodPut, "/bucket?tagging", nil, []byte(tagXML([][2]string{{"aws:key", "1"}})))
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>InvalidTag</Code>")
	require.Contains(t, resp.Body, "System tags cannot be added/updated by requester")

	// Every allowed character class is accepted.
	objectAPI := &taggingObjectLayer{fakeObjectLayer: &fakeObjectLayer{}}
	resp = serve(t, objectAPI, http.MethodPut, "/bucket/key?tagging", nil, []byte(tagXML([][2]string{{"Ab1 +-=._:/@é", "ü 9"}})))
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
}
