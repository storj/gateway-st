// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"net/http"
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
