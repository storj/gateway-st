// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/gateway/api"
	"storj.io/gateway/api/apierr"
	"storj.io/minio/cmd"
)

const (
	testAccessKeyID     = "access"
	testSecretAccessKey = "secret"
)

type testCredentialsProvider struct{}

func (testCredentialsProvider) Provide(_ context.Context, accessKeyID string) (string, api.AuthData, error) {
	return testSecretAccessKey, api.AuthData{AccessKeyID: accessKeyID}, nil
}

// fakeObjectLayer is a cmd.ObjectLayer whose methods panic unless overridden
// by a test. Overridable methods are recorded in calls, and return err instead
// of calling the override when err is set.
type fakeObjectLayer struct {
	cmd.ObjectLayer

	err error

	mu    sync.Mutex
	calls []string

	listBuckets             func(ctx context.Context) ([]cmd.BucketInfo, error)
	getObjectInfo           func(ctx context.Context, bucket, object string, opts cmd.ObjectOptions) (cmd.ObjectInfo, error)
	getObjectNInfo          func(ctx context.Context, bucket, object string, rs *cmd.HTTPRangeSpec, h http.Header, lockType cmd.LockType, opts cmd.ObjectOptions) (*cmd.GetObjectReader, error)
	copyObject              func(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo cmd.ObjectInfo, srcOpts, dstOpts cmd.ObjectOptions) (cmd.ObjectInfo, error)
	deleteObject            func(ctx context.Context, bucket, object string, opts cmd.ObjectOptions) (cmd.ObjectInfo, error)
	deleteObjectTags        func(ctx context.Context, bucket, object string, opts cmd.ObjectOptions) (cmd.ObjectInfo, error)
	deleteObjects           func(ctx context.Context, bucket string, objects []cmd.ObjectToDelete, opts cmd.ObjectOptions) ([]cmd.DeletedObject, []cmd.DeleteObjectsError, error)
	newMultipartUpload      func(ctx context.Context, bucket, object string, opts cmd.ObjectOptions) (string, error)
	completeMultipartUpload func(ctx context.Context, bucket, object, uploadID string, parts []cmd.CompletePart, opts cmd.ObjectOptions) (cmd.ObjectInfo, error)
	abortMultipartUpload    func(ctx context.Context, bucket, object, uploadID string, opts cmd.ObjectOptions) error
	listObjectParts         func(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, opts cmd.ObjectOptions) (cmd.ListPartsInfo, error)
	listMultipartUploads    func(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (cmd.ListMultipartsInfo, error)
}

func (f *fakeObjectLayer) record(method string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, method)
	return f.err
}

// Calls returns the names of the methods called so far.
func (f *fakeObjectLayer) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// storageErrors returns err as miniogw may return it: bare and wrapped.
func storageErrors(err error) map[string]error {
	return map[string]error{"bare": err, "wrapped": errs.Wrap(err)}
}

func (f *fakeObjectLayer) ListBuckets(ctx context.Context) ([]cmd.BucketInfo, error) {
	if err := f.record("ListBuckets"); err != nil {
		return nil, err
	}
	return f.listBuckets(ctx)
}

func (f *fakeObjectLayer) GetObjectInfo(ctx context.Context, bucket, object string, opts cmd.ObjectOptions) (cmd.ObjectInfo, error) {
	if err := f.record("GetObjectInfo"); err != nil {
		return cmd.ObjectInfo{}, err
	}
	return f.getObjectInfo(ctx, bucket, object, opts)
}

func (f *fakeObjectLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *cmd.HTTPRangeSpec, h http.Header, lockType cmd.LockType, opts cmd.ObjectOptions) (*cmd.GetObjectReader, error) {
	if err := f.record("GetObjectNInfo"); err != nil {
		return nil, err
	}
	return f.getObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
}

func (f *fakeObjectLayer) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo cmd.ObjectInfo, srcOpts, dstOpts cmd.ObjectOptions) (cmd.ObjectInfo, error) {
	if err := f.record("CopyObject"); err != nil {
		return cmd.ObjectInfo{}, err
	}
	return f.copyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
}

func (f *fakeObjectLayer) DeleteObject(ctx context.Context, bucket, object string, opts cmd.ObjectOptions) (cmd.ObjectInfo, error) {
	if err := f.record("DeleteObject"); err != nil {
		return cmd.ObjectInfo{}, err
	}
	return f.deleteObject(ctx, bucket, object, opts)
}

func (f *fakeObjectLayer) DeleteObjectTags(ctx context.Context, bucket, object string, opts cmd.ObjectOptions) (cmd.ObjectInfo, error) {
	if err := f.record("DeleteObjectTags"); err != nil {
		return cmd.ObjectInfo{}, err
	}
	return f.deleteObjectTags(ctx, bucket, object, opts)
}

func (f *fakeObjectLayer) DeleteObjects(ctx context.Context, bucket string, objects []cmd.ObjectToDelete, opts cmd.ObjectOptions) ([]cmd.DeletedObject, []cmd.DeleteObjectsError, error) {
	if err := f.record("DeleteObjects"); err != nil {
		return nil, nil, err
	}
	return f.deleteObjects(ctx, bucket, objects, opts)
}

func (f *fakeObjectLayer) NewMultipartUpload(ctx context.Context, bucket, object string, opts cmd.ObjectOptions) (string, error) {
	if err := f.record("NewMultipartUpload"); err != nil {
		return "", err
	}
	return f.newMultipartUpload(ctx, bucket, object, opts)
}

func (f *fakeObjectLayer) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []cmd.CompletePart, opts cmd.ObjectOptions) (cmd.ObjectInfo, error) {
	if err := f.record("CompleteMultipartUpload"); err != nil {
		return cmd.ObjectInfo{}, err
	}
	return f.completeMultipartUpload(ctx, bucket, object, uploadID, parts, opts)
}

func (f *fakeObjectLayer) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts cmd.ObjectOptions) error {
	if err := f.record("AbortMultipartUpload"); err != nil {
		return err
	}
	return f.abortMultipartUpload(ctx, bucket, object, uploadID, opts)
}

func (f *fakeObjectLayer) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, opts cmd.ObjectOptions) (cmd.ListPartsInfo, error) {
	if err := f.record("ListObjectParts"); err != nil {
		return cmd.ListPartsInfo{}, err
	}
	return f.listObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, opts)
}

func (f *fakeObjectLayer) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (cmd.ListMultipartsInfo, error) {
	if err := f.record("ListMultipartUploads"); err != nil {
		return cmd.ListMultipartsInfo{}, err
	}
	return f.listMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}

type response struct {
	StatusCode int
	Header     http.Header
	Body       string
}

func newTestRouter(objectAPI cmd.ObjectLayer) *mux.Router {
	router := mux.NewRouter()
	api.New(objectAPI, testCredentialsProvider{}, api.Config{}).RegisterHandlers(router)
	return router
}

// serve sends a signed request to an API backed by objectAPI and returns the response.
// The request body is an in-memory reader; use serveHTTP where body delivery matters.
func serve(t *testing.T, objectAPI cmd.ObjectLayer, method, target string, header http.Header, body []byte) response {
	t.Helper()

	router := newTestRouter(objectAPI)

	req := httptest.NewRequestWithContext(t.Context(), method, target, bytes.NewReader(body))
	for k, v := range header {
		req.Header[k] = v
	}
	signV4(req, body, time.Now())

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	// Canonicalize header keys like an HTTP client would. Handlers may set non-canonical keys.
	canonical := make(http.Header, len(rec.Header()))
	for k, v := range rec.Header() {
		canonical[http.CanonicalHeaderKey(k)] = append(canonical[http.CanonicalHeaderKey(k)], v...)
	}

	return response{
		StatusCode: rec.Code,
		Header:     canonical,
		Body:       rec.Body.String(),
	}
}

// serveHTTP is like serve, but sends the request over a real connection to an
// httptest.Server, so the handler reads a production net/http request body.
// To sign a payload other than body, set X-Amz-Content-Sha256 in header.
func serveHTTP(t *testing.T, objectAPI cmd.ObjectLayer, method, target string, header http.Header, body []byte) response {
	t.Helper()

	srv := httptest.NewServer(newTestRouter(objectAPI))
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(t.Context(), method, srv.URL+target, bytes.NewReader(body))
	require.NoError(t, err)
	for k, v := range header {
		req.Header[k] = v
	}
	signV4(req, body, time.Now())

	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return response{
		StatusCode: resp.StatusCode,
		Header:     resp.Header,
		Body:       string(respBody),
	}
}

// signV4 signs a request with AWS Signature Version 4 using the test credentials, following the
// AWS specification. Every header present on the request is signed. X-Amz-Content-Sha256 is set
// to the hash of body unless the request already has it.
//
// TODO: awsig differs from the specification for repeated headers (a line per value instead of
// comma-joined values), runs of spaces inside header values (kept instead of collapsed) and
// repeated values of the first query parameter (not separated by '&'). Requests with those are
// rejected with 403 until awsig is fixed.
func signV4(req *http.Request, body []byte, now time.Time) {
	const region, service = "us-east-1", "s3"

	amzDate := now.UTC().Format("20060102T150405Z")
	date := amzDate[:8]
	req.Header.Set("X-Amz-Date", amzDate)
	if req.Header.Get("X-Amz-Content-Sha256") == "" {
		req.Header.Set("X-Amz-Content-Sha256", hexSHA256(body))
	}

	headers := map[string]string{"host": req.Host}
	for k, v := range req.Header {
		trimmed := make([]string, len(v))
		for i := range v {
			trimmed[i] = strings.Join(strings.Fields(v[i]), " ")
		}
		headers[strings.ToLower(k)] = strings.Join(trimmed, ",")
	}
	names := slices.Sorted(maps.Keys(headers))
	var canonicalHeaders strings.Builder
	for _, name := range names {
		canonicalHeaders.WriteString(name + ":" + headers[name] + "\n")
	}
	signedHeaders := strings.Join(names, ";")

	uriEncode := func(s string) string {
		return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
	}
	// Parameters are sorted by name, then by value.
	query := req.URL.Query()
	var queryParams []string
	for _, k := range slices.Sorted(maps.Keys(query)) {
		for _, v := range slices.Sorted(slices.Values(query[k])) {
			queryParams = append(queryParams, uriEncode(k)+"="+uriEncode(v))
		}
	}

	canonicalRequest := strings.Join([]string{
		req.Method,
		// Like the verifier, encode everything in the path except unreserved characters and '/'.
		strings.ReplaceAll(uriEncode(req.URL.Path), "%2F", "/"),
		strings.Join(queryParams, "&"),
		canonicalHeaders.String(),
		signedHeaders,
		req.Header.Get("X-Amz-Content-Sha256"),
	}, "\n")

	scope := date + "/" + region + "/" + service + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hexSHA256([]byte(canonicalRequest))

	key := []byte("AWS4" + testSecretAccessKey)
	for _, part := range []string{date, region, service, "aws4_request"} {
		key = hmacSHA256(key, part)
	}
	signature := hex.EncodeToString(hmacSHA256(key, stringToSign))

	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+testAccessKeyID+"/"+scope+
		", SignedHeaders="+signedHeaders+", Signature="+signature)
}

func hexSHA256(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

func TestSignV4QueryOrder(t *testing.T) {
	objectAPI := singleObjectLayer(cmd.ObjectInfo{Bucket: "bucket", Name: "key", Size: 4}, "data")

	// Sorting "a-b=1" and "a=2" as strings would put a-b first, but parameters are sorted by name.
	resp := serve(t, objectAPI, http.MethodHead, "/bucket/key?a-b=1&a=2", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRequestID(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		listBuckets: func(context.Context) ([]cmd.BucketInfo, error) {
			return []cmd.BucketInfo{{Name: "bucket"}}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodGet, "/", nil, nil)
	body := resp.Body
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	require.Contains(t, body, "<Name>bucket</Name>")
	require.NotEmpty(t, resp.Header.Get("X-Amz-Request-Id"))
}

func TestListMultipartUploadsRouting(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		listMultipartUploads: func(_ context.Context, bucket, _, _, _, _ string, _ int) (cmd.ListMultipartsInfo, error) {
			return cmd.ListMultipartsInfo{
				Uploads: []cmd.MultipartInfo{{Bucket: bucket, Object: "key", UploadID: "upload-id"}},
			}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodGet, "/bucket?uploads", nil, nil)
	body := resp.Body
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	require.Contains(t, body, "<ListMultipartUploadsResult")
	require.Contains(t, body, "<UploadId>upload-id</UploadId>")
}

func TestErrorResponse(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		listMultipartUploads: func(context.Context, string, string, string, string, string, int) (cmd.ListMultipartsInfo, error) {
			return cmd.ListMultipartsInfo{}, apierr.CodeAccessDenied
		},
	}

	resp := serve(t, objectAPI, http.MethodGet, "/bucket?uploads", nil, nil)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	var errResp struct {
		XMLName    xml.Name `xml:"Error"`
		Code       string
		Message    string
		BucketName string
		Resource   string
		RequestID  string `xml:"RequestId"`
	}
	require.NoError(t, xml.Unmarshal([]byte(resp.Body), &errResp))
	require.Equal(t, "AccessDenied", errResp.Code)
	require.Equal(t, "Access Denied", errResp.Message)
	require.Equal(t, "bucket", errResp.BucketName)
	require.Equal(t, "/bucket", errResp.Resource)
	require.Equal(t, resp.Header.Get("X-Amz-Request-Id"), errResp.RequestID)
	require.NotEmpty(t, errResp.RequestID)

	// Virtual-hosted-style requests take the bucket from the host.
	router := mux.NewRouter()
	api.New(objectAPI, testCredentialsProvider{}, api.Config{Domains: []string{"example.com"}}).RegisterHandlers(router)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://bucket.example.com/key?tagging", nil))
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	require.Equal(t, "/bucket/key", errResp.Resource)
}

func TestDeleteObjectsErrors(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		deleteObjects: func(_ context.Context, _ string, objects []cmd.ObjectToDelete, _ cmd.ObjectOptions) ([]cmd.DeletedObject, []cmd.DeleteObjectsError, error) {
			return nil, []cmd.DeleteObjectsError{{ObjectName: objects[0].ObjectName, Error: apierr.CodeAccessDenied}}, nil
		},
	}

	body := []byte(`<Delete><Object><Key>key</Key></Object></Delete>`)
	sum := md5.Sum(body)
	header := http.Header{"Content-Md5": {base64.StdEncoding.EncodeToString(sum[:])}}

	resp := serve(t, objectAPI, http.MethodPost, "/bucket?delete", header, body)
	respBody := resp.Body
	require.Equal(t, http.StatusOK, resp.StatusCode, respBody)
	require.Contains(t, respBody, "<Error><Code>AccessDenied</Code><Message>Access Denied</Message><Key>key</Key>")
}

// singleObjectLayer returns a fake object layer containing a single object.
func singleObjectLayer(objInfo cmd.ObjectInfo, data string) *fakeObjectLayer {
	getObjectInfo := func(_ context.Context, bucket, object string, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
		if bucket != objInfo.Bucket || object != objInfo.Name {
			return cmd.ObjectInfo{}, apierr.CodeNoSuchKey
		}
		return objInfo, nil
	}
	return &fakeObjectLayer{
		getObjectInfo: getObjectInfo,
		getObjectNInfo: func(ctx context.Context, bucket, object string, rs *cmd.HTTPRangeSpec, _ http.Header, _ cmd.LockType, opts cmd.ObjectOptions) (*cmd.GetObjectReader, error) {
			objInfo, err := getObjectInfo(ctx, bucket, object, opts)
			if err != nil {
				return nil, err
			}
			start, length, err := rs.GetOffsetLength(objInfo.Size)
			if err != nil {
				return nil, apierr.CodeInvalidRange
			}
			return cmd.NewGetObjectReaderFromReader(strings.NewReader(data[start:start+length]), objInfo, opts)
		},
	}
}

func TestGetAndHeadObject(t *testing.T) {
	modTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	objInfo := cmd.ObjectInfo{
		Bucket:      "bucket",
		Name:        "key",
		ModTime:     modTime,
		Size:        10,
		ETag:        "etag",
		ContentType: "text/plain",
		VersionID:   "version",
		UserTags:    "a=1&b=2",
		UserDefined: map[string]string{
			"Content-Type":            "text/plain",
			"X-Amz-Meta-Foo":          "bar",
			"X-Minio-Internal-Secret": "hidden",
			"X-Amz-Meta-X-Amz-Unencrypted-Content-Length": "1",
			"Content-Range": "bytes 0-2/3",
			"Set-Cookie":    "session=1",
		},
	}
	objectAPI := singleObjectLayer(objInfo, "0123456789")

	lastModified := modTime.Format(http.TimeFormat)
	before := modTime.Add(-time.Hour).Format(http.TimeFormat)
	after := modTime.Add(time.Hour).Format(http.TimeFormat)

	for _, tt := range []struct {
		name      string
		target    string
		header    http.Header
		expStatus int
		expCode   string
		expBody   string
		expHeader map[string]string
	}{
		{
			name:      "entire object",
			target:    "/bucket/key",
			expStatus: http.StatusOK,
			expBody:   "0123456789",
			expHeader: map[string]string{
				"ETag":                    `"etag"`,
				"Last-Modified":           lastModified,
				"Content-Length":          "10",
				"Content-Type":            "text/plain",
				"Accept-Ranges":           "bytes",
				"X-Amz-Version-Id":        "version",
				"X-Amz-Tagging-Count":     "2",
				"X-Amz-Meta-Foo":          "bar",
				"X-Minio-Internal-Secret": "",
				"X-Amz-Meta-X-Amz-Unencrypted-Content-Length": "",
				"Content-Range": "",
				"Set-Cookie":    "",
			},
		},
		{
			name:      "range",
			target:    "/bucket/key",
			header:    http.Header{"Range": {"bytes=2-4"}},
			expStatus: http.StatusPartialContent,
			expBody:   "234",
			expHeader: map[string]string{"Content-Length": "3", "Content-Range": "bytes 2-4/10"},
		},
		{
			name:      "suffix range",
			target:    "/bucket/key",
			header:    http.Header{"Range": {"bytes=-3"}},
			expStatus: http.StatusPartialContent,
			expBody:   "789",
			expHeader: map[string]string{"Content-Range": "bytes 7-9/10"},
		},
		{
			name:      "range past end is truncated",
			target:    "/bucket/key",
			header:    http.Header{"Range": {"bytes=8-100"}},
			expStatus: http.StatusPartialContent,
			expBody:   "89",
			expHeader: map[string]string{"Content-Range": "bytes 8-9/10"},
		},
		{
			name:      "unsatisfiable range",
			target:    "/bucket/key",
			header:    http.Header{"Range": {"bytes=10-"}},
			expStatus: http.StatusRequestedRangeNotSatisfiable,
			expCode:   "InvalidRange",
		},
		{
			name:      "invalid range",
			target:    "/bucket/key",
			header:    http.Header{"Range": {"bytes=5-1"}},
			expStatus: http.StatusRequestedRangeNotSatisfiable,
			expCode:   "InvalidRange",
		},
		{
			name:      "if-range etag match",
			target:    "/bucket/key",
			header:    http.Header{"Range": {"bytes=2-4"}, "If-Range": {`"etag"`}},
			expStatus: http.StatusPartialContent,
			expBody:   "234",
			expHeader: map[string]string{"Content-Range": "bytes 2-4/10"},
		},
		{
			name:      "if-range etag mismatch",
			target:    "/bucket/key",
			header:    http.Header{"Range": {"bytes=2-4"}, "If-Range": {`"other"`}},
			expStatus: http.StatusOK,
			expBody:   "0123456789",
			expHeader: map[string]string{"Content-Length": "10", "Content-Range": ""},
		},
		{
			name:      "if-range weak etag",
			target:    "/bucket/key",
			header:    http.Header{"Range": {"bytes=2-4"}, "If-Range": {`W/"etag"`}},
			expStatus: http.StatusOK,
			expBody:   "0123456789",
		},
		{
			name:      "if-range date match",
			target:    "/bucket/key",
			header:    http.Header{"Range": {"bytes=2-4"}, "If-Range": {lastModified}},
			expStatus: http.StatusPartialContent,
			expBody:   "234",
		},
		{
			name:      "if-range date mismatch",
			target:    "/bucket/key",
			header:    http.Header{"Range": {"bytes=2-4"}, "If-Range": {before}},
			expStatus: http.StatusOK,
			expBody:   "0123456789",
		},
		{
			name:      "malformed range is ignored",
			target:    "/bucket/key",
			header:    http.Header{"Range": {"bytes=a-b"}},
			expStatus: http.StatusOK,
			expBody:   "0123456789",
		},
		{
			name:      "range with part number",
			target:    "/bucket/key?partNumber=1",
			header:    http.Header{"Range": {"bytes=0-1"}},
			expStatus: http.StatusBadRequest,
			expCode:   "InvalidRequest",
		},
		{
			name:      "range with nonexistent part number",
			target:    "/bucket/key?partNumber=2",
			header:    http.Header{"Range": {"bytes=0-1"}},
			expStatus: http.StatusBadRequest,
			expCode:   "InvalidRequest",
		},
		{
			name:      "first part",
			target:    "/bucket/key?partNumber=1",
			expStatus: http.StatusPartialContent,
			expBody:   "0123456789",
			expHeader: map[string]string{"Content-Range": "bytes 0-9/10", "Content-Length": "10"},
		},
		{
			name:      "nonexistent part",
			target:    "/bucket/key?partNumber=2",
			expStatus: http.StatusRequestedRangeNotSatisfiable,
			expCode:   "InvalidPartNumber",
		},
		{
			name:      "invalid part number",
			target:    "/bucket/key?partNumber=0",
			expStatus: http.StatusBadRequest,
			expCode:   "InvalidArgument",
		},
		{
			name:      "nonexistent key",
			target:    "/bucket/missing",
			expStatus: http.StatusNotFound,
			expCode:   "NoSuchKey",
		},
		{
			name:      "if-match mismatch",
			target:    "/bucket/key",
			header:    http.Header{"If-Match": {`"other"`}},
			expStatus: http.StatusPreconditionFailed,
			expCode:   "PreconditionFailed",
		},
		{
			name:      "if-match match",
			target:    "/bucket/key",
			header:    http.Header{"If-Match": {`"etag"`}},
			expStatus: http.StatusOK,
			expBody:   "0123456789",
		},
		{
			name:      "if-none-match match",
			target:    "/bucket/key",
			header:    http.Header{"If-None-Match": {"etag"}},
			expStatus: http.StatusNotModified,
			expHeader: map[string]string{"ETag": `"etag"`, "Last-Modified": lastModified},
		},
		{
			name:      "if-modified-since not modified",
			target:    "/bucket/key",
			header:    http.Header{"If-Modified-Since": {after}},
			expStatus: http.StatusNotModified,
		},
		{
			name:      "if-modified-since modified",
			target:    "/bucket/key",
			header:    http.Header{"If-Modified-Since": {before}},
			expStatus: http.StatusOK,
			expBody:   "0123456789",
		},
		{
			name:      "if-modified-since one second before",
			target:    "/bucket/key",
			header:    http.Header{"If-Modified-Since": {modTime.Add(-time.Second).Format(http.TimeFormat)}},
			expStatus: http.StatusOK,
			expBody:   "0123456789",
		},
		{
			name:      "if-modified-since equal",
			target:    "/bucket/key",
			header:    http.Header{"If-Modified-Since": {lastModified}},
			expStatus: http.StatusNotModified,
		},
		{
			name:      "if-unmodified-since one second before",
			target:    "/bucket/key",
			header:    http.Header{"If-Unmodified-Since": {modTime.Add(-time.Second).Format(http.TimeFormat)}},
			expStatus: http.StatusPreconditionFailed,
			expCode:   "PreconditionFailed",
		},
		{
			name:      "if-unmodified-since modified",
			target:    "/bucket/key",
			header:    http.Header{"If-Unmodified-Since": {before}},
			expStatus: http.StatusPreconditionFailed,
			expCode:   "PreconditionFailed",
		},
		{
			name:      "response header overrides",
			target:    "/bucket/key?response-content-type=application%2Fjson&response-cache-control=no-cache",
			expStatus: http.StatusOK,
			expBody:   "0123456789",
			expHeader: map[string]string{"Content-Type": "application/json", "Cache-Control": "no-cache"},
		},
		{
			name:      "response header overrides are case-sensitive",
			target:    "/bucket/key?Response-Content-Type=application%2Fjson",
			expStatus: http.StatusOK,
			expBody:   "0123456789",
			expHeader: map[string]string{"Content-Type": "text/plain"},
		},
		{
			name:      "encryption requested",
			target:    "/bucket/key",
			header:    http.Header{"X-Amz-Server-Side-Encryption": {"AES256"}},
			expStatus: http.StatusBadRequest,
			expCode:   "BadRequest",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			check := func(t *testing.T, resp response) {
				require.Equal(t, tt.expStatus, resp.StatusCode)
				for k, v := range tt.expHeader {
					require.Equal(t, v, resp.Header.Get(k), k)
				}
			}

			t.Run("GET", func(t *testing.T) {
				resp := serve(t, objectAPI, http.MethodGet, tt.target, tt.header, nil)
				body := resp.Body
				check(t, resp)
				if tt.expCode != "" {
					require.Contains(t, body, "<Code>"+tt.expCode+"</Code>")
				} else {
					require.Equal(t, tt.expBody, body)
				}
			})

			t.Run("HEAD", func(t *testing.T) {
				resp := serve(t, objectAPI, http.MethodHead, tt.target, tt.header, nil)
				check(t, resp)
				require.Empty(t, resp.Body)
			})
		})
	}
}

func TestGetObjectFirstReadFailure(t *testing.T) {
	objInfo := cmd.ObjectInfo{
		Bucket:          "bucket",
		Name:            "key",
		Size:            10,
		ETag:            "etag",
		ContentEncoding: "gzip",
		UserDefined:     map[string]string{"X-Amz-Meta-A": "1"},
	}
	objectAPI := &fakeObjectLayer{
		getObjectNInfo: func(_ context.Context, _, _ string, _ *cmd.HTTPRangeSpec, _ http.Header, _ cmd.LockType, opts cmd.ObjectOptions) (*cmd.GetObjectReader, error) {
			return cmd.NewGetObjectReaderFromReader(iotest.ErrReader(errors.New("read failed")), objInfo, opts)
		},
	}

	for name, header := range map[string]http.Header{
		"unranged": nil,
		"ranged":   {"Range": {"bytes=0-4"}},
	} {
		t.Run(name, func(t *testing.T) {
			resp := serve(t, objectAPI, http.MethodGet, "/bucket/key", header, nil)
			require.Equal(t, http.StatusInternalServerError, resp.StatusCode, resp.Body)
			require.Contains(t, resp.Body, "<Code>InternalError</Code>")
			for _, k := range []string{"Content-Encoding", "Content-Range", "ETag", "X-Amz-Meta-A"} {
				require.Empty(t, resp.Header.Get(k), k)
			}
		})
	}
}

func TestGetEmptyObject(t *testing.T) {
	objInfo := cmd.ObjectInfo{
		Bucket:      "bucket",
		Name:        "key",
		ETag:        "etag",
		ContentType: "text/plain",
		VersionID:   "version",
		UserDefined: map[string]string{"X-Amz-Meta-A": "1"},
	}
	objectAPI := singleObjectLayer(objInfo, "")

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			resp := serve(t, objectAPI, method, "/bucket/key", nil, nil)
			require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
			require.Empty(t, resp.Body)
			for k, v := range map[string]string{
				"ETag":             `"etag"`,
				"Content-Type":     "text/plain",
				"Content-Length":   "0",
				"X-Amz-Version-Id": "version",
				"X-Amz-Meta-A":     "1",
				// The object has no modification time.
				"Last-Modified": "",
			} {
				require.Equal(t, v, resp.Header.Get(k), k)
			}
		})
	}
}

func TestObjectMetadataHeaders(t *testing.T) {
	objInfo := cmd.ObjectInfo{
		Bucket: "bucket",
		Name:   "key",
		Size:   4,
		UserDefined: map[string]string{
			"Set-Cookie":              "session=1",
			"s3:etag":                 "etag",
			"s3:tags":                 "a=1&b=2",
			"X-Minio-Internal-Secret": "hidden",
			"X-Amz-Meta-A":            "1",
			"Cache-Control":           "no-cache",
			"x-amz-object-lock-mode":  "GOVERNANCE",
			"X-Amz-Storage-Class":     "STANDARD_IA",
		},
	}
	objectAPI := singleObjectLayer(objInfo, "data")

	check := func(t *testing.T, resp response) {
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		for k, v := range map[string]string{
			"X-Amz-Meta-A":            "1",
			"Cache-Control":           "no-cache",
			"X-Amz-Object-Lock-Mode":  "GOVERNANCE",
			"X-Amz-Storage-Class":     "STANDARD_IA",
			"X-Amz-Tagging-Count":     "2",
			"Content-Type":            "binary/octet-stream",
			"Set-Cookie":              "",
			"X-Minio-Internal-Secret": "",
		} {
			require.Equal(t, v, resp.Header.Get(k), k)
		}
	}

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			// A recorder sees header names that net/http would not send.
			resp := serve(t, objectAPI, method, "/bucket/key", nil, nil)
			check(t, resp)
			for k := range resp.Header {
				require.NotContains(t, k, ":", k)
			}
		})
	}
}

func TestDeleteObject(t *testing.T) {
	const versionID = "00000000-0000-0000-0000-000000000001"

	var gotOpts cmd.ObjectOptions
	objectAPI := &fakeObjectLayer{
		deleteObject: func(_ context.Context, bucket, object string, opts cmd.ObjectOptions) (cmd.ObjectInfo, error) {
			gotOpts = opts
			switch object {
			case "key":
				return cmd.ObjectInfo{Bucket: bucket, Name: object, VersionID: "marker", DeleteMarker: true}, nil
			case "missing":
				return cmd.ObjectInfo{}, apierr.CodeNoSuchKey
			default:
				return cmd.ObjectInfo{}, apierr.CodeAccessDenied
			}
		},
	}

	t.Run("delete marker", func(t *testing.T) {
		resp := serve(t, objectAPI, http.MethodDelete, "/bucket/key?versionId="+versionID,
			http.Header{"X-Amz-Bypass-Governance-Retention": {"true"}}, nil)
		require.Equal(t, http.StatusNoContent, resp.StatusCode, resp.Body)
		require.Equal(t, "marker", resp.Header.Get("X-Amz-Version-Id"))
		require.Equal(t, "true", resp.Header.Get("X-Amz-Delete-Marker"))
		require.Equal(t, versionID, gotOpts.VersionID)
		require.True(t, gotOpts.BypassGovernanceRetention)
	})

	t.Run("nonexistent key", func(t *testing.T) {
		resp := serve(t, objectAPI, http.MethodDelete, "/bucket/missing", nil, nil)
		require.Equal(t, http.StatusNoContent, resp.StatusCode, resp.Body)
		require.Empty(t, resp.Header.Get("X-Amz-Version-Id"))
	})

	t.Run("error", func(t *testing.T) {
		resp := serve(t, objectAPI, http.MethodDelete, "/bucket/other", nil, nil)
		require.Equal(t, http.StatusForbidden, resp.StatusCode)
		require.Contains(t, resp.Body, "<Code>AccessDenied</Code>")
	})

	t.Run("invalid version ID", func(t *testing.T) {
		resp := serve(t, objectAPI, http.MethodDelete, "/bucket/key?versionId=invalid", nil, nil)
		require.Contains(t, resp.Body, "<Code>NoSuchVersion</Code>")
	})
}

func TestDeleteObjectMissingStorageObject(t *testing.T) {
	for _, storageErr := range []error{
		cmd.ObjectNotFound{Bucket: "bucket", Object: "missing"},
		cmd.VersionNotFound{Bucket: "bucket", Object: "missing", VersionID: "00000000-0000-0000-0000-000000000001"},
	} {
		for _, wrapped := range []bool{false, true} {
			t.Run(fmt.Sprintf("%T/wrapped=%t", storageErr, wrapped), func(t *testing.T) {
				layer := &fakeObjectLayer{deleteObject: func(context.Context, string, string, cmd.ObjectOptions) (cmd.ObjectInfo, error) {
					if wrapped {
						return cmd.ObjectInfo{}, fmt.Errorf("delete: %w", storageErr)
					}
					return cmd.ObjectInfo{}, storageErr
				}}
				resp := serve(t, layer, http.MethodDelete, "/bucket/missing?versionId=00000000-0000-0000-0000-000000000001", nil, nil)
				require.Equal(t, http.StatusNoContent, resp.StatusCode, resp.Body)
				require.Empty(t, resp.Body)
			})
		}
	}
}

func TestDeleteObjectTagging(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		deleteObjectTags: func(_ context.Context, bucket, object string, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
			return cmd.ObjectInfo{Bucket: bucket, Name: object, VersionID: "version"}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodDelete, "/bucket/key?tagging", nil, nil)
	require.Equal(t, http.StatusNoContent, resp.StatusCode, resp.Body)
	require.Equal(t, "version", resp.Header.Get("X-Amz-Version-Id"))
}

func TestCopyObject(t *testing.T) {
	const versionID = "00000000-0000-0000-0000-000000000001"

	modTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	srcInfo := cmd.ObjectInfo{
		Bucket:   "bucket",
		Name:     "src",
		ModTime:  modTime,
		Size:     10,
		ETag:     "etag",
		UserTags: "a=1",
		UserDefined: map[string]string{
			"Content-Type":                 "text/plain",
			"X-Amz-Meta-Foo":               "bar",
			"X-Amz-Object-Lock-Mode":       "COMPLIANCE",
			"X-Amz-Object-Lock-Legal-Hold": "ON",
			"x-amz-object-lock-legal-hold": "ON",
			"X-Amz-Server-Side-Encryption": "AES256",
		},
	}

	type copyCall struct {
		srcBucket, srcObject, dstBucket, dstObject string
		srcInfo                                    cmd.ObjectInfo
		srcOpts, dstOpts                           cmd.ObjectOptions
	}

	setup := func() (*fakeObjectLayer, *copyCall) {
		objectAPI := singleObjectLayer(srcInfo, "0123456789")
		call := new(copyCall)
		objectAPI.copyObject = func(_ context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo cmd.ObjectInfo, srcOpts, dstOpts cmd.ObjectOptions) (cmd.ObjectInfo, error) {
			*call = copyCall{srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts}
			return cmd.ObjectInfo{Bucket: dstBucket, Name: dstObject, ETag: "newetag", ModTime: modTime, VersionID: "newversion"}, nil
		}
		return objectAPI, call
	}

	t.Run("copy", func(t *testing.T) {
		objectAPI, call := setup()
		resp := serve(t, objectAPI, http.MethodPut, "/dstbucket/dst", http.Header{
			"X-Amz-Copy-Source":            {"/bucket/src"},
			"X-Amz-Object-Lock-Legal-Hold": {"ON"},
		}, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, "<CopyObjectResult")
		require.Contains(t, resp.Body, "<LastModified>2026-01-02T03:04:05.000Z</LastModified><ETag>&#34;newetag&#34;</ETag>")
		require.Equal(t, `"newetag"`, resp.Header.Get("ETag"))
		require.Equal(t, "newversion", resp.Header.Get("X-Amz-Version-Id"))

		require.Equal(t, []string{"bucket", "src", "dstbucket", "dst"},
			[]string{call.srcBucket, call.srcObject, call.dstBucket, call.dstObject})
		require.Equal(t, map[string]string{
			"Content-Type":   "text/plain",
			"X-Amz-Meta-Foo": "bar",
			"X-Amz-Tagging":  "a=1",
		}, call.srcInfo.UserDefined)
		require.NotNil(t, call.dstOpts.LegalHold)
		require.EqualValues(t, "ON", *call.dstOpts.LegalHold)
	})

	t.Run("replace metadata and tags", func(t *testing.T) {
		objectAPI, call := setup()
		resp := serve(t, objectAPI, http.MethodPut, "/bucket/dst", http.Header{
			"X-Amz-Copy-Source":        {"/bucket/src"},
			"X-Amz-Metadata-Directive": {"REPLACE"},
			"X-Amz-Tagging-Directive":  {"REPLACE"},
			"X-Amz-Meta-New":           {"value"},
			"X-Amz-Tagging":            {"b=2"},
		}, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Equal(t, map[string]string{
			"Content-Type":            "binary/octet-stream",
			"X-Amz-Meta-New":          "value",
			"X-Amz-Tagging":           "b=2",
			"X-Amz-Tagging-Directive": "REPLACE",
		}, call.srcInfo.UserDefined)
	})

	t.Run("source version", func(t *testing.T) {
		objectAPI, call := setup()
		getObjectInfo := objectAPI.getObjectInfo
		objectAPI.getObjectInfo = func(ctx context.Context, bucket, object string, opts cmd.ObjectOptions) (cmd.ObjectInfo, error) {
			info, err := getObjectInfo(ctx, bucket, object, opts)
			info.VersionID = versionID
			return info, err
		}
		// The header reports the version copied, also when the request names none.
		for _, source := range []string{"/bucket/src", "/bucket/src?versionId=" + versionID} {
			resp := serve(t, objectAPI, http.MethodPut, "/dstbucket/dst", http.Header{
				"X-Amz-Copy-Source": {source},
			}, nil)
			require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
			require.Equal(t, "src", call.srcObject)
			require.Equal(t, versionID, resp.Header.Get("X-Amz-Copy-Source-Version-Id"), source)
		}
		require.Equal(t, versionID, call.srcOpts.VersionID)
	})

	for _, tt := range []struct {
		name      string
		header    http.Header
		expStatus int
		expCode   string
	}{
		{
			name:      "copy to itself",
			header:    http.Header{"X-Amz-Copy-Source": {"/bucket/src"}},
			expStatus: http.StatusBadRequest,
			expCode:   "InvalidRequest",
		},
		{
			name:      "invalid copy source",
			header:    http.Header{"X-Amz-Copy-Source": {"/bucket"}},
			expStatus: http.StatusBadRequest,
			expCode:   "InvalidArgument",
		},
		{
			name:      "invalid source version",
			header:    http.Header{"X-Amz-Copy-Source": {"/bucket/src?versionId=invalid"}},
			expStatus: http.StatusNotFound,
			expCode:   "NoSuchVersion",
		},
		{
			name:      "nonexistent source",
			header:    http.Header{"X-Amz-Copy-Source": {"/bucket/missing"}},
			expStatus: http.StatusNotFound,
			expCode:   "NoSuchKey",
		},
		{
			name:      "invalid metadata directive",
			header:    http.Header{"X-Amz-Copy-Source": {"/bucket/src"}, "X-Amz-Metadata-Directive": {"MOVE"}},
			expStatus: http.StatusBadRequest,
			expCode:   "InvalidArgument",
		},
		{
			name:      "precondition failed",
			header:    http.Header{"X-Amz-Copy-Source": {"/bucket/src"}, "X-Amz-Copy-Source-If-Match": {`"other"`}},
			expStatus: http.StatusPreconditionFailed,
			expCode:   "PreconditionFailed",
		},
		{
			name:      "encryption requested",
			header:    http.Header{"X-Amz-Copy-Source": {"/bucket/other"}, "X-Amz-Server-Side-Encryption": {"AES256"}},
			expStatus: http.StatusNotImplemented,
			expCode:   "NotImplemented",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			objectAPI, _ := setup()
			resp := serve(t, objectAPI, http.MethodPut, "/bucket/src", tt.header, nil)
			require.Equal(t, tt.expStatus, resp.StatusCode, resp.Body)
			require.Contains(t, resp.Body, "<Code>"+tt.expCode+"</Code>")
		})
	}
}

func TestCreateMultipartUpload(t *testing.T) {
	var gotOpts cmd.ObjectOptions
	objectAPI := &fakeObjectLayer{
		newMultipartUpload: func(_ context.Context, _, _ string, opts cmd.ObjectOptions) (string, error) {
			gotOpts = opts
			return "upload-id", nil
		},
	}

	resp := serve(t, objectAPI, http.MethodPost, "/bucket/key?uploads", http.Header{
		"X-Amz-Meta-Foo":               {"bar"},
		"X-Amz-Tagging":                {"a=1"},
		"X-Amz-Object-Lock-Legal-Hold": {"ON"},
	}, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<InitiateMultipartUploadResult")
	require.Contains(t, resp.Body, "<Bucket>bucket</Bucket><Key>key</Key><UploadId>upload-id</UploadId>")
	require.Equal(t, map[string]string{
		"Content-Type":   "binary/octet-stream",
		"X-Amz-Meta-Foo": "bar",
		"X-Amz-Tagging":  "a=1",
	}, gotOpts.UserDefined)
	require.NotNil(t, gotOpts.LegalHold)
	require.EqualValues(t, "ON", *gotOpts.LegalHold)

	resp = serve(t, objectAPI, http.MethodPost, "/bucket/key?uploads", http.Header{"X-Amz-Storage-Class": {"INVALID"}}, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>InvalidStorageClass</Code>")
}

func TestCompleteMultipartUpload(t *testing.T) {
	var gotParts []cmd.CompletePart
	objectAPI := &fakeObjectLayer{
		completeMultipartUpload: func(_ context.Context, bucket, object, uploadID string, parts []cmd.CompletePart, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
			if uploadID != "upload-id" {
				return cmd.ObjectInfo{}, apierr.CodeNoSuchUpload
			}
			gotParts = parts
			return cmd.ObjectInfo{Bucket: bucket, Name: object, ETag: "etag-2", VersionID: "version"}, nil
		},
	}

	complete := func(t *testing.T, uploadID, body string) response {
		return serve(t, objectAPI, http.MethodPost, "/bucket/key?uploadId="+uploadID, nil, []byte(body))
	}

	t.Run("success", func(t *testing.T) {
		resp := complete(t, "upload-id", `<CompleteMultipartUpload>`+
			`<Part><PartNumber>1</PartNumber><ETag>"a"</ETag></Part>`+
			`<Part><PartNumber>2</PartNumber><ETag>b</ETag></Part>`+
			`</CompleteMultipartUpload>`)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, "<CompleteMultipartUploadResult")
		require.Contains(t, resp.Body, "<Location>http://example.com/bucket/key</Location>")
		require.Contains(t, resp.Body, "<ETag>&#34;etag-2&#34;</ETag>")
		require.Equal(t, "application/xml", resp.Header.Get("Content-Type"))
		require.Equal(t, `"etag-2"`, resp.Header.Get("ETag"))
		require.Equal(t, "version", resp.Header.Get("X-Amz-Version-Id"))
		require.Equal(t, []cmd.CompletePart{{PartNumber: 1, ETag: "a"}, {PartNumber: 2, ETag: "b"}}, gotParts)
	})

	for _, tt := range []struct {
		name      string
		uploadID  string
		body      string
		expStatus int
		expCode   string
	}{
		{
			name:      "parts out of order",
			uploadID:  "upload-id",
			body:      `<CompleteMultipartUpload><Part><PartNumber>2</PartNumber></Part><Part><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>`,
			expStatus: http.StatusBadRequest,
			expCode:   "InvalidPartOrder",
		},
		{
			name:      "no parts",
			uploadID:  "upload-id",
			body:      `<CompleteMultipartUpload></CompleteMultipartUpload>`,
			expStatus: http.StatusBadRequest,
			expCode:   "MalformedXML",
		},
		{
			name:      "malformed XML",
			uploadID:  "upload-id",
			body:      `<CompleteMultipartUpload>`,
			expStatus: http.StatusBadRequest,
			expCode:   "MalformedXML",
		},
		{
			name:      "nonexistent upload",
			uploadID:  "other",
			body:      `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>`,
			expStatus: http.StatusNotFound,
			expCode:   "NoSuchUpload",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resp := complete(t, tt.uploadID, tt.body)
			require.Equal(t, tt.expStatus, resp.StatusCode, resp.Body)
			require.Contains(t, resp.Body, "<Code>"+tt.expCode+"</Code>")
		})
	}

	t.Run("keep-alive", func(t *testing.T) {
		api.SetCompleteMultipartUploadKeepAliveInterval(t, time.Millisecond)

		slowObjectAPI := &fakeObjectLayer{
			completeMultipartUpload: func(ctx context.Context, bucket, object, uploadID string, parts []cmd.CompletePart, opts cmd.ObjectOptions) (cmd.ObjectInfo, error) {
				time.Sleep(20 * time.Millisecond)
				return objectAPI.completeMultipartUpload(ctx, bucket, object, uploadID, parts, opts)
			},
		}
		body := []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>`)

		resp := serve(t, slowObjectAPI, http.MethodPost, "/bucket/key?uploadId=upload-id", nil, body)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.True(t, strings.HasPrefix(resp.Body, xml.Header+" "), resp.Body)
		require.Equal(t, 1, strings.Count(resp.Body, "<?xml"), resp.Body)
		require.Contains(t, resp.Body, "<CompleteMultipartUploadResult")

		// Errors are reported with a 200 OK status once whitespace has been sent.
		resp = serve(t, slowObjectAPI, http.MethodPost, "/bucket/key?uploadId=other", nil, body)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.True(t, strings.HasPrefix(resp.Body, xml.Header+" "), resp.Body)
		require.Equal(t, 1, strings.Count(resp.Body, "<?xml"), resp.Body)
		require.Contains(t, resp.Body, "<Error><Code>NoSuchUpload</Code>")
	})

	t.Run("keep-alive stops on panic", func(t *testing.T) {
		api.SetCompleteMultipartUploadKeepAliveInterval(t, time.Millisecond)

		panickingObjectAPI := &fakeObjectLayer{
			completeMultipartUpload: func(context.Context, string, string, string, []cmd.CompletePart, cmd.ObjectOptions) (cmd.ObjectInfo, error) {
				time.Sleep(5 * time.Millisecond)
				panic("object layer panic")
			},
		}
		router := mux.NewRouter()
		api.New(panickingObjectAPI, testCredentialsProvider{}, api.Config{}).RegisterHandlers(router)

		body := []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>a</ETag></Part></CompleteMultipartUpload>`)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/bucket/key?uploadId=upload-id", bytes.NewReader(body))
		signV4(req, body, time.Now())

		rec := httptest.NewRecorder()
		require.Panics(t, func() { router.ServeHTTP(rec, req) })

		// Nothing is written once the handler has returned.
		n := rec.Body.Len()
		time.Sleep(10 * time.Millisecond)
		require.Equal(t, n, rec.Body.Len())
	})

	t.Run("body too large", func(t *testing.T) {
		body := append([]byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>`), bytes.Repeat([]byte(" "), 5<<20)...)
		resp := serve(t, objectAPI, http.MethodPost, "/bucket/key?uploadId=upload-id", nil, body)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, "<Code>EntityTooLarge</Code>")
	})
}

func TestAbortMultipartUpload(t *testing.T) {
	var gotUploadID string
	objectAPI := &fakeObjectLayer{
		abortMultipartUpload: func(_ context.Context, _, _, uploadID string, _ cmd.ObjectOptions) error {
			gotUploadID = uploadID
			return nil
		},
	}

	resp := serve(t, objectAPI, http.MethodDelete, "/bucket/key?uploadId=upload-id", nil, nil)
	require.Equal(t, http.StatusNoContent, resp.StatusCode, resp.Body)
	require.Equal(t, "upload-id", gotUploadID)
}

func TestListParts(t *testing.T) {
	type call struct {
		uploadID                   string
		partNumberMarker, maxParts int
	}
	var got call
	objectAPI := &fakeObjectLayer{
		listObjectParts: func(_ context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, _ cmd.ObjectOptions) (cmd.ListPartsInfo, error) {
			got = call{uploadID, partNumberMarker, maxParts}
			return cmd.ListPartsInfo{
				Bucket:               bucket,
				Object:               object,
				UploadID:             uploadID,
				PartNumberMarker:     partNumberMarker,
				NextPartNumberMarker: 3,
				MaxParts:             maxParts,
				IsTruncated:          true,
				Parts: []cmd.PartInfo{{
					PartNumber:   3,
					LastModified: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
					ETag:         "etag",
					Size:         5,
				}},
			}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodGet, "/bucket/a%20key?uploadId=upload-id&part-number-marker=2&max-parts=1&encoding-type=url", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, call{"upload-id", 2, 1}, got)
	require.Contains(t, resp.Body, "<ListPartsResult")
	require.Contains(t, resp.Body, "<Bucket>bucket</Bucket><Key>a+key</Key><UploadId>upload-id</UploadId>")
	require.Contains(t, resp.Body, "<PartNumberMarker>2</PartNumberMarker><NextPartNumberMarker>3</NextPartNumberMarker><MaxParts>1</MaxParts><IsTruncated>true</IsTruncated>")
	require.Contains(t, resp.Body, "<Part><PartNumber>3</PartNumber><LastModified>2026-01-02T03:04:05.000Z</LastModified><ETag>&#34;etag&#34;</ETag><Size>5</Size></Part>")

	resp = serve(t, objectAPI, http.MethodGet, "/bucket/key?uploadId=upload-id", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Equal(t, call{"upload-id", 0, 1000}, got)

	resp = serve(t, objectAPI, http.MethodGet, "/bucket/key?uploadId=upload-id&max-parts=-1", nil, nil)
	require.Contains(t, resp.Body, "Argument max-parts must be an integer")

	resp = serve(t, objectAPI, http.MethodGet, "/bucket/key?uploadId=upload-id&part-number-marker=x", nil, nil)
	require.Contains(t, resp.Body, "Argument partNumberMarker must be an integer.")

	resp = serve(t, objectAPI, http.MethodGet, "/bucket/key?uploadId=upload-id&encoding-type=bogus", nil, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "Invalid Encoding Method specified in Request")
}

func TestListPartsEmptyETag(t *testing.T) {
	objectAPI := &fakeObjectLayer{
		listObjectParts: func(context.Context, string, string, string, int, int, cmd.ObjectOptions) (cmd.ListPartsInfo, error) {
			return cmd.ListPartsInfo{Parts: []cmd.PartInfo{{PartNumber: 1, Size: 5}}}, nil
		},
	}

	resp := serve(t, objectAPI, http.MethodGet, "/bucket/key?uploadId=upload-id", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<ETag></ETag>")
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

	resp = serve(t, objectAPI, http.MethodGet, "/bucket/key?uploadId=upload-id&part-number-marker=10000", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)

	for _, query := range []string{"max-parts=x", "max-parts=-1", "part-number-marker=-1", "part-number-marker=10001", "part-number-marker=4294967297"} {
		resp = serve(t, objectAPI, http.MethodGet, "/bucket/key?uploadId=upload-id&"+query, nil, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, query)
		require.Contains(t, resp.Body, "<Code>InvalidArgument</Code>", query)
	}
}

func TestSignatureVerification(t *testing.T) {
	router := newTestRouter(&fakeObjectLayer{})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	signV4(req, nil, time.Now())
	req.URL.RawQuery = "tampered"

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Contains(t, rec.Body.String(), "<Code>SignatureDoesNotMatch</Code>")

	// Keys with characters that Go leaves unescaped in paths are signed like the verifier expects.
	objectAPI := singleObjectLayer(cmd.ObjectInfo{Bucket: "bucket", Name: "other"}, "")
	resp := serve(t, objectAPI, http.MethodHead, "/bucket/a+=(),:;@!$'*~%20key", nil, nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestServeHTTP(t *testing.T) {
	newLayer := func() *fakeObjectLayer {
		return &fakeObjectLayer{
			completeMultipartUpload: func(_ context.Context, bucket, object, _ string, parts []cmd.CompletePart, _ cmd.ObjectOptions) (cmd.ObjectInfo, error) {
				if !slices.Equal([]cmd.CompletePart{{PartNumber: 1, ETag: "a"}}, parts) {
					return cmd.ObjectInfo{}, fmt.Errorf("unexpected parts %v", parts)
				}
				return cmd.ObjectInfo{Bucket: bucket, Name: object, ETag: "etag"}, nil
			},
		}
	}

	t.Run("request body", func(t *testing.T) {
		objectAPI := newLayer()
		body := []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>a</ETag></Part></CompleteMultipartUpload>`)
		resp := serveHTTP(t, objectAPI, http.MethodPost, "/bucket/key?uploadId=upload-id", nil, body)
		require.Equal(t, http.StatusOK, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, "<CompleteMultipartUploadResult")
		require.Equal(t, `"etag"`, resp.Header.Get("ETag"))
		require.Equal(t, []string{"CompleteMultipartUpload"}, objectAPI.Calls())
	})

	t.Run("rejected before storage", func(t *testing.T) {
		objectAPI := newLayer()
		body := []byte(`<CompleteMultipartUpload><Part><PartNumber>2</PartNumber></Part><Part><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>`)
		resp := serveHTTP(t, objectAPI, http.MethodPost, "/bucket/key?uploadId=upload-id", nil, body)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, "<Code>InvalidPartOrder</Code>")
		require.Empty(t, objectAPI.Calls())
	})

	for name, storageErr := range storageErrors(cmd.ObjectNotFound{Bucket: "bucket", Object: "missing"}) {
		t.Run("storage error/"+name, func(t *testing.T) {
			objectAPI := &fakeObjectLayer{err: storageErr}
			resp := serveHTTP(t, objectAPI, http.MethodDelete, "/bucket/missing", nil, nil)
			require.Equal(t, http.StatusNoContent, resp.StatusCode, resp.Body)
			require.Equal(t, []string{"DeleteObject"}, objectAPI.Calls())
		})
	}
}
