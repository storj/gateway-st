// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/stretchr/testify/require"

	"storj.io/minio/cmd"
	"storj.io/minio/pkg/bucket/object/lock"
	"storj.io/minio/pkg/bucket/versioning"
	"storj.io/minio/pkg/event"
)

// bodyObjectLayer extends fakeObjectLayer with recorded, successful implementations of the
// methods called by the handlers that parse XML request bodies.
type bodyObjectLayer struct{ *fakeObjectLayer }

func (f bodyObjectLayer) GetBucketInfo(context.Context, string) (cmd.BucketInfo, error) {
	return cmd.BucketInfo{}, f.record("GetBucketInfo")
}

func (f bodyObjectLayer) MakeBucketWithLocation(context.Context, string, cmd.BucketOptions) error {
	return f.record("MakeBucketWithLocation")
}

func (f bodyObjectLayer) SetBucketNotificationConfig(context.Context, string, *event.Config) error {
	return f.record("SetBucketNotificationConfig")
}

func (f bodyObjectLayer) SetObjectLockConfig(context.Context, string, *lock.Config) error {
	return f.record("SetObjectLockConfig")
}

func (f bodyObjectLayer) SetBucketTagging(context.Context, string, *tags.Tags) error {
	return f.record("SetBucketTagging")
}

func (f bodyObjectLayer) SetBucketVersioning(context.Context, string, *versioning.Versioning) error {
	return f.record("SetBucketVersioning")
}

func (f bodyObjectLayer) SetObjectLegalHold(context.Context, string, string, string, *lock.ObjectLegalHold) error {
	return f.record("SetObjectLegalHold")
}

func (f bodyObjectLayer) SetObjectRetention(context.Context, string, string, string, cmd.ObjectOptions) error {
	return f.record("SetObjectRetention")
}

func (f bodyObjectLayer) PutObjectTags(context.Context, string, string, string, cmd.ObjectOptions) (cmd.ObjectInfo, error) {
	return cmd.ObjectInfo{}, f.record("PutObjectTags")
}

func newBodyObjectLayer() bodyObjectLayer {
	return bodyObjectLayer{&fakeObjectLayer{
		getObjectInfo: func(context.Context, string, string, cmd.ObjectOptions) (cmd.ObjectInfo, error) {
			return cmd.ObjectInfo{}, nil
		},
		completeMultipartUpload: func(context.Context, string, string, string, []cmd.CompletePart, cmd.ObjectOptions) (cmd.ObjectInfo, error) {
			return cmd.ObjectInfo{}, nil
		},
		deleteObjects: func(context.Context, string, []cmd.ObjectToDelete, cmd.ObjectOptions) ([]cmd.DeletedObject, []cmd.DeleteObjectsError, error) {
			return []cmd.DeletedObject{{}}, nil, nil
		},
	}}
}

func base64MD5(b []byte) string {
	sum := md5.Sum(b)
	return base64.StdEncoding.EncodeToString(sum[:])
}

func base64CRC32(b []byte) string {
	return base64.StdEncoding.EncodeToString(binary.BigEndian.AppendUint32(nil, crc32.ChecksumIEEE(b)))
}

const (
	aclBody      = `<AccessControlPolicy><AccessControlList><Grant><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>`
	completeBody = `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>a</ETag></Part></CompleteMultipartUpload>`
)

var xmlBodyHandlers = []struct {
	name, method, target, body string
	requireMD5                 bool // the operation requires a checksum header
	noCRC32                    bool // x-amz-checksum-* doesn't describe the body
}{
	{name: "PutObjectAcl", method: http.MethodPut, target: "/bucket/key?acl", body: aclBody},
	{name: "PutObjectLegalHold", method: http.MethodPut, target: "/bucket/key?legal-hold", body: `<LegalHold><Status>ON</Status></LegalHold>`, requireMD5: true},
	{name: "PutObjectRetention", method: http.MethodPut, target: "/bucket/key?retention", body: `<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2100-01-01T00:00:00Z</RetainUntilDate></Retention>`, requireMD5: true},
	{name: "PutObjectTagging", method: http.MethodPut, target: "/bucket/key?tagging", body: `<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>`},
	{name: "CreateBucket", method: http.MethodPut, target: "/bucket", body: `<CreateBucketConfiguration><LocationConstraint>us-east-1</LocationConstraint></CreateBucketConfiguration>`},
	{name: "PutBucketAcl", method: http.MethodPut, target: "/bucket?acl", body: aclBody},
	{name: "PutBucketNotificationConfiguration", method: http.MethodPut, target: "/bucket?notification", body: `<NotificationConfiguration></NotificationConfiguration>`},
	{name: "PutObjectLockConfiguration", method: http.MethodPut, target: "/bucket?object-lock", body: `<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>`},
	{name: "PutBucketTagging", method: http.MethodPut, target: "/bucket?tagging", body: `<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>`},
	{name: "PutBucketVersioning", method: http.MethodPut, target: "/bucket?versioning", body: `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`},
	{name: "CompleteMultipartUpload", method: http.MethodPost, target: "/bucket/key?uploadId=upload-id", body: completeBody, noCRC32: true},
	{name: "DeleteObjects", method: http.MethodPost, target: "/bucket?delete", body: `<Delete><Object><Key>key</Key></Object></Delete>`, requireMD5: true},
}

func TestXMLBodyVerification(t *testing.T) {
	for _, h := range xmlBodyHandlers {
		t.Run(h.name, func(t *testing.T) {
			body := []byte(h.body)
			tampered := []byte(strings.Replace(h.body, ">", "> ", 1))
			padded := append(bytes.Clone(body), bytes.Repeat([]byte(" "), 8<<10)...)

			header := func(kv ...string) http.Header {
				hdr := http.Header{}
				if h.requireMD5 {
					hdr.Set("Content-Md5", base64MD5(body))
				}
				for i := 0; i < len(kv); i += 2 {
					hdr.Set(kv[i], kv[i+1])
				}
				return hdr
			}

			t.Run("valid", func(t *testing.T) {
				objectAPI := newBodyObjectLayer()
				resp := serveHTTP(t, objectAPI, h.method, h.target, header(), body)
				require.Less(t, resp.StatusCode, 300, resp.Body)
				require.NotEmpty(t, objectAPI.Calls())
			})

			cases := []struct {
				name    string
				header  http.Header
				body    []byte
				expCode string
			}{
				{"content-md5", header("Content-Md5", base64MD5(tampered)), body, "BadDigest"},
				{"payload hash", header("X-Amz-Content-Sha256", hexSHA256(tampered)), body, "XAmzContentSHA256Mismatch"},
				{"padding", header("Content-Md5", base64MD5(body)), padded, "BadDigest"},
			}
			if !h.noCRC32 {
				cases = append(cases, struct {
					name    string
					header  http.Header
					body    []byte
					expCode string
				}{"checksum-crc32", header("X-Amz-Checksum-Crc32", base64CRC32(tampered)), body, "BadDigest"})
			}
			for _, tt := range cases {
				t.Run(tt.name, func(t *testing.T) {
					objectAPI := newBodyObjectLayer()
					resp := serveHTTP(t, objectAPI, h.method, h.target, tt.header, tt.body)
					require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
					require.Contains(t, resp.Body, "<Code>"+tt.expCode+"</Code>")
					require.Empty(t, objectAPI.Calls())
				})
			}
		})
	}
}

// endlessReader yields prefix followed by an unlimited amount of whitespace.
type endlessReader struct{ prefix []byte }

func (r *endlessReader) Read(p []byte) (int, error) {
	n := copy(p, r.prefix)
	r.prefix = r.prefix[n:]
	for i := n; i < len(p); i++ {
		p[i] = ' '
	}
	return len(p), nil
}

func TestXMLBodyLimits(t *testing.T) {
	t.Run("chunked", func(t *testing.T) {
		for _, h := range xmlBodyHandlers {
			if h.name == "CompleteMultipartUpload" || h.name == "DeleteObjects" {
				continue // an unknown length is rejected before reading
			}
			t.Run(h.name, func(t *testing.T) {
				objectAPI := newBodyObjectLayer()
				srv := httptest.NewServer(newTestRouter(objectAPI))
				t.Cleanup(srv.Close)

				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()
				req, err := http.NewRequestWithContext(ctx, h.method, srv.URL+h.target, &endlessReader{prefix: []byte(h.body)})
				require.NoError(t, err)
				req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
				if h.requireMD5 {
					req.Header.Set("Content-Md5", base64MD5([]byte(h.body)))
				}
				signV4(req, nil, time.Now())

				resp, err := srv.Client().Do(req)
				require.NoError(t, err)
				defer func() { _ = resp.Body.Close() }()
				respBody, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Equal(t, http.StatusBadRequest, resp.StatusCode, string(respBody))
				require.Contains(t, string(respBody), "<Code>EntityTooLarge</Code>")
				require.Empty(t, objectAPI.Calls())
			})
		}
	})

	t.Run("CompleteMultipartUpload over 5 MiB", func(t *testing.T) {
		objectAPI := newBodyObjectLayer()
		body := append([]byte(completeBody), bytes.Repeat([]byte(" "), 5<<20)...)
		resp := serveHTTP(t, objectAPI, http.MethodPost, "/bucket/key?uploadId=upload-id", nil, body)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
		require.Contains(t, resp.Body, "<Code>EntityTooLarge</Code>")
		require.Empty(t, objectAPI.Calls())
	})

	t.Run("empty body", func(t *testing.T) {
		for _, tt := range []struct {
			name, target string
			header       http.Header
		}{
			{"CompleteMultipartUpload", "/bucket/key?uploadId=upload-id", nil},
			{"DeleteObjects", "/bucket?delete", http.Header{"Content-Md5": {base64MD5(nil)}}},
		} {
			t.Run(tt.name, func(t *testing.T) {
				objectAPI := newBodyObjectLayer()
				resp := serveHTTP(t, objectAPI, http.MethodPost, tt.target, tt.header, nil)
				require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
				require.Contains(t, resp.Body, "<Code>MalformedXML</Code>")
				require.Empty(t, objectAPI.Calls())
			})
		}
	})
}

func TestXMLBodyTruncatedChunk(t *testing.T) {
	objectAPI := newBodyObjectLayer()
	// The chunk declares 0x40 bytes, but the body ends early.
	body := []byte("40\r\n<Tagging><TagSet></TagSet></Tagging>")
	resp := serve(t, objectAPI, http.MethodPut, "/bucket?tagging", http.Header{
		"X-Amz-Content-Sha256":         {"STREAMING-UNSIGNED-PAYLOAD-TRAILER"},
		"Content-Encoding":             {"aws-chunked"},
		"X-Amz-Decoded-Content-Length": {"64"},
		"X-Amz-Trailer":                {"x-amz-checksum-crc32"},
	}, body)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "<Code>IncompleteBody</Code>")
	require.Empty(t, objectAPI.Calls())
}
