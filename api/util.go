// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.
// This file incorporates code from MinIO Cloud Storage and includes changes made by Storj Labs, Inc.

/*
 * MinIO Cloud Storage, (C) 2015-2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package api

import (
	"encoding/base64"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/amwolff/awsig"
	"github.com/gorilla/mux"

	"storj.io/gateway/api/apierr"
	"storj.io/minio/cmd"
	xhttp "storj.io/minio/cmd/http"
)

const urlEncodingType = "url"

// nopCharsetConverter is an XML charset reader that performs no conversion.
// It is used to ignore the encoding that may be specified in the body of an S3 request.
func nopCharsetConverter(_ string, input io.Reader) (io.Reader, error) {
	return input, nil
}

func xmlDecoder(body io.Reader, v any, size int64) error {
	limitedBody := body
	if size > 0 {
		limitedBody = io.LimitReader(body, size)
	}
	d := xml.NewDecoder(limitedBody)
	d.CharsetReader = nopCharsetConverter
	return d.Decode(v)
}

func extractContentMD5(h http.Header) ([]byte, error) {
	values := h[xhttp.ContentMD5]
	if len(values) == 0 {
		return nil, nil
	}
	md5Str := values[0]
	if md5Str == "" {
		return nil, errors.New("Content-Md5 is empty")
	}
	// S3 uses strict decoding. It rejects Base64 strings where the unused padding bits aren't zero.
	md5, err := base64.StdEncoding.Strict().DecodeString(md5Str)
	if err != nil {
		return nil, errors.New("Content-Md5 is not a valid Base64 string")
	}
	if len(md5) != 16 {
		return nil, errors.New("Content-Md5 must be 16 bytes long")
	}
	return md5, nil
}

func getContentMD5ChecksumRequest(h http.Header) (checksumReq awsig.ChecksumRequest, present bool, err error) {
	md5, err := extractContentMD5(h)
	if err != nil {
		return awsig.ChecksumRequest{}, false, err
	}
	if md5 == nil {
		return awsig.ChecksumRequest{}, false, nil
	}
	req, err := awsig.NewChecksumRequest(awsig.AlgorithmMD5, base64.StdEncoding.EncodeToString(md5))
	if err != nil {
		return awsig.ChecksumRequest{}, true, err
	}
	return req, true, nil
}

// getResource returns "/bucketName/objectName" for path-style or virtual-hosted-style requests.
func getResource(r *http.Request) string {
	vHostBucket := getVirtualHostedBucket(r)
	if vHostBucket == "" {
		return r.URL.Path
	}
	return cmd.SlashSeparator + pathJoin(vHostBucket, r.URL.Path)
}

// pathJoin functions identically to path.Join but retains the trailing slash of the last element.
func pathJoin(elems ...string) string {
	trailingSlash := ""
	if len(elems) > 0 {
		if strings.HasSuffix(elems[len(elems)-1], cmd.SlashSeparator) {
			trailingSlash = cmd.SlashSeparator
		}
	}
	return path.Join(elems...) + trailingSlash
}

func pathClean(p string) string {
	cp := path.Clean(p)
	if cp == "." {
		return ""
	}
	return cp
}

func trimLeadingSlash(ep string) string {
	if len(ep) > 0 && ep[0] == '/' {
		keepTrailingSlash := strings.HasSuffix(ep, cmd.SlashSeparator) && len(ep) > 1
		ep = path.Clean(ep)
		if keepTrailingSlash {
			ep += cmd.SlashSeparator
		}
	}
	return ep
}

func shouldEscape(c byte) bool {
	if 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' {
		return false
	}

	switch c {
	case '-', '_', '.', '/', '*':
		return false
	}
	return true
}

// s3URLEncode is based on Golang's url.QueryEscape() code,
// while considering some S3 exceptions:
//   - Avoid encoding '/' and '*'
//   - Force encoding of '~'
func s3URLEncode(s string) string {
	spaceCount, hexCount := 0, 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if shouldEscape(c) {
			if c == ' ' {
				spaceCount++
			} else {
				hexCount++
			}
		}
	}

	if spaceCount == 0 && hexCount == 0 {
		return s
	}

	var buf [64]byte
	var t []byte

	required := len(s) + 2*hexCount
	if required <= len(buf) {
		t = buf[:required]
	} else {
		t = make([]byte, required)
	}

	if hexCount == 0 {
		copy(t, s)
		for i := 0; i < len(s); i++ {
			if s[i] == ' ' {
				t[i] = '+'
			}
		}
		return string(t)
	}

	j := 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == ' ':
			t[j] = '+'
			j++
		case shouldEscape(c):
			t[j] = '%'
			t[j+1] = "0123456789ABCDEF"[c>>4]
			t[j+2] = "0123456789ABCDEF"[c&15]
			j += 3
		default:
			t[j] = s[i]
			j++
		}
	}
	return string(t)
}

func s3EncodeName(name string, encodingType string) (result string) {
	if strings.ToLower(encodingType) == urlEncodingType {
		return s3URLEncode(name)
	}
	return name
}

// GetObjectURL gets the fully qualified URL of an object.
func GetObjectURL(r *http.Request, object string) string {
	scheme := strings.ToLower(r.Header.Get("X-Forwarded-Proto"))
	if scheme == "" {
		if r.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}

	var urlPath string
	if len(getVirtualHostedBucket(r)) != 0 {
		urlPath = path.Join(cmd.SlashSeparator, object)
	} else {
		bucket := mux.Vars(r)["bucket"]
		urlPath = path.Join(cmd.SlashSeparator, bucket, object)
	}

	return (&url.URL{
		Host:   r.Host,
		Path:   urlPath,
		Scheme: scheme,
	}).String()
}

type limitedAwsigReader struct {
	reader   awsig.Reader
	limit    int64
	n        int64
	limitErr error
}

// NewLimitedAwsigReader returns an awsig.Reader that limits the amount of bytes
// read from an awsig.Reader it wraps. It can be used in cases where there is a
// need to determine the specific reason why reading has stopped, either because
// a reader has truly run out of data to read or because there is more data to
// read but the limit has been reached. In the latter case,
// apierr.CodeEntityTooLarge is returned by Read.
func NewLimitedAwsigReader(r awsig.Reader, limit int64) awsig.Reader {
	return &limitedAwsigReader{
		reader: r,
		limit:  limit,
	}
}

// Read implements the awsig.Reader interface.
func (lr *limitedAwsigReader) Read(p []byte) (n int, err error) {
	if lr.limitErr != nil {
		return 0, lr.limitErr
	}

	remaining := lr.limit - lr.n
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}

	n, err = lr.reader.Read(p)
	lr.n += int64(n)

	if err != nil {
		lr.limitErr = err
		return n, err
	}

	if lr.n < lr.limit {
		return n, nil
	}

	var buf [1]byte
	probeN, probeErr := lr.reader.Read(buf[:])
	if probeN > 0 {
		lr.limitErr = apierr.CodeEntityTooLarge
	}
	if probeErr != nil {
		lr.limitErr = probeErr
	}

	return n, lr.limitErr
}

// Checksums implements te awsig.Reader interface.
func (lr *limitedAwsigReader) Checksums() (map[awsig.ChecksumAlgorithm][]byte, error) {
	return lr.reader.Checksums()
}
