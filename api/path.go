// Copyright (C) 2026 Storj Labs, Inc.
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
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/gorilla/mux"

	"storj.io/minio/cmd"
)

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

func unescapePath(p string) (string, error) {
	ep, err := url.PathUnescape(p)
	if err != nil {
		return "", err
	}
	// S3 ignores the first leading slash of object keys provided in request URLs.
	return trimLeadingSlash(ep), nil
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

func splitCopySourcePath(path string) (bucketName, objectKey string) {
	path = strings.TrimPrefix(path, cmd.SlashSeparator)
	before, after, ok := strings.Cut(path, cmd.SlashSeparator)
	if !ok {
		return path, ""
	}
	return before, after
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
