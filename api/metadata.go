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
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/amwolff/awsig"

	xhttp "storj.io/minio/cmd/http"
)

const (
	amzStorageClass          = "X-Amz-Storage-Class"
	streamingContentEncoding = "aws-chunked"
)

var metadataHeaders = []string{
	"Cache-Control",
	"Content-Encoding",
	"Content-Disposition",
	"Content-Language",
	"Content-Type",
	"Expires",
	amzStorageClass,
}

// userMetadataKeyPrefixes contains the prefixes of user-defined metadata keys.
// All values stored with a key starting with one of the following prefixes
// must be extracted from the header.
var userMetadataKeyPrefixes = []string{
	"X-Amz-Meta-",
	"X-Minio-Meta-",
}

func extractMetadata(ctx context.Context, r *http.Request) (metadata map[string]string, err error) {
	metadata = make(map[string]string)

	ExtractMetadataFromQuery(r.URL, metadata)
	ExtractMetadataFromHeader(r.Header, metadata)

	if _, ok := metadata[xhttp.ContentType]; !ok {
		metadata[xhttp.ContentType] = "binary/octet-stream"
	}

	// https://github.com/google/security-research/security/advisories/GHSA-76wf-9vgp-pj7w
	for k := range metadata {
		if strings.EqualFold(k, xhttp.AmzMetaUnencryptedContentLength) || strings.EqualFold(k, xhttp.AmzMetaUnencryptedContentMD5) {
			delete(metadata, k)
		}
	}

	if contentEncoding, ok := metadata[xhttp.ContentEncoding]; ok {
		contentEncoding = removeChunkedContentEncoding(contentEncoding)
		if contentEncoding != "" {
			metadata[xhttp.ContentEncoding] = contentEncoding
		} else {
			delete(metadata, xhttp.ContentEncoding)
		}
	}

	return metadata, nil
}

// ExtractMetadataFromQuery extracts object metadata from a URL's query string.
func ExtractMetadataFromQuery(reqURL *url.URL, metadata map[string]string) {
	// S3 only extracts the values of query parameters whose key matches the casing of the last
	// instance of that key. Those values are then joined with a comma. For example:
	//
	// - "x-amz-meta-foo=bar&x-amz-meta-foo=baz" produces the value "bar,baz".
	// - "x-amz-meta-FOO=bar&x-amz-meta-foo=baz" produces the value "baz".
	// - "x-amz-meta-FOO=bar&x-amz-meta-foo=baz&x-amz-meta-FOO=quux" produces the value
	//   "bar,quux". "baz" is ignored because its key, "x-amz-meta-foo", does not match
	//   the casing of "x-amz-meta-FOO".

	queryKeys := QueryKeys(reqURL.RawQuery)
	queryValues, _ := url.ParseQuery(reqURL.RawQuery)

	// canonicalKeyToExact maps the canonical form of a query parameter key to
	// its last seen exact casing.
	canonicalKeyToExact := make(map[string]string)
	// exactKeyValues maps a query parameter key's last seen exact casing to a slice
	// containing the values associated with that casing.
	exactKeyValues := make(map[string][]string)

	// TODO: This strategy is unreliable. Non-presigned requests may have these query parameters set.
	// This information should be provided to us by awsig.
	isPresigned := queryValues.Has(xhttp.AmzAlgorithm) || queryValues.Has(xhttp.AmzAccessKeyID)

	trackKey := func(key, canonKey string) {
		canonicalKeyToExact[canonKey] = key
		if _, seen := exactKeyValues[key]; !seen {
			exactKeyValues[key] = queryValues[key]
		}
	}

	for _, key := range queryKeys {
		canonKey := http.CanonicalHeaderKey(key)

		var tracked bool
		if isPresigned {
			for _, metadataHeaderKey := range metadataHeaders {
				if canonKey == metadataHeaderKey {
					trackKey(key, canonKey)
					tracked = true
				}
			}
		} else if canonKey == amzStorageClass {
			trackKey(key, canonKey)
			tracked = true
		}

		if tracked {
			continue
		}

		for _, prefix := range userMetadataKeyPrefixes {
			if strings.HasPrefix(canonKey, prefix) {
				trackKey(key, canonKey)
				break
			}
		}
	}

	for canonKey, exactKey := range canonicalKeyToExact {
		values := exactKeyValues[exactKey]
		if len(values) > 0 {
			metadata[canonKey] = strings.Join(values, ",")
		}
	}
}

// QueryKeys returns a slice of query parameter keys in the order that they appear in the query string.
// Invalid query parameters (those that would be skipped by (*url.URL).Query()) are silently ignored.
func QueryKeys(rawQuery string) []string {
	parts := strings.Split(rawQuery, "&")
	keys := make([]string, 0, len(parts))

	for _, part := range parts {
		if part == "" {
			continue
		}
		rawKey, rawValue, _ := strings.Cut(part, "=")
		if strings.Contains(rawKey, ";") {
			continue
		}
		key, err := url.QueryUnescape(rawKey)
		if err != nil {
			continue
		}
		if _, err = url.QueryUnescape(rawValue); err != nil {
			continue
		}
		keys = append(keys, key)
	}

	return keys
}

// ExtractMetadataFromHeader extracts object metadata from an HTTP header. The keys of the header
// are expected to be canonicalized.
func ExtractMetadataFromHeader(h http.Header, metadata map[string]string) {
	for _, key := range metadataHeaders {
		if values, ok := h[key]; ok {
			metadata[key] = strings.Join(values, ",")
		}
	}
	for key, values := range h {
		for _, prefix := range userMetadataKeyPrefixes {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			metadata[key] = strings.Join(values, ",")
		}
	}
}

func removeChunkedContentEncoding(contentEncoding string) string {
	var builder strings.Builder
	builder.Grow(len(contentEncoding))

	var written bool
	for enc := range strings.SplitSeq(contentEncoding, ",") {
		if !strings.EqualFold(streamingContentEncoding, strings.TrimSpace(enc)) {
			if written {
				builder.WriteByte(',')
			} else {
				written = true
			}
			builder.WriteString(enc)
		}
	}

	return builder.String()
}

// ExtractMetadataFromPostForm extracts object metadata from a POST form.
func ExtractMetadataFromPostForm(postForm awsig.PostForm) (map[string]string, error) {
	metadata := make(map[string]string, len(postForm))
	for _, key := range metadataHeaders {
		if !postForm.Has(key) {
			continue
		}
		if _, ok := metadata[key]; ok {
			return nil, fmt.Errorf("POST form contains duplicate case-insensitive field %q", key)
		}
		metadata[key] = getPostFormValue(postForm, key)
	}

	for key, elems := range postForm {
		key = http.CanonicalHeaderKey(key)
		for _, prefix := range userMetadataKeyPrefixes {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			if _, ok := metadata[key]; ok {
				return nil, fmt.Errorf("POST form contains duplicate case-insensitive field %q", key)
			}
			values := make([]string, 0, len(elems))
			for _, elem := range elems {
				values = append(values, elem.Value)
			}
			metadata[key] = strings.Join(values, ",")
		}
	}

	return metadata, nil
}
