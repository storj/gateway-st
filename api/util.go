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
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/amwolff/awsig"
	"github.com/minio/minio-go/v7/pkg/tags"

	"storj.io/gateway/api/apierr"
	xhttp "storj.io/minio/cmd/http"
	objectlock "storj.io/minio/pkg/bucket/object/lock"
)

const urlEncodingType = "url"

// nopCharsetConverter is an XML charset reader that performs no conversion.
// It is used to ignore the encoding that may be specified in the body of an S3 request.
func nopCharsetConverter(_ string, input io.Reader) (io.Reader, error) {
	return input, nil
}

func xmlDecoder(body io.Reader, v any) error {
	d := xml.NewDecoder(body)
	d.CharsetReader = nopCharsetConverter
	return d.Decode(v)
}

func extractVersionID(v url.Values) (string, error) {
	if !v.Has(xhttp.VersionID) {
		return "", nil
	}
	versionID := v.Get(xhttp.VersionID)
	if versionID == "" {
		return "", apierr.CodeEmptyVersionID
	}
	if err := validateVersionID(versionID); err != nil {
		return "", err
	}
	return versionID, nil
}

// validateVersionID checks that versionID is a version ID the object layer can address: hex that
// decodes to a 16-byte stream version ID.
//
// TODO: "null" addresses the object without a version ID, which the object layer can't address.
// Its latest version is only the same in buckets that were never versioned, so "null" is
// rejected until the object layer can address it.
func validateVersionID(versionID string) error {
	if versionID == nullVersionID {
		return apierr.CodeNotImplemented
	}
	if b, err := hex.DecodeString(versionID); err != nil || len(b) != 16 {
		return apierr.CodeInvalidVersionID
	}
	return nil
}

func extractContentMD5(h http.Header) ([]byte, error) {
	values := h[xhttp.ContentMD5]
	if len(values) == 0 {
		return nil, nil
	}
	md5Str := values[0]
	if md5Str == "" {
		return nil, apierr.CodeInvalidContentMD5
	}
	// S3 uses strict decoding. It rejects Base64 strings where the unused padding bits aren't zero.
	md5, err := base64.StdEncoding.Strict().DecodeString(md5Str)
	if err != nil {
		return nil, apierr.CodeInvalidContentMD5
	}
	if len(md5) != 16 {
		return nil, apierr.CodeInvalidContentMD5
	}
	return md5, nil
}

// validateTaggingHeader validates the value of the x-amz-tagging header.
func validateTaggingHeader(value string) error {
	_, err := tags.ParseObjectTags(value)
	if err != nil && !errors.As(err, new(tags.Error)) {
		// The value isn't a valid URL query.
		return apierr.CodeInvalidTaggingHeader
	}
	return err
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
		return awsig.ChecksumRequest{}, true, apierr.CodeInvalidContentMD5
	}
	return req, true, nil
}

// checksumHeaders maps the X-Amz-Checksum-<algorithm> headers to the algorithms they carry.
var checksumHeaders = map[string]awsig.ChecksumAlgorithm{
	"X-Amz-Checksum-Crc32":     awsig.AlgorithmCRC32,
	"X-Amz-Checksum-Crc32c":    awsig.AlgorithmCRC32C,
	"X-Amz-Checksum-Crc64nvme": awsig.AlgorithmCRC64NVME,
	"X-Amz-Checksum-Sha1":      awsig.AlgorithmSHA1,
	"X-Amz-Checksum-Sha256":    awsig.AlgorithmSHA256,
}

// getChecksumRequests returns the checksum requests for verifying a request body: its
// Content-MD5 header, and at most one X-Amz-Checksum-<algorithm> header or X-Amz-Trailer.
func getChecksumRequests(h http.Header) ([]awsig.ChecksumRequest, error) {
	var reqs []awsig.ChecksumRequest
	req, present, err := getContentMD5ChecksumRequest(h)
	if err != nil {
		return nil, err
	}
	if present {
		reqs = append(reqs, req)
	}

	var checksums int
	for header, algorithm := range checksumHeaders {
		value := h.Get(header)
		if value == "" {
			continue
		}
		req, err := awsig.NewChecksumRequest(algorithm, value)
		if err != nil {
			return nil, apierr.CodeInvalidRequest
		}
		reqs = append(reqs, req)
		checksums++
	}

	// A trailing checksum needs a streaming payload with trailers, and such a payload must
	// declare its trailer.
	trailer := h.Get("X-Amz-Trailer")
	if (trailer != "") != strings.HasSuffix(h.Get("X-Amz-Content-Sha256"), "-TRAILER") {
		return nil, apierr.CodeInvalidRequest
	}
	if trailer != "" {
		algorithm, ok := checksumHeaders[http.CanonicalHeaderKey(strings.TrimSpace(trailer))]
		if !ok {
			return nil, apierr.CodeInvalidRequest
		}
		req, err := awsig.NewTrailingChecksumRequest(algorithm)
		if err != nil {
			return nil, apierr.CodeInvalidRequest
		}
		reqs = append(reqs, req)
		checksums++
	}

	// S3 rejects requests that specify more than one checksum algorithm.
	if checksums > 1 {
		return nil, apierr.CodeInvalidRequest
	}

	return reqs, nil
}

// setChecksumHeaders sets the X-Amz-Checksum-<algorithm> response headers for the checksums
// that the request header h asked to verify while reading a request body. The SHA-256 of a
// signed payload isn't echoed unless it was asked for.
func setChecksumHeaders(w http.ResponseWriter, h http.Header, body awsig.Reader) {
	sums, err := body.Checksums()
	if err != nil {
		return
	}
	trailer := http.CanonicalHeaderKey(strings.TrimSpace(h.Get("X-Amz-Trailer")))
	for header, algorithm := range checksumHeaders {
		if _, requested := h[header]; !requested && header != trailer {
			continue
		}
		if sum, ok := sums[algorithm]; ok {
			w.Header().Set(header, base64.StdEncoding.EncodeToString(sum))
		}
	}
}

func s3EncodeName(name string, encodingType string) (result string) {
	if strings.ToLower(encodingType) == urlEncodingType {
		return s3URLEncode(name)
	}
	return name
}

func parseObjectLockHeaders(h http.Header, bucket, object string) (retMode objectlock.RetMode, retDate objectlock.RetentionDate, legalHold objectlock.ObjectLegalHold, err error) {
	retentionRequested := objectlock.IsObjectLockRetentionRequested(h)
	legalHoldRequested := objectlock.IsObjectLockLegalHoldRequested(h)

	if retentionRequested {
		if retMode, retDate, err = objectlock.ParseObjectLockRetentionHeaders(h); err != nil {
			return objectlock.RetMode(""), objectlock.RetentionDate{}, objectlock.ObjectLegalHold{}, err
		}
	}

	if legalHoldRequested {
		if legalHold, err = objectlock.ParseObjectLockLegalHoldHeaders(h); err != nil {
			return objectlock.RetMode(""), objectlock.RetentionDate{}, objectlock.ObjectLegalHold{}, err
		}
	}

	return retMode, retDate, legalHold, nil
}
