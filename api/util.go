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
	"path"

	"github.com/amwolff/awsig"

	xhttp "storj.io/minio/cmd/http"
)

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

func getChecksumRequests(h http.Header) ([]awsig.ChecksumRequest, error) {
	md5, err := extractContentMD5(h)
	if err != nil {
		return nil, err
	}
	if md5 == nil {
		return nil, nil
	}
	var checksumReqs []awsig.ChecksumRequest
	req, err := awsig.NewChecksumRequest(awsig.AlgorithmMD5, base64.StdEncoding.EncodeToString(md5))
	if err != nil {
		return nil, err
	}
	checksumReqs = append(checksumReqs, req)
	return checksumReqs, nil
}

func pathClean(p string) string {
	cp := path.Clean(p)
	if cp == "." {
		return ""
	}
	return cp
}
