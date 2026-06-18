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
	"strconv"
	"strings"

	"github.com/amwolff/awsig"

	"storj.io/common/uuid"
	"storj.io/gateway/api/apierr"
	"storj.io/minio/cmd"
	xhttp "storj.io/minio/cmd/http"
)

const (
	urlEncodingType = "url"
	byteRangePrefix = "bytes="
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

func extractVersionID(v url.Values) (string, error) {
	versionID := strings.TrimSpace(v.Get(xhttp.VersionID))
	if versionID != "" && versionID != nullVersionID {
		if _, err := uuid.FromString(versionID); err != nil {
			return "", apierr.CodeNoSuchVersion
		}
	}
	return versionID, nil
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

func s3EncodeName(name string, encodingType string) (result string) {
	if strings.ToLower(encodingType) == urlEncodingType {
		return s3URLEncode(name)
	}
	return name
}

func isStreamingSigV4(r *http.Request) bool {
	if strings.HasPrefix(r.Header.Get(xhttp.Authorization), "AWS4-") {
		switch r.Header.Get(xhttp.AmzContentSha256) {
		case "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
			"STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
			"STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER":
			return true
		}
	}
	return false
}

// ParseRange parses an HTTP range string as an HTTPRangeSpec. It returns apierr.CodeMalformedCopySourceRange
// if the range string is malformed and apierr.CodeInvalidCopySourceRange if it is well-formed but
// semantically invalid.
func ParseRange(rangeString string) (*cmd.HTTPRangeSpec, error) {
	byteRangeStr, ok := strings.CutPrefix(rangeString, byteRangePrefix)
	if !ok {
		return nil, apierr.CodeMalformedCopySourceRange
	}

	beginOffsetStr, endOffsetStr, ok := strings.Cut(byteRangeStr, "-")
	if !ok {
		return nil, apierr.CodeMalformedCopySourceRange
	}

	beginOffset, hasBeginOffset, err := parseRangeOffset(beginOffsetStr)
	if err != nil {
		return nil, apierr.CodeMalformedCopySourceRange
	}

	endOffset, hasEndOffset, err := parseRangeOffset(endOffsetStr)
	if err != nil {
		return nil, apierr.CodeMalformedCopySourceRange
	}

	switch {
	case hasBeginOffset && hasEndOffset:
		if beginOffset > endOffset {
			return nil, apierr.CodeInvalidCopySourceRange
		}
		return &cmd.HTTPRangeSpec{
			IsSuffixLength: false,
			Start:          beginOffset,
			End:            endOffset,
		}, nil
	case hasBeginOffset:
		return &cmd.HTTPRangeSpec{
			IsSuffixLength: false,
			Start:          beginOffset,
			End:            -1,
		}, nil
	case hasEndOffset:
		if endOffset == 0 {
			return nil, apierr.CodeInvalidCopySourceRange
		}
		return &cmd.HTTPRangeSpec{
			IsSuffixLength: true,
			Start:          -endOffset,
			End:            -1,
		}, nil
	default:
		return nil, apierr.CodeMalformedCopySourceRange
	}
}

func parseRangeOffset(s string) (offset int64, present bool, err error) {
	if s == "" {
		return 0, false, nil
	}
	if s[0] == '+' || s[0] == '-' {
		return 0, false, apierr.CodeMalformedCopySourceRange
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, false, apierr.CodeMalformedCopySourceRange
	}
	return n, true, nil
}

// parseRangeForCopy parses an HTTP range string as an HTTPRangeSpec. It functions identically to
// parseRange with the exception of returning apierr.CodeInvalidCopySourceRange for range strings
// that are not of the form "bytes=first-last", where first and last are zero-based offsets.
func parseRangeForCopy(rangeStr string) (rangeSpec *cmd.HTTPRangeSpec, err error) {
	rangeSpec, err = ParseRange(rangeStr)
	if err != nil {
		return nil, err
	}
	if rangeSpec.IsSuffixLength || rangeSpec.Start < 0 || rangeSpec.End < 0 {
		return nil, apierr.CodeInvalidCopySourceRange
	}
	return rangeSpec, nil
}

func (api *API) verifyWithBody(r *http.Request, requireContentMD5 bool) (awsig.Reader, error) {
	vr, err := api.verifier.Verify(r, getVirtualHostedBucket(r))
	if err != nil {
		return nil, err
	}

	var checksumReqs []awsig.ChecksumRequest
	checksumReq, hasContentMD5, err := getContentMD5ChecksumRequest(r.Header)
	if err != nil {
		return nil, err
	}

	if hasContentMD5 {
		checksumReqs = append(checksumReqs, checksumReq)
	} else if requireContentMD5 {
		return nil, apierr.CodeMissingContentMD5
	}

	body, err := vr.Reader(checksumReqs...)
	if err != nil {
		return nil, err
	}

	return body, nil
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
