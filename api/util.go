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
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"hash/crc32"
	"hash/crc64"
	"io"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"

	"github.com/amwolff/awsig"

	"storj.io/gateway/api/apierr"
	xhttp "storj.io/minio/cmd/http"
	objectlock "storj.io/minio/pkg/bucket/object/lock"
	"storj.io/minio/pkg/tags"
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

// tagError is a tag validation error. It implements tags.Error, so it's reported like one.
type tagError struct {
	code    string
	message string
}

func (err tagError) Code() string  { return err.code }
func (err tagError) Error() string { return err.message }

var (
	errTooManyObjectTags = tagError{"BadRequest", "Object tags cannot be greater than 10"}
	errInvalidTagKey     = tagError{"InvalidTag", "The TagKey you have provided is invalid"}
	errInvalidTagValue   = tagError{"InvalidTag", "The TagValue you have provided is invalid"}
	errAWSObjectTagKey   = tagError{"InvalidTag", "Your TagKey cannot be prefixed with aws:"}
	errAWSBucketTagKey   = tagError{"InvalidTag", "System tags cannot be added/updated by requester"}

	// tagCharsRegexp matches the characters that S3 allows in tag keys and values.
	tagCharsRegexp = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-@]*$`)
)

// validateTaggingHeader validates the value of the x-amz-tagging header.
func validateTaggingHeader(header string) error {
	// url.ParseQuery merges duplicate keys, so the pairs are split manually.
	tagMap := make(map[string]string)
	for _, pair := range strings.Split(header, "&") {
		if pair == "" {
			continue
		}
		rawKey, rawValue, _ := strings.Cut(pair, "=")
		key, keyErr := url.QueryUnescape(rawKey)
		value, valueErr := url.QueryUnescape(rawValue)
		if _, dup := tagMap[key]; dup || keyErr != nil || valueErr != nil || strings.Contains(pair, ";") {
			return apierr.CodeInvalidTaggingHeader
		}
		tagMap[key] = value
	}
	if len(tagMap) > 10 {
		return errTooManyObjectTags
	}
	if _, err := tags.MapToObjectTags(tagMap); err != nil {
		return err
	}
	return validateTags(tagMap, true)
}

// parseTaggingXML parses a Tagging XML document and validates its tags.
func parseTaggingXML(r io.Reader, isObject bool) (*tags.Tags, error) {
	parse := tags.ParseBucketXML
	if isObject {
		parse = tags.ParseObjectXML
	}
	t, err := parse(r)
	if err != nil {
		// Too many tags is the only BadRequest that minio-go returns.
		if tagsErr := tags.Error(nil); isObject && errors.As(err, &tagsErr) && tagsErr.Code() == "BadRequest" {
			return nil, errTooManyObjectTags
		}
		return nil, err
	}
	return t, validateTags(t.ToMap(), isObject)
}

// validateTags checks the tag rules that minio-go doesn't: the reserved aws: prefix and allowed characters.
func validateTags(tagMap map[string]string, isObject bool) error {
	for key, value := range tagMap {
		if strings.HasPrefix(key, "aws:") {
			if isObject {
				return errAWSObjectTagKey
			}
			return errAWSBucketTagKey
		}
		if !tagCharsRegexp.MatchString(key) {
			return errInvalidTagKey
		}
		if !tagCharsRegexp.MatchString(value) {
			return errInvalidTagValue
		}
	}
	return nil
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
	"X-Amz-Checksum-Sha512":    awsig.AlgorithmSHA512,
	"X-Amz-Checksum-Xxhash64":  awsig.AlgorithmXXHASH64,
	"X-Amz-Checksum-Xxhash3":   awsig.AlgorithmXXHASH3,
	"X-Amz-Checksum-Xxhash128": awsig.AlgorithmXXHASH128,
}

// unimplementedChecksumHeaders are X-Amz-Checksum-<algorithm> headers of algorithms that awsig
// can't verify. They're rejected with NotImplemented rather than InvalidChecksumAlgorithm, which
// S3 may answer instead, so that a client sending one sees why its request fails.
var unimplementedChecksumHeaders = []string{
	"X-Amz-Checksum-Md5",
}

// checksumHeaderAlgorithm returns the algorithm of an X-Amz-Checksum-<algorithm> header.
func checksumHeaderAlgorithm(header string) (awsig.ChecksumAlgorithm, error) {
	header = http.CanonicalHeaderKey(header)
	if algorithm, ok := checksumHeaders[header]; ok {
		return algorithm, nil
	}
	if slices.Contains(unimplementedChecksumHeaders, header) {
		return 0, apierr.CodeChecksumsUnsupported
	}
	return 0, apierr.CodeInvalidChecksumAlgorithm
}

// checksumSize returns the decoded size of a checksum.
func checksumSize(algorithm awsig.ChecksumAlgorithm) int {
	switch algorithm {
	case awsig.AlgorithmCRC32, awsig.AlgorithmCRC32C:
		return crc32.Size
	case awsig.AlgorithmCRC64NVME:
		return crc64.Size
	case awsig.AlgorithmSHA1:
		return sha1.Size
	case awsig.AlgorithmSHA256:
		return sha256.Size
	case awsig.AlgorithmSHA512:
		return sha512.Size
	case awsig.AlgorithmXXHASH64, awsig.AlgorithmXXHASH3:
		return 8
	case awsig.AlgorithmXXHASH128:
		return 16
	default:
		return 0
	}
}

// checksumValueHeaders returns the X-Amz-Checksum-<algorithm> headers of h, sorted, and
// the algorithms they carry. It rejects unknown and unsupported algorithms.
func checksumValueHeaders(h http.Header) (headers []string, algorithms []awsig.ChecksumAlgorithm, err error) {
	for _, header := range slices.Sorted(maps.Keys(h)) {
		switch canonical := http.CanonicalHeaderKey(header); {
		case !strings.HasPrefix(canonical, xAmzChecksumPrefix),
			canonical == "X-Amz-Checksum-Type",
			canonical == "X-Amz-Checksum-Algorithm",
			canonical == "X-Amz-Checksum-Mode":
			continue
		}
		algorithm, err := checksumHeaderAlgorithm(header)
		if err != nil {
			return nil, nil, err
		}
		headers = append(headers, header)
		algorithms = append(algorithms, algorithm)
	}
	return headers, algorithms, nil
}

// checkMultipartChecksumHeaders checks the X-Amz-Checksum-Algorithm and X-Amz-Checksum-Type
// headers of a request that creates a multipart upload. Only CRC checksums can be of the whole
// object, and CRC64NVME checksums can only be.
//
// The requested checksums aren't returned by the later requests of the upload, because the object
// layer doesn't store checksums.
func checkMultipartChecksumHeaders(h http.Header) error {
	invalid := func(description string) error {
		return apierr.Response{Code: "InvalidRequest", Description: description, HTTPStatusCode: http.StatusBadRequest}
	}

	name, checksumType := h.Get("X-Amz-Checksum-Algorithm"), h.Get("X-Amz-Checksum-Type")
	if name == "" {
		if checksumType != "" {
			return invalid("The x-amz-checksum-type header can only be used with the x-amz-checksum-algorithm header.")
		}
		return nil
	}
	algorithm, err := checksumHeaderAlgorithm(xAmzChecksumPrefix + name)
	switch {
	case errors.Is(err, apierr.CodeChecksumsUnsupported):
		return err
	case err != nil:
		return invalid("Checksum algorithm provided is unsupported.")
	}

	isCRC := algorithm == awsig.AlgorithmCRC32 || algorithm == awsig.AlgorithmCRC32C || algorithm == awsig.AlgorithmCRC64NVME
	switch checksumType {
	case "":
	case "COMPOSITE":
		if algorithm == awsig.AlgorithmCRC64NVME {
			return invalid("The COMPOSITE checksum type cannot be used with the CRC64NVME checksum algorithm.")
		}
	case "FULL_OBJECT":
		if !isCRC {
			return invalid("The FULL_OBJECT checksum type cannot be used with the " + strings.ToUpper(algorithm.String()) + " checksum algorithm.")
		}
	default:
		return invalid("Value for x-amz-checksum-type header is invalid.")
	}
	return nil
}

// getChecksumRequests returns the checksum requests for verifying a request body: its
// Content-MD5 header, and at most one X-Amz-Checksum-<algorithm> header or X-Amz-Trailer,
// which X-Amz-Sdk-Checksum-Algorithm must match if present.
func getChecksumRequests(h http.Header) ([]awsig.ChecksumRequest, error) {
	var reqs []awsig.ChecksumRequest
	req, present, err := getContentMD5ChecksumRequest(h)
	if err != nil {
		return nil, err
	}
	if present {
		reqs = append(reqs, req)
	}

	headers, algorithms, err := checksumValueHeaders(h)
	if err != nil {
		return nil, err
	}
	for i, header := range headers {
		invalid := apierr.Response{
			Code:           "InvalidRequest",
			Description:    "Value for x-amz-checksum-" + algorithms[i].String() + " header is invalid.",
			HTTPStatusCode: http.StatusBadRequest,
		}
		values := h[header]
		if len(values) != 1 {
			return nil, invalid
		}
		// awsig checks only the encoded length, which lets through values with extra padding.
		// TODO: decode strictly in awsig, so this check is not needed here.
		decoded, err := base64.StdEncoding.Strict().DecodeString(values[0])
		if err != nil || len(decoded) != checksumSize(algorithms[i]) {
			return nil, invalid
		}
		req, err := awsig.NewChecksumRequest(algorithms[i], values[0])
		if err != nil {
			return nil, invalid
		}
		reqs = append(reqs, req)
	}

	// A trailing checksum needs a streaming payload with trailers, and such a payload must
	// declare its trailer.
	trailer := strings.TrimSpace(h.Get("X-Amz-Trailer"))
	if (trailer != "") != strings.HasSuffix(h.Get("X-Amz-Content-Sha256"), "-TRAILER") {
		return nil, apierr.CodeInvalidRequest
	}
	if trailer != "" {
		if !strings.HasPrefix(http.CanonicalHeaderKey(trailer), xAmzChecksumPrefix) {
			return nil, apierr.CodeInvalidRequest
		}
		algorithm, err := checksumHeaderAlgorithm(trailer)
		if err != nil {
			return nil, err
		}
		req, err := awsig.NewTrailingChecksumRequest(algorithm)
		if err != nil {
			return nil, apierr.CodeInvalidRequest
		}
		reqs = append(reqs, req)
		algorithms = append(algorithms, algorithm)
	}

	// S3 rejects requests that specify more than one checksum algorithm.
	if len(algorithms) > 1 {
		return nil, apierr.CodeMultipleChecksums
	}

	if sdkAlgorithm := h.Get("X-Amz-Sdk-Checksum-Algorithm"); sdkAlgorithm != "" {
		algorithm, err := checksumHeaderAlgorithm(xAmzChecksumPrefix + sdkAlgorithm)
		switch {
		case errors.Is(err, apierr.CodeChecksumsUnsupported):
			return nil, err
		case err != nil:
			return nil, apierr.CodeInvalidSdkChecksumAlgorithm
		case len(algorithms) == 0:
			return nil, apierr.CodeMissingChecksumForSdkAlgorithm
		case algorithm != algorithms[0]:
			return nil, apierr.CodeInvalidSdkChecksumAlgorithm
		}
	}

	return reqs, nil
}

// setChecksumHeaders sets the X-Amz-Checksum-<algorithm> response headers for the checksums
// that the request header h asked to verify while reading a request body and reports whether
// it set any. The SHA-256 of a signed payload isn't echoed unless it was asked for.
func setChecksumHeaders(w http.ResponseWriter, h http.Header, body awsig.Reader) (set bool) {
	sums, err := body.Checksums()
	if err != nil {
		return false
	}
	trailer := http.CanonicalHeaderKey(strings.TrimSpace(h.Get("X-Amz-Trailer")))
	for header, algorithm := range checksumHeaders {
		if _, requested := h[header]; !requested && header != trailer {
			continue
		}
		if sum, ok := sums[algorithm]; ok {
			w.Header().Set(header, base64.StdEncoding.EncodeToString(sum))
			set = true
		}
	}
	return set
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
