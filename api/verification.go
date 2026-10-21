// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/amwolff/awsig"

	"storj.io/gateway/api/apierr"
	xhttp "storj.io/minio/cmd/http"
)

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

// requestObjectSize returns the size of the object or part in r's body. net/http reads a request
// with neither Content-Length nor Transfer-Encoding as having an empty body, but S3 rejects it as
// missing its length.
func requestObjectSize(r *http.Request) (int64, error) {
	if r.ContentLength == 0 && r.Header.Get(xhttp.ContentLength) == "" {
		return 0, apierr.CodeMissingContentLength
	}

	size := r.ContentLength
	if isStreamingSigV4(r) {
		var err error
		size, err = strconv.ParseInt(r.Header.Get(xhttp.AmzDecodedContentLength), 10, 64)
		if err != nil {
			// This shouldn't happen here as this case is handled previously by awsig's validation
			return 0, err
		}
	}
	if size == -1 {
		return 0, apierr.CodeMissingContentLength
	}
	return size, nil
}

func (api *API) verifyWithBody(r *http.Request, requireChecksum bool) (awsig.Reader, error) {
	vr, err := api.verifier.Verify(r, getVirtualHostedBucket(r))
	if err != nil {
		return nil, err
	}

	checksumReqs, err := getChecksumRequests(r.Header)
	if err != nil {
		return nil, err
	}
	if requireChecksum && len(checksumReqs) == 0 {
		return nil, apierr.CodeMissingContentMD5
	}

	return requestBody(vr.Reader(checksumReqs...))
}

// requestBody wraps the reader of a request's body so that a body ending before its declared
// length can be told apart from an io.ErrUnexpectedEOF of the object layer.
func requestBody(r awsig.Reader, err error) (awsig.Reader, error) {
	if err != nil {
		return nil, err
	}
	return requestBodyReader{r}, nil
}

type requestBodyReader struct {
	awsig.Reader
}

func (r requestBodyReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = incompleteBodyError{err}
	}
	return n, err
}

// incompleteBodyError is the error of a request body that ends before its declared chunk or
// content length.
type incompleteBodyError struct {
	error
}

func (e incompleteBodyError) Unwrap() error { return e.error }

// readVerifiedBody reads the entire body, failing with apierr.CodeEntityTooLarge if it is
// longer than limit. awsig verifies Content-MD5, x-amz-checksum-* and the signed payload
// hash only at EOF, which parsers that stop at the XML root element never reach.
func readVerifiedBody(body awsig.Reader, limit int64) ([]byte, error) {
	return io.ReadAll(NewLimitedAwsigReader(body, limit))
}

// decodeVerifiedXML reads the entire bounded body before decoding it so that checksum
// and signature errors are reported even when they occur after the XML root element.
func decodeVerifiedXML(body awsig.Reader, v any, limit int64) error {
	data, err := readVerifiedBody(body, limit)
	if err != nil {
		return err
	}
	return xmlDecoder(bytes.NewReader(data), v)
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
	// Readers may return the last bytes together with io.EOF, which still means the limit was exceeded.
	if probeErr != nil && (probeN == 0 || !errors.Is(probeErr, io.EOF)) {
		lr.limitErr = probeErr
	}

	return n, lr.limitErr
}

// Checksums implements te awsig.Reader interface.
func (lr *limitedAwsigReader) Checksums() (map[awsig.ChecksumAlgorithm][]byte, error) {
	return lr.reader.Checksums()
}
