// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api

import (
	"net/http"
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
