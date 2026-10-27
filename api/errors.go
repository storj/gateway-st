// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package api

import (
	"errors"
	"slices"

	"github.com/amwolff/awsig"

	"storj.io/gateway/api/apierr"
)

// awsigErrToCodes maps awsig errors to their corresponding error codes. awsig nests some errors
// in others, such as ErrInvalidChunkSize in ErrEntityTooSmall and ErrMessageTooLarge in
// ErrMalformedPOSTRequest, so the more specific errors come first.
//
// TODO: map awsig.ErrInvalidToken to InvalidToken (400) once awsig is bumped to include
// storj/awsig's credentials branch, which adds it. Otherwise a request carrying a session token
// gets InternalError.
//
// TODO: awsig rejects these malformed requests with different errors than S3 (checked against S3
// in October 2026):
//
//   - SignedHeaders doesn't list host (test_sigv4_host_bad), or a header that it lists is missing
//     (test_sigv4_missing_header): awsig returns MissingSecurityHeader (400), ceph's tests expect
//     403 (S3 returns AccessDenied and SignatureDoesNotMatch respectively);
//   - the Authorization header has an unsupported scheme, such as "AWS4 HAHAHA" or "Bearer x":
//     awsig checks the date first, returning AccessDenied (403) without a valid Date or X-Amz-Date
//     and UnsupportedSignature (400) with one, while S3 returns InvalidArgument (400) either way.
var awsigErrToCodes = []struct {
	err  error
	code apierr.Code
}{
	{awsig.ErrInvalidChunkSize, apierr.CodeInvalidChunkSize},
	{awsig.ErrMessageTooLarge, apierr.CodeMaxPostPreDataLengthExceeded},
	{awsig.ErrMalformedTrailer, apierr.CodeMalformedTrailer},
	{awsig.ErrAccessDenied, apierr.CodeAccessDenied},
	{awsig.ErrAuthorizationHeaderMalformed, apierr.CodeAuthorizationHeaderMalformed},
	{awsig.ErrAuthorizationQueryParametersError, apierr.CodeAuthorizationQueryParameters},
	{awsig.ErrBadDigest, apierr.CodeBadDigest},
	{awsig.ErrContentLengthWithTransferEncoding, apierr.CodeContentLengthWithTransferEncoding},
	{awsig.ErrEntityTooLarge, apierr.CodeEntityTooLarge},
	{awsig.ErrEntityTooSmall, apierr.CodeEntityTooSmall},
	{awsig.ErrInvalidAccessKeyID, apierr.CodeInvalidAccessKeyID},
	{awsig.ErrInvalidArgument, apierr.CodeInvalidArgument},
	{awsig.ErrInvalidChecksumRequest, apierr.CodeInvalidRequest},
	{awsig.ErrInvalidDateHeader, apierr.CodeInvalidDateHeader},
	{awsig.ErrInvalidDigest, apierr.CodeInvalidContentMD5},
	{awsig.ErrInvalidPOSTDate, apierr.CodeMalformedPOSTDate},
	{awsig.ErrInvalidPresignedDate, apierr.CodeMalformedPresignedDate},
	{awsig.ErrInvalidPresignedExpiration, apierr.CodeMalformedExpires},
	{awsig.ErrInvalidPresignedXAmzContentSHA256, apierr.CodeContentSHA256Mismatch},
	{awsig.ErrInvalidRequest, apierr.CodeInvalidRequest},
	{awsig.ErrInvalidSignature, apierr.CodeSignatureDoesNotMatch},
	{awsig.ErrInvalidXAmzContentSHA256, apierr.CodeInvalidContentSHA256},
	{awsig.ErrInvalidXAmzDecodedContentLength, apierr.CodeMissingContentLength},
	{awsig.ErrMalformedPOSTRequest, apierr.CodeMalformedPOSTRequest},
	{awsig.ErrMissingContentLength, apierr.CodeMissingContentLength},
	{awsig.ErrMissingPOSTPolicy, apierr.CodeMissingPOSTPolicy},
	{awsig.ErrMissingSecurityHeader, apierr.CodeMissingSecurityHeader},
	{awsig.ErrNegativePresignedExpiration, apierr.CodeNegativeExpires},
	{awsig.ErrNotImplemented, apierr.CodeUnsupportedECDSAP256SHA256},
	{awsig.ErrPresignedExpirationTooLarge, apierr.CodeMaximumExpires},
	{awsig.ErrRequestExpired, apierr.CodeExpiredPresignedRequest},
	{awsig.ErrRequestNotYetValid, apierr.CodeRequestNotReadyYet},
	{awsig.ErrRequestTimeTooSkewed, apierr.CodeRequestTimeTooSkewed},
	{awsig.ErrSignatureDoesNotMatch, apierr.CodeSignatureDoesNotMatch},
	{awsig.ErrUnsignedHeader, apierr.CodeUnsignedHeaders},
	{awsig.ErrUnsupportedSignature, apierr.CodeUnsupportedSignature},
}

func awsigErrToCode(err error) (code apierr.Code, ok bool) {
	for _, m := range awsigErrToCodes {
		if errors.Is(err, m.err) {
			return m.code, true
		}
	}
	return apierr.CodeNone, false
}

func getChecksumMismatchFromError(err error) (mismatch awsig.ChecksumMismatch, ok bool) {
	var mismatchErr awsig.ChecksumMismatchError
	if !errors.As(err, &mismatchErr) {
		return awsig.ChecksumMismatch{}, false
	}
	// When S3 determines what information to include in an error for a request containing
	// multiple integrity failures, X-Amz-Content-Sha256 takes priority over Content-MD5,
	// which takes priority over X-Amz-Checksum-<algorithm>.
	slices.SortFunc(mismatchErr.Mismatches, func(a awsig.ChecksumMismatch, b awsig.ChecksumMismatch) int {
		ord := func(mismatch awsig.ChecksumMismatch) int {
			switch {
			case mismatch.Algorithm == awsig.AlgorithmSHA256 && mismatch.IsContentSHA256:
				return 0
			case mismatch.Algorithm == awsig.AlgorithmMD5:
				return 1
			default:
				return 2
			}
		}
		return ord(a) - ord(b)
	})
	return mismatchErr.Mismatches[0], true
}
