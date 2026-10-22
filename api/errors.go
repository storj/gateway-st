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
// TODO: find out whether S3 rejects these malformed Signature Version 4 requests like awsig or like
// ceph's boto2 tests in s3tests/functional/test_headers.py expect:
//
//   - a header listed in SignedHeaders is missing (test_object_create_bad_ua_none_aws4,
//     test_bucket_create_bad_ua_none_aws4, test_object_create_missing_signed_header_aws4 and
//     test_object_create_missing_signed_custom_header_aws4): awsig returns MissingSecurityHeader
//     (400), the tests expect 403;
//   - the credential's date doesn't match X-Amz-Date
//     (test_object_create_bad_authorization_incorrect_aws4): awsig returns
//     AuthorizationHeaderMalformed (400), the test expects 403;
//   - a malformed Credential (test_bucket_create_bad_authorization_invalid_aws4): awsig rejects
//     the date header first with AccessDenied (403), the test expects 400;
//   - both Content-MD5 and X-Amz-Content-Sha256 mismatch
//     (test_object_create_bad_contentlength_mismatch_below_aws4): awsig reports BadDigest for
//     Content-MD5 first, the test expects XAmzContentSHA256Mismatch.
//
// awsig's author verified its error codes against S3, and their fork of the tests omits these.
// CI doesn't run them, because testsuite/ceph-s3-tests/run.sh doesn't set S3_USE_SIGV4, and they
// need a host name to sign for rather than an IP address. The other 30 of them pass against the
// filesystem object layer.
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
	// multiple integrity failures, Content-MD5 takes priority over X-Amz-Content-Sha256,
	// which takes priority over X-Amz-Checksum-<algorithm>.
	slices.SortFunc(mismatchErr.Mismatches, func(a awsig.ChecksumMismatch, b awsig.ChecksumMismatch) int {
		ord := func(mismatch awsig.ChecksumMismatch) int {
			switch {
			case mismatch.Algorithm == awsig.AlgorithmMD5:
				return 0
			case mismatch.Algorithm == awsig.AlgorithmSHA256 && mismatch.IsContentSHA256:
				return 1
			default:
				return 2
			}
		}
		return ord(a) - ord(b)
	})
	return mismatchErr.Mismatches[0], true
}
