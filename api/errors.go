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
// in others, such as ErrInvalidChunkSize in ErrEntityTooSmall, ErrMessageTooLarge in
// ErrMalformedPOSTRequest, ErrUnsignedHeader in ErrMissingSecurityHeader and
// ErrAuthorizationQueryParametersError in ErrAuthorizationHeaderMalformed, so the more specific
// errors come first.
//
// TODO: map awsig.ErrInvalidToken to InvalidToken (400) once awsig validates session tokens.
// storj/awsig's credentials branch adds it, but on top of the earlier fork, so it needs porting
// to the branch gateway uses first. Otherwise a request carrying a session token gets
// InternalError.
//
// TODO: awsig answers some malformed Authorization headers differently from S3. Below, "the date
// error" is 403 AccessDenied "AWS authentication requires a valid Date or x-amz-date header".
// S3's answers were checked in October and November 2026.
//
//   - SignedHeaders leaves out host (ceph's test_sigv4_host_bad), or lists a header that the
//     request doesn't have (test_sigv4_missing_header).
//     S3: 403 AccessDenied for the first. For the second it signs the missing header as empty,
//     so it answers 403 SignatureDoesNotMatch unless the client signed it that way too.
//     awsig: 400 MissingSecurityHeader for both.
//   - The scheme is unknown, such as "AWS4 HAHAHA", "AWS4-HMAC-SHA1 ...", "Bearer x" or "Basic ...".
//     S3: 400 InvalidArgument "Unsupported Authorization Type" with a valid date. Without one, it
//     answers either that or the date error, varying between requests.
//     awsig: 400 UnsupportedSignature with a Date header, otherwise the date error.
//   - There's no space after the scheme, as in a bare "AWS4-HMAC-SHA256" or a single word.
//     S3: 400 InvalidArgument "Authorization header is invalid -- one and only one ' ' (space)
//     required", even without a date, though a single word without a date sometimes gets the date
//     error instead.
//     awsig: 400 AuthorizationHeaderMalformed, or the date error when it finds no date.
//   - The scheme is in lower case, as in "aws4-hmac-sha256 ...".
//     S3: treats it as SigV4, so it's 403 AccessDenied or SignatureDoesNotMatch depending on the
//     signed headers, or the date error. It has also accepted a correctly signed request.
//     awsig: 400 UnsupportedSignature with a Date header, otherwise the date error.
//   - The Credential is malformed and the date is valid. Without a valid date, both answer the date
//     error first.
//     S3: 400 AuthorizationHeaderMalformed with a message that says what's wrong, after "The
//     authorization header is malformed;". For example "the region 'eu-west-1' is wrong;
//     expecting 'us-east-1'", or "the Credential is mal-formed; expecting
//     \"<YOUR-AKID>/YYYYMMDD/REGION/SERVICE/aws4_request\"" for a missing or extra part.
//     awsig: the same code with a generic message, except a wrong region, which is 403
//     SignatureDoesNotMatch.
//   - There's no x-amz-content-sha256 header, with GET, HEAD or PUT.
//     S3: 400 InvalidRequest "Missing required header for this request: x-amz-content-sha256".
//     awsig: 400 MissingSecurityHeader, as for the SignedHeaders problems above, which it checks
//     first. Telling them apart needs an awsig error of its own.
//
// TODO: awsig answers some malformed aws-chunked bodies and signatures differently from S3.
// S3's answers were checked in November 2026.
//
//   - Data follows the CRLF that ends a body without a trailer.
//     S3: 400 IncompleteBody. awsig: 400 InvalidRequest.
//   - An unsigned trailer ends after its header line, without the final blank line.
//     S3: accepts it. awsig: 400 IncompleteBody.
//   - Nothing follows the final chunk, or an unsigned trailer is a blank line or has a second
//     header line.
//     S3: 400 MalformedTrailerError. awsig: 400 IncompleteBody.
//   - A trailer line has no colon.
//     S3: 400 IncompleteBody. awsig: 400 MalformedTrailerError.
//   - A signed trailer has a signature line but no trailing header.
//     S3: 403 SignatureDoesNotMatch. awsig: 400 IncompleteBody.
//   - The path contains a raw '+'.
//     S3: signs it as a space (%20). awsig: signs it as %2B.
//   - A presigned URL's X-Amz-SignedHeaders contains an uppercase name.
//     S3: lowercases it, so it's 403 SignatureDoesNotMatch unless the request was signed with the
//     lowercase name. awsig: 400 AuthorizationHeaderMalformed.
var awsigErrToCodes = []struct {
	err  error
	code apierr.Code
}{
	{awsig.ErrInvalidChunkSize, apierr.CodeInvalidChunkSize},
	{awsig.ErrMessageTooLarge, apierr.CodeMaxPostPreDataLengthExceeded},
	{awsig.ErrMalformedTrailer, apierr.CodeMalformedTrailer},
	{awsig.ErrUnsignedHeader, apierr.CodeUnsignedHeaders},
	{awsig.ErrAuthorizationQueryParametersError, apierr.CodeAuthorizationQueryParameters},
	{awsig.ErrAccessDenied, apierr.CodeAccessDenied},
	{awsig.ErrAuthorizationHeaderMalformed, apierr.CodeAuthorizationHeaderMalformed},
	{awsig.ErrBadDigest, apierr.CodeBadDigest},
	{awsig.ErrContentLengthWithTransferEncoding, apierr.CodeContentLengthWithTransferEncoding},
	{awsig.ErrEntityTooLarge, apierr.CodeEntityTooLarge},
	{awsig.ErrEntityTooSmall, apierr.CodeEntityTooSmall},
	{awsig.ErrInvalidAccessKeyID, apierr.CodeInvalidAccessKeyID},
	{awsig.ErrInvalidArgument, apierr.CodeInvalidArgument},
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
	// which takes priority over X-Amz-Checksum-<algorithm>. The operations with an XML body
	// report Content-MD5 before X-Amz-Content-Sha256.
	md5First := errors.As(err, new(xmlBodyChecksumError))
	slices.SortFunc(mismatchErr.Mismatches, func(a awsig.ChecksumMismatch, b awsig.ChecksumMismatch) int {
		ord := func(mismatch awsig.ChecksumMismatch) int {
			switch {
			case mismatch.Algorithm == awsig.AlgorithmSHA256 && mismatch.IsContentSHA256:
				if md5First {
					return 1
				}
				return 0
			case mismatch.Algorithm == awsig.AlgorithmMD5:
				if md5First {
					return 0
				}
				return 1
			default:
				return 2
			}
		}
		return ord(a) - ord(b)
	})
	return mismatchErr.Mismatches[0], true
}
