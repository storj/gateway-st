// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package api

import (
	"errors"
	"slices"

	"github.com/amwolff/awsig"

	"storj.io/gateway/api/apierr"
)

// awsigErrToCodeMap maps awsig errors to their corresponding error codes.
var awsigErrToCodeMap = map[error]apierr.Code{
	awsig.ErrAccessDenied:                      apierr.CodeAccessDenied,
	awsig.ErrAuthorizationHeaderMalformed:      apierr.CodeAuthorizationHeaderMalformed,
	awsig.ErrBadDigest:                         apierr.CodeBadDigest,
	awsig.ErrContentLengthWithTransferEncoding: apierr.CodeContentLengthWithTransferEncoding,
	awsig.ErrEntityTooLarge:                    apierr.CodeEntityTooLarge,
	awsig.ErrEntityTooSmall:                    apierr.CodeEntityTooSmall,
	awsig.ErrInvalidAccessKeyID:                apierr.CodeInvalidAccessKeyID,
	awsig.ErrInvalidDateHeader:                 apierr.CodeMalformedDate,
	awsig.ErrInvalidDigest:                     apierr.CodeInvalidContentMD5,
	awsig.ErrInvalidPOSTDate:                   apierr.CodeMalformedPOSTDate,
	awsig.ErrInvalidPresignedDate:              apierr.CodeMalformedPresignedDate,
	awsig.ErrInvalidPresignedExpiration:        apierr.CodeMalformedExpires,
	awsig.ErrInvalidPresignedXAmzContentSHA256: apierr.CodeContentSHA256Mismatch,
	awsig.ErrInvalidRequest:                    apierr.CodeInvalidRequest,
	awsig.ErrInvalidSignature:                  apierr.CodeSignatureDoesNotMatch,
	awsig.ErrInvalidXAmzContentSHA256:          apierr.CodeInvalidContentSHA256,
	awsig.ErrInvalidXAmzDecodedContentLength:   apierr.CodeMissingContentLength,
	awsig.ErrMalformedPOSTRequest:              apierr.CodeMalformedPOSTRequest,
	awsig.ErrMissingContentLength:              apierr.CodeMissingContentLength,
	awsig.ErrMissingPOSTPolicy:                 apierr.CodeMissingPOSTPolicy,
	awsig.ErrMissingSecurityHeader:             apierr.CodeMissingSecurityHeader,
	awsig.ErrNegativePresignedExpiration:       apierr.CodeNegativeExpires,
	awsig.ErrNotImplemented:                    apierr.CodeUnsupportedECDSAP256SHA256,
	awsig.ErrPresignedExpirationTooLarge:       apierr.CodeMaximumExpires,
	awsig.ErrRequestExpired:                    apierr.CodeExpiredPresignedRequest,
	awsig.ErrRequestNotYetValid:                apierr.CodeRequestNotReadyYet,
	awsig.ErrRequestTimeTooSkewed:              apierr.CodeRequestTimeTooSkewed,
	awsig.ErrSignatureDoesNotMatch:             apierr.CodeSignatureDoesNotMatch,
	awsig.ErrUnsupportedSignature:              apierr.CodeUnsupportedSignature,
}

func awsigErrToCode(err error) (code apierr.Code, ok bool) {
	for awsigErr, code := range awsigErrToCodeMap {
		if errors.Is(err, awsigErr) {
			return code, true
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
			case a.Algorithm == awsig.AlgorithmMD5:
				return 0
			case a.Algorithm == awsig.AlgorithmSHA256 && a.IsContentSHA256:
				return 1
			default:
				return 2
			}
		}
		return ord(a) - ord(b)
	})
	return mismatchErr.Mismatches[0], true
}
