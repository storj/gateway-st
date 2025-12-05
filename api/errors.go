// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package api

import (
	"errors"
	"net/http"

	"github.com/amwolff/awsig"

	"storj.io/minio/cmd"
)

// awsigAPIErrorCodes maps awsig errors to APIErrorCodes. The awsig errors that don't have a corresponding
// APIErrorCode are listed in awsigAPIErrors.
var awsigAPIErrorCodes = map[error]cmd.APIErrorCode{
	awsig.ErrAuthorizationHeaderMalformed:    cmd.ErrAuthorizationHeaderMalformed,
	awsig.ErrInvalidAccessKeyID:              cmd.ErrInvalidAccessKeyID,
	awsig.ErrInvalidDateHeader:               cmd.ErrMalformedDate,
	awsig.ErrInvalidPresignedDate:            cmd.ErrMalformedPresignedDate,
	awsig.ErrInvalidPresignedExpiration:      cmd.ErrMalformedExpires,
	awsig.ErrInvalidSignature:                cmd.ErrSignatureDoesNotMatch,
	awsig.ErrInvalidXAmzDecodedContentSHA256: cmd.ErrMissingContentLength,
	awsig.ErrMalformedPOSTRequest:            cmd.ErrMalformedPOSTRequest,
	awsig.ErrMissingAuthenticationToken:      cmd.ErrAccessDenied,
	awsig.ErrMissingContentLength:            cmd.ErrMissingContentLength,
	awsig.ErrMissingSecurityHeader:           cmd.ErrMissingSecurityHeader,
	awsig.ErrNegativePresignedExpiration:     cmd.ErrNegativeExpires,
	awsig.ErrPresignedExpirationTooLarge:     cmd.ErrMaximumExpires,
	awsig.ErrRequestNotYetValid:              cmd.ErrRequestNotReadyYet,
	awsig.ErrRequestExpired:                  cmd.ErrExpiredPresignRequest,
	awsig.ErrRequestTimeTooSkewed:            cmd.ErrRequestTimeTooSkewed,
	awsig.ErrSignatureDoesNotMatch:           cmd.ErrSignatureDoesNotMatch,
}

// awsigAPIErrors maps awsig errors to APIErrors. For awsig errors that correspond to an APIErrorCode,
// awsigAPIErrorCodes should be used instead.
var awsigAPIErrors = map[error]cmd.APIError{
	awsig.ErrContentLengthWithTransferEncoding: {
		Code:           "InvalidRequest",
		Description:    "Cannot specify both Content-Length and Transfer-Encoding HTTP headers.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	awsig.ErrInvalidPOSTDate: {
		Code:           "InvalidArgument",
		Description:    "X-Amz-Date must be formatted via ISO8601 Long format.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	awsig.ErrInvalidXAmzContentSHA256: {
		Code:           "InvalidArgument",
		Description:    "x-amz-content-sha256 must be UNSIGNED-PAYLOAD, STREAMING-AWS4-HMAC-SHA256-PAYLOAD, or a valid sha256 value.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	awsig.ErrMissingPOSTPolicy: {
		Code:           "InvalidArgument",
		Description:    "Bucket POST must contain a field named 'policy'.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	awsig.ErrNotImplemented: {
		Code:           "NotImplemented",
		Description:    "The AWS4-ECDSA-P256-SHA256 algorithm is not implemented yet.",
		HTTPStatusCode: http.StatusNotImplemented,
	},
	awsig.ErrUnsupportedSignature: {
		Code:           "UnsupportedSignature",
		Description:    "The provided request is signed with an unsupported STS Token version or the signature version is not supported.",
		HTTPStatusCode: http.StatusBadRequest,
	},
}

func awsigToAPIError(err error) cmd.APIError {
	for awsigErr, code := range awsigAPIErrorCodes {
		if errors.Is(err, awsigErr) {
			return cmd.GetAPIError(code)
		}
	}
	for awsigErr, apiErr := range awsigAPIErrors {
		if errors.Is(err, awsigErr) {
			return apiErr
		}
	}
	return cmd.GetAPIError(cmd.ErrInternalError)
}
