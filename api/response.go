// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.
// This file incorporates code from MinIO Cloud Storage and includes changes made by Storj Labs, Inc.

/*
 * MinIO Cloud Storage, (C) 2015, 2016, 2017, 2018 MinIO, Inc.
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
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/amwolff/awsig"
	miniogo "github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"storj.io/gateway/api/apierr"
	"storj.io/minio/cmd"
	"storj.io/minio/cmd/crypto"
	xhttp "storj.io/minio/cmd/http"
)

var internalErrorResponse = apierr.Response{
	Code:           "InternalError",
	Description:    "We encountered an internal error. Please try again.",
	HTTPStatusCode: http.StatusInternalServerError,
}

type mimeType string

const mimeNone mimeType = ""

func writeResponse(w http.ResponseWriter, statusCode int, response []byte, mType mimeType) {
	setCommonHeaders(w)
	if mType != mimeNone {
		w.Header().Set(xhttp.ContentType, string(mType))
	}
	w.Header().Set(xhttp.ContentLength, strconv.Itoa(len(response)))
	w.WriteHeader(statusCode)
	if response != nil {
		_, _ = w.Write(response)
		w.(http.Flusher).Flush()
	}
}

func setCommonHeaders(w http.ResponseWriter) {
	w.Header().Set(xhttp.ServerInfo, "Storj")
	w.Header().Set(xhttp.AcceptRanges, "bytes")
	crypto.RemoveSensitiveHeaders(w.Header())
}

func writeSuccessResponseHeadersOnly(w http.ResponseWriter) {
	writeResponse(w, http.StatusOK, nil, mimeNone)
}

func writeSuccessNoContent(w http.ResponseWriter) {
	writeResponse(w, http.StatusNoContent, nil, mimeNone)
}

func (api *API) writeErrorResponse(ctx context.Context, w http.ResponseWriter, err error) {
	resp, matched := errToResponse(err)
	if !matched {
		api.log.Error("unexpected error", zap.Error(err))
		resp = internalErrorResponse
	}

	buf := bytes.NewBufferString(xml.Header)
	e := xml.NewEncoder(buf)
	if xmlErr := e.Encode(resp); xmlErr != nil {
		api.log.Error("error encoding XML error response", zap.Error(err))
		writeResponse(w, http.StatusInternalServerError, nil, mimeNone)
	}
}

func (api *API) writeErrorResponseHeadersOnly(w http.ResponseWriter, err error) {
	resp, matched := errToResponse(err)
	if !matched {
		api.log.Error("unexpected error", zap.Error(err))
		resp = internalErrorResponse
	}
	writeResponse(w, resp.HTTPStatusCode, nil, mimeNone)
}

func errToResponse(err error) (resp apierr.Response, matched bool) {
	if errors.As(err, &resp) {
		return resp, true
	}

	if code := apierr.Code(0); errors.As(err, &code) {
		var ok bool
		resp, ok = code.ToResponse()
		if ok {
			return resp, true
		}
		return apierr.Response{}, false
	}

	if miniogoResp := (miniogo.ErrorResponse{}); errors.As(err, &miniogoResp) {
		return apierr.Response{
			Code:           miniogoResp.Code,
			Description:    miniogoResp.Message,
			HTTPStatusCode: miniogoResp.StatusCode,
		}, true
	}

	if provider, ok := err.(cmd.APIErrorProvider); ok {
		apiErr := provider.ToAPIError()
		return apierr.Response{
			Code:           apiErr.Code,
			Description:    apiErr.Description,
			HTTPStatusCode: apiErr.HTTPStatusCode,
		}, true
	}

	if code, ok := awsigErrToCode(err); ok {
		if code == apierr.CodeBadDigest {
			if mismatch, ok := getChecksumMismatchFromError(err); ok {
				switch {
				case mismatch.Algorithm == awsig.AlgorithmMD5:
					code = apierr.CodeContentMD5Mismatch
				case mismatch.Algorithm == awsig.AlgorithmSHA256 && mismatch.IsContentSHA256:
					code = apierr.CodeContentSHA256Mismatch
				default:
					return apierr.Response{
						Code:           "BadDigest",
						Description:    "The " + strings.ToUpper(mismatch.Algorithm.String()) + " you specified did not match the calculated checksum.",
						HTTPStatusCode: http.StatusBadRequest,
					}, true
				}
			}
		}

		resp, ok = code.ToResponse()
		if ok {
			return resp, true
		}
		return apierr.Response{}, false
	}

	return apierr.Response{}, false
}
