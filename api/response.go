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
	"encoding/base64"
	"encoding/xml"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/amwolff/awsig"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/tags"

	"storj.io/gateway/api/apierr"
	"storj.io/minio/cmd"
	"storj.io/minio/cmd/crypto"
	xhttp "storj.io/minio/cmd/http"
)

const (
	defaultOwnerID          = "7b25a206cc747e61355f1af9395c2e1dc93664b7b64838ca859b245e20dead3c"
	defaultOwnerDisplayName = "storj"
	defaultStorageClass     = "STANDARD"
	iso8601Milli            = "2006-01-02T15:04:05.000Z"
)

type mimeType string

const (
	mimeNone mimeType = ""
	mimeXML  mimeType = "application/xml"
)

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

func encodeResponse(response any) ([]byte, error) {
	bytesBuffer := bytes.NewBufferString(xml.Header)
	if err := xml.NewEncoder(bytesBuffer).Encode(response); err != nil {
		return nil, err
	}
	return bytesBuffer.Bytes(), nil
}

func writeRedirectSeeOther(w http.ResponseWriter, location string) {
	w.Header().Set(xhttp.Location, location)
	writeResponse(w, http.StatusSeeOther, nil, mimeNone)
}

func writeSuccessResponseHeadersOnly(w http.ResponseWriter) {
	writeResponse(w, http.StatusOK, nil, mimeNone)
}

func writeSuccessResponseXML(w http.ResponseWriter, response []byte) {
	writeResponse(w, http.StatusOK, response, mimeXML)
}

func writeSuccessNoContent(w http.ResponseWriter) {
	writeResponse(w, http.StatusNoContent, nil, mimeNone)
}

func (api *API) writeErrorResponse(w http.ResponseWriter, r *http.Request, err error) {
	api.writeErrorResponseWithFallback(w, r, err, nil)
}

func (api *API) writeErrorResponseWithFallback(w http.ResponseWriter, r *http.Request, err error, fallbackErr error) {
	resp, matched := errToResponse(err)
	if !matched && fallbackErr != nil {
		resp, matched = errToResponse(fallbackErr)
	}
	if !matched {
		api.log.Error(r, "unexpected error", err)
		// It's safe to ignore the second return value (whether a corresponding response exists)
		// because apierr.Code constants and their responses are generated together. A defined
		// apierr.Code constant is guaranteed to have a defined response.
		resp, _ = apierr.CodeInternal.ToResponse()
	}

	buf := bytes.NewBufferString(xml.Header)
	e := xml.NewEncoder(buf)
	if xmlErr := e.Encode(resp); xmlErr != nil {
		api.log.Error(r, "error encoding XML error response", err)
		writeResponse(w, http.StatusInternalServerError, nil, mimeNone)
	}

	writeResponse(w, resp.HTTPStatusCode, buf.Bytes(), mimeXML)
}

func (api *API) writeErrorResponseHeadersOnly(r *http.Request, w http.ResponseWriter, err error) {
	resp, matched := errToResponse(err)
	if !matched {
		api.log.Error(r, "unexpected error", err)
		resp, _ = apierr.CodeInternal.ToResponse()
	}
	writeResponse(w, resp.HTTPStatusCode, nil, mimeNone)
}

func errToResponse(err error) (resp apierr.Response, matched bool) {
	if errors.As(err, &resp) {
		return resp, true
	}

	if provider := apierr.ResponseProvider(nil); errors.As(err, &provider) {
		return provider.ToResponse(), true
	}

	if miniogoResp := (miniogo.ErrorResponse{}); errors.As(err, &miniogoResp) {
		return apierr.Response{
			Code:           miniogoResp.Code,
			Description:    miniogoResp.Message,
			HTTPStatusCode: miniogoResp.StatusCode,
		}, true
	}

	if provider := cmd.APIErrorProvider(nil); errors.As(err, &provider) {
		apiErr := provider.ToAPIError()
		return apierr.Response{
			Code:           apiErr.Code,
			Description:    apiErr.Description,
			HTTPStatusCode: apiErr.HTTPStatusCode,
		}, true
	}

	if code := apierr.Code(0); errors.As(err, &code) {
		var ok bool
		resp, ok = code.ToResponse()
		if ok {
			return resp, true
		}
		return apierr.Response{}, false
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

	if tagsErr := tags.Error(nil); errors.As(err, &tagsErr) {
		return apierr.Response{
			Code:           tagsErr.Code(),
			Description:    tagsErr.Error(),
			HTTPStatusCode: http.StatusBadRequest,
		}, true
	}

	if xmlErr := (*xml.SyntaxError)(nil); errors.As(err, &xmlErr) {
		return apierr.CodeMalformedXML.ToResponse()
	}

	return apierr.Response{}, false
}

func generateListObjectsResponse(bucketName string, params listObjectsParams, listInfo cmd.ListObjectsInfo) cmd.ListObjectsResponse {
	data := cmd.ListObjectsResponse{
		Name:           bucketName,
		Contents:       make([]cmd.Object, 0, len(listInfo.Objects)),
		EncodingType:   params.encodingType,
		Prefix:         s3EncodeName(params.prefix, params.encodingType),
		Marker:         s3EncodeName(params.marker, params.encodingType),
		Delimiter:      s3EncodeName(params.delimiter, params.encodingType),
		MaxKeys:        params.maxKeys,
		NextMarker:     s3EncodeName(listInfo.NextMarker, params.encodingType),
		IsTruncated:    listInfo.IsTruncated,
		CommonPrefixes: make([]cmd.CommonPrefix, 0, len(listInfo.Prefixes)),
	}

	for _, object := range listInfo.Objects {
		if object.Name == "" {
			continue
		}

		content := cmd.Object{
			Key:          s3EncodeName(object.Name, params.encodingType),
			LastModified: object.ModTime.UTC().Format(iso8601Milli),
			Size:         object.Size,
			Owner: cmd.Owner{
				ID:          defaultOwnerID,
				DisplayName: defaultOwnerDisplayName,
			},
		}

		if object.ETag != "" {
			content.ETag = "\"" + object.ETag + "\""
		}

		if object.StorageClass != "" {
			content.StorageClass = object.StorageClass
		} else {
			content.StorageClass = defaultStorageClass
		}

		data.Contents = append(data.Contents, content)
	}

	for _, prefix := range listInfo.Prefixes {
		data.CommonPrefixes = append(data.CommonPrefixes, cmd.CommonPrefix{
			Prefix: s3EncodeName(prefix, params.encodingType),
		})
	}

	return data
}

func generateListObjectsV2Response(bucketName string, params listObjectsV2Params, listInfo cmd.ListObjectsV2Info) cmd.ListObjectsV2Response {
	data := cmd.ListObjectsV2Response{
		Name:                  bucketName,
		Contents:              make([]cmd.Object, 0, len(listInfo.Objects)),
		EncodingType:          params.encodingType,
		StartAfter:            s3EncodeName(params.startAfter, params.encodingType),
		Delimiter:             s3EncodeName(params.delimiter, params.encodingType),
		Prefix:                s3EncodeName(params.prefix, params.encodingType),
		MaxKeys:               params.maxKeys,
		ContinuationToken:     base64.StdEncoding.EncodeToString([]byte(params.token)),
		NextContinuationToken: base64.StdEncoding.EncodeToString([]byte(listInfo.NextContinuationToken)),
		IsTruncated:           listInfo.IsTruncated,
		CommonPrefixes:        make([]cmd.CommonPrefix, 0, len(listInfo.Prefixes)),
	}

	for _, object := range listInfo.Objects {
		if object.Name == "" {
			continue
		}

		content := cmd.Object{
			Key:          s3EncodeName(object.Name, params.encodingType),
			LastModified: object.ModTime.UTC().Format(iso8601Milli),
			Size:         object.Size,
			Owner: cmd.Owner{
				ID:          defaultOwnerID,
				DisplayName: defaultOwnerDisplayName,
			},
		}

		if object.ETag != "" {
			content.ETag = "\"" + object.ETag + "\""
		}

		if object.StorageClass != "" {
			content.StorageClass = object.StorageClass
		} else {
			content.StorageClass = defaultStorageClass
		}

		data.Contents = append(data.Contents, content)
	}

	for _, prefix := range listInfo.Prefixes {
		data.CommonPrefixes = append(data.CommonPrefixes, cmd.CommonPrefix{
			Prefix: s3EncodeName(prefix, params.encodingType),
		})
	}

	data.KeyCount = len(data.Contents) + len(data.CommonPrefixes)

	return data
}

func generateListVersionsResponse(bucketName string, params listObjectVersionsParams, listInfo cmd.ListObjectVersionsInfo) cmd.ListVersionsResponse {
	data := cmd.ListVersionsResponse{
		Name:                bucketName,
		Versions:            make([]cmd.ObjectVersion, 0, len(listInfo.Objects)),
		EncodingType:        params.encodingType,
		Prefix:              s3EncodeName(params.prefix, params.encodingType),
		KeyMarker:           s3EncodeName(params.marker, params.encodingType),
		Delimiter:           s3EncodeName(params.delimiter, params.encodingType),
		MaxKeys:             params.maxKeys,
		NextKeyMarker:       s3EncodeName(listInfo.NextMarker, params.encodingType),
		NextVersionIDMarker: listInfo.NextVersionIDMarker,
		VersionIDMarker:     params.versionIDMarker,
		IsTruncated:         listInfo.IsTruncated,
		CommonPrefixes:      make([]cmd.CommonPrefix, 0, len(listInfo.Prefixes)),
	}

	for _, object := range listInfo.Objects {
		if object.Name == "" {
			continue
		}

		content := cmd.ObjectVersion{
			Object: cmd.Object{
				Key:          s3EncodeName(object.Name, params.encodingType),
				LastModified: object.ModTime.UTC().Format(iso8601Milli),
				Size:         object.Size,
				Owner: cmd.Owner{
					ID:          defaultOwnerID,
					DisplayName: defaultOwnerDisplayName,
				},
			},
			VersionID:      object.VersionID,
			IsLatest:       object.IsLatest,
			IsDeleteMarker: object.DeleteMarker,
		}

		if object.ETag != "" {
			content.ETag = "\"" + object.ETag + "\""
		}

		if object.StorageClass != "" {
			content.StorageClass = object.StorageClass
		} else {
			content.StorageClass = defaultStorageClass
		}

		if content.VersionID == "" {
			content.VersionID = nullVersionID
		}

		data.Versions = append(data.Versions, content)
	}

	for _, prefix := range listInfo.Prefixes {
		data.CommonPrefixes = append(data.CommonPrefixes, cmd.CommonPrefix{
			Prefix: s3EncodeName(prefix, params.encodingType),
		})
	}

	return data
}

func generateListMultipartUploadsResponse(bucket string, listInfo cmd.ListMultipartsInfo, encodingType string) cmd.ListMultipartUploadsResponse {
	listMultipartUploadsResponse := cmd.ListMultipartUploadsResponse{
		Bucket:             bucket,
		Delimiter:          s3EncodeName(listInfo.Delimiter, encodingType),
		IsTruncated:        listInfo.IsTruncated,
		EncodingType:       encodingType,
		Prefix:             s3EncodeName(listInfo.Prefix, encodingType),
		KeyMarker:          s3EncodeName(listInfo.KeyMarker, encodingType),
		NextKeyMarker:      s3EncodeName(listInfo.NextKeyMarker, encodingType),
		MaxUploads:         listInfo.MaxUploads,
		NextUploadIDMarker: listInfo.NextUploadIDMarker,
		UploadIDMarker:     listInfo.UploadIDMarker,
		CommonPrefixes:     make([]cmd.CommonPrefix, len(listInfo.CommonPrefixes)),
		Uploads:            make([]cmd.Upload, len(listInfo.Uploads)),
	}

	for index, commonPrefix := range listInfo.CommonPrefixes {
		listMultipartUploadsResponse.CommonPrefixes[index] = cmd.CommonPrefix{
			Prefix: s3EncodeName(commonPrefix, encodingType),
		}
	}

	for index, upload := range listInfo.Uploads {
		listMultipartUploadsResponse.Uploads[index] = cmd.Upload{
			UploadID:  upload.UploadID,
			Key:       s3EncodeName(upload.Object, encodingType),
			Initiated: upload.Initiated.UTC().Format(iso8601Milli),
		}
	}

	return listMultipartUploadsResponse
}
