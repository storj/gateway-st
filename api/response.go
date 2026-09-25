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
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/amwolff/awsig"
	"github.com/gorilla/mux"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/tags"

	"storj.io/gateway/api/apierr"
	"storj.io/minio/cmd"
	"storj.io/minio/cmd/crypto"
	xhttp "storj.io/minio/cmd/http"
	objectlock "storj.io/minio/pkg/bucket/object/lock"
	"storj.io/minio/pkg/bucket/versioning"
	"storj.io/minio/pkg/event"
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

func (api *API) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, response []byte, mType mimeType) {
	setCommonHeaders(w)
	if mType != mimeNone {
		w.Header().Set(xhttp.ContentType, string(mType))
	}
	w.Header().Set(xhttp.ContentLength, strconv.Itoa(len(response)))
	w.WriteHeader(statusCode)
	if response != nil {
		_, err := w.Write(response)
		if err != nil {
			api.log.Error(r, "error writing response", err)
		}
		_ = http.NewResponseController(w).Flush()
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

func (api *API) writeRedirectSeeOther(w http.ResponseWriter, r *http.Request, location string) {
	w.Header().Set(xhttp.Location, location)
	api.writeResponse(w, r, http.StatusSeeOther, nil, mimeNone)
}

func (api *API) writeSuccessResponseHeadersOnly(w http.ResponseWriter, r *http.Request) {
	api.writeResponse(w, r, http.StatusOK, nil, mimeNone)
}

func (api *API) writeSuccessResponseXML(w http.ResponseWriter, r *http.Request, response []byte) {
	api.writeResponse(w, r, http.StatusOK, response, mimeXML)
}

func (api *API) writeSuccessNoContent(w http.ResponseWriter, r *http.Request) {
	api.writeResponse(w, r, http.StatusNoContent, nil, mimeNone)
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

	// Responses to HEAD requests must not have a body.
	if r.Method == http.MethodHead {
		api.writeResponse(w, r, resp.HTTPStatusCode, nil, mimeNone)
		return
	}

	encodedResp, xmlErr := encodeResponse(newErrorResponse(w, r, resp))
	if xmlErr != nil {
		api.log.Error(r, "error encoding XML error response", xmlErr)
		api.writeResponse(w, r, http.StatusInternalServerError, nil, mimeNone)
		return
	}

	api.writeResponse(w, r, resp.HTTPStatusCode, encodedResp, mimeXML)
}

// errorResponse is the XML body of an S3 error response.
type errorResponse struct {
	XMLName    xml.Name `xml:"Error"`
	Code       string
	Message    string
	BucketName string `xml:",omitempty"`
	Key        string `xml:",omitempty"`
	Resource   string
	RequestID  string `xml:"RequestId"`
}

func newErrorResponse(w http.ResponseWriter, r *http.Request, resp apierr.Response) errorResponse {
	vars := mux.Vars(r)
	objectKey, err := unescapePath(vars["object"])
	if err != nil {
		objectKey = ""
	}
	return errorResponse{
		Code:       resp.Code,
		Message:    resp.Description,
		BucketName: vars["bucket"],
		Key:        objectKey,
		Resource:   getResource(r),
		RequestID:  w.Header().Get(xhttp.AmzRequestID),
	}
}

func (api *API) writeErrorResponseHeadersOnly(r *http.Request, w http.ResponseWriter, err error) {
	resp, matched := errToResponse(err)
	if !matched {
		api.log.Error(r, "unexpected error", err)
		resp, _ = apierr.CodeInternal.ToResponse()
	}
	api.writeResponse(w, r, resp.HTTPStatusCode, nil, mimeNone)
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

	if versioningErr := (versioning.Error{}); errors.As(err, &versioningErr) {
		return apierr.Response{
			Code:           "IllegalVersioningConfigurationException",
			Description:    "Versioning configuration specified in the request is invalid. (" + versioningErr.Error() + ")",
			HTTPStatusCode: http.StatusBadRequest,
		}, true
	}

	if code, ok := minioErrToAPIErrorCode(err); ok {
		apiErr := cmd.GetAPIError(code)
		return apierr.Response{
			Code:           apiErr.Code,
			Description:    apiErr.Description,
			HTTPStatusCode: apiErr.HTTPStatusCode,
		}, true
	}

	// io.EOF isn't mapped here, because the object layer can return it too. Handlers that decode an
	// empty XML body get MalformedXML from writeErrorResponseWithFallback.
	if errors.As(err, new(*xml.SyntaxError)) || errors.As(err, new(xml.UnmarshalError)) {
		return apierr.CodeMalformedXML.ToResponse()
	}

	return apierr.Response{}, false
}

// minioErrToAPIErrorCode maps the object lock and event notification configuration errors
// to MinIO API error codes the same way MinIO does.
func minioErrToAPIErrorCode(err error) (cmd.APIErrorCode, bool) {
	switch {
	case errors.Is(err, objectlock.ErrInvalidRetentionDate):
		return cmd.ErrInvalidRetentionDate, true
	case errors.Is(err, objectlock.ErrPastObjectLockRetainDate):
		return cmd.ErrPastObjectLockRetainDate, true
	case errors.Is(err, objectlock.ErrUnknownWORMModeDirective):
		return cmd.ErrUnknownWORMModeDirective, true
	case errors.Is(err, objectlock.ErrObjectLockInvalidHeaders):
		return cmd.ErrObjectLockInvalidHeaders, true
	case errors.Is(err, objectlock.ErrMalformedXML), errors.Is(err, objectlock.ErrMalformedBucketObjectConfig):
		return cmd.ErrMalformedXML, true
	case errors.Is(err, objectlock.ErrInvalidRetentionPeriod):
		return cmd.ErrInvalidRetentionPeriod, true
	case errors.Is(err, objectlock.ErrRetentionPeriodTooLarge):
		return cmd.ErrRetentionPeriodTooLarge, true
	}

	switch {
	case errorAs[*event.ErrInvalidEventName](err):
		return cmd.ErrEventNotification, true
	case errorAs[*event.ErrInvalidARN](err), errorAs[*event.ErrARNNotFound](err):
		return cmd.ErrARNNotification, true
	case errorAs[*event.ErrUnknownRegion](err):
		return cmd.ErrRegionNotification, true
	case errorAs[*event.ErrInvalidFilterName](err):
		return cmd.ErrFilterNameInvalid, true
	case errorAs[*event.ErrFilterNamePrefix](err):
		return cmd.ErrFilterNamePrefix, true
	case errorAs[*event.ErrFilterNameSuffix](err):
		return cmd.ErrFilterNameSuffix, true
	case errorAs[*event.ErrInvalidFilterValue](err):
		return cmd.ErrFilterValueInvalid, true
	case errorAs[*event.ErrDuplicateEventName](err):
		return cmd.ErrOverlappingConfigs, true
	case errorAs[*event.ErrDuplicateQueueConfiguration](err), errorAs[*event.ErrDuplicateTopicConfiguration](err):
		return cmd.ErrOverlappingFilterNotification, true
	case errorAs[*event.ErrUnsupportedConfiguration](err):
		return cmd.ErrUnsupportedNotification, true
	}

	return 0, false
}

// errorAs reports whether err wraps an error of type T.
func errorAs[T error](err error) bool {
	var target T
	return errors.As(err, &target)
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

func generateListObjectsV2Response(bucketName string, params listObjectsV2Params, listInfo cmd.ListObjectsV2Info) listObjectsV2Response {
	data := listObjectsV2Response{
		Name:                  bucketName,
		Contents:              make([]listObjectsV2Object, 0, len(listInfo.Objects)),
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

		content := listObjectsV2Object{
			Key:          s3EncodeName(object.Name, params.encodingType),
			LastModified: object.ModTime.UTC().Format(iso8601Milli),
			Size:         object.Size,
		}

		if params.fetchOwner {
			content.Owner = &cmd.Owner{
				ID:          defaultOwnerID,
				DisplayName: defaultOwnerDisplayName,
			}
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

func generateListPartsResponse(partsInfo cmd.ListPartsInfo, encodingType string) cmd.ListPartsResponse {
	resp := cmd.ListPartsResponse{
		Bucket:   partsInfo.Bucket,
		Key:      s3EncodeName(partsInfo.Object, encodingType),
		UploadID: partsInfo.UploadID,
		Initiator: cmd.Initiator{
			ID:          defaultOwnerID,
			DisplayName: defaultOwnerDisplayName,
		},
		Owner: cmd.Owner{
			ID:          defaultOwnerID,
			DisplayName: defaultOwnerDisplayName,
		},
		StorageClass:         defaultStorageClass,
		PartNumberMarker:     partsInfo.PartNumberMarker,
		NextPartNumberMarker: partsInfo.NextPartNumberMarker,
		MaxParts:             partsInfo.MaxParts,
		IsTruncated:          partsInfo.IsTruncated,
		Parts:                make([]cmd.Part, 0, len(partsInfo.Parts)),
	}

	for _, part := range partsInfo.Parts {
		respPart := cmd.Part{
			PartNumber:   part.PartNumber,
			LastModified: part.LastModified.UTC().Format(iso8601Milli),
			Size:         part.Size,
		}
		if part.ETag != "" {
			respPart.ETag = `"` + part.ETag + `"`
		}
		resp.Parts = append(resp.Parts, respPart)
	}

	return resp
}

func generateListBucketsResponse(bucketInfos []cmd.BucketInfo) cmd.ListBucketsResponse {
	resp := cmd.ListBucketsResponse{
		Owner: cmd.Owner{
			ID:          defaultOwnerID,
			DisplayName: defaultOwnerDisplayName,
		},
	}
	resp.Buckets.Buckets = make([]cmd.Bucket, 0, len(bucketInfos))

	for _, bucketInfo := range bucketInfos {
		resp.Buckets.Buckets = append(resp.Buckets.Buckets, cmd.Bucket{
			Name:         bucketInfo.Name,
			CreationDate: bucketInfo.Created.UTC().Format(iso8601Milli),
		})
	}

	return resp
}

func generateCopyObjectPartResponse(partInfo cmd.PartInfo) cmd.CopyObjectPartResponse {
	return cmd.CopyObjectPartResponse{
		ETag:         "\"" + partInfo.ETag + "\"",
		LastModified: partInfo.LastModified.UTC().Format(iso8601Milli),
	}
}

// objectMetadataHeaders are the stored metadata keys, other than user-defined metadata, that
// are returned as object response headers. Other keys, including internal ones, are not
// returned.
var objectMetadataHeaders = []string{
	xhttp.CacheControl,
	xhttp.ContentDisposition,
	xhttp.ContentLanguage,
	xhttp.ContentEncoding,
	xhttp.Expires,
	xhttp.AmzStorageClass,
}

// objectLockMetadataPrefix is the prefix of the object lock metadata keys that the object layer
// returns in lower case.
const objectLockMetadataPrefix = "x-amz-object-lock-"

// setObjectHeaders sets the response headers describing an object, or the portion of it
// selected by rangeSpec or partNumber, for GetObject and HeadObject responses. Content-Range is
// set only for partial content, and partial reports whether it was set.
func setObjectHeaders(w http.ResponseWriter, objInfo cmd.ObjectInfo, rangeSpec *cmd.HTTPRangeSpec, partNumber int) (partial bool, err error) {
	setCommonHeaders(w)

	h := w.Header()
	if validModTime(objInfo.ModTime) {
		h.Set(xhttp.LastModified, objInfo.ModTime.UTC().Format(http.TimeFormat))
	}

	if objInfo.ETag != "" {
		h[xhttp.ETag] = []string{`"` + objInfo.ETag + `"`}
	}
	// Without a Content-Type, net/http would sniff one from the data, so browsers could render an
	// object as HTML. The default is the one PutObject stores.
	contentType := objInfo.ContentType
	if contentType == "" {
		contentType = "binary/octet-stream"
	}
	h.Set(xhttp.ContentType, contentType)
	if objInfo.ContentEncoding != "" {
		h.Set(xhttp.ContentEncoding, objInfo.ContentEncoding)
	}
	if !objInfo.Expires.IsZero() {
		h.Set(xhttp.Expires, objInfo.Expires.UTC().Format(http.TimeFormat))
	}

	userTags := objInfo.UserTags
	if userTags == "" {
		// The object layer keeps tags in the metadata instead of UserTags.
		userTags = objInfo.UserDefined["s3:tags"]
	}
	if userTags != "" {
		if objTags, _ := url.ParseQuery(userTags); len(objTags) > 0 {
			h[xhttp.AmzTagCount] = []string{strconv.Itoa(len(objTags))}
		}
	}

	for k, v := range objInfo.UserDefined {
		lowerK := strings.ToLower(k)
		// https://github.com/google/security-research/security/advisories/GHSA-76wf-9vgp-pj7w
		if strings.EqualFold(k, xhttp.AmzMetaUnencryptedContentLength) || strings.EqualFold(k, xhttp.AmzMetaUnencryptedContentMD5) {
			continue
		}
		if slices.ContainsFunc(userMetadataKeyPrefixes, func(prefix string) bool {
			return strings.HasPrefix(lowerK, strings.ToLower(prefix))
		}) {
			// User-defined metadata keys are returned in lowercase, like S3 does.
			h[lowerK] = []string{v}
			continue
		}
		if strings.HasPrefix(lowerK, objectLockMetadataPrefix) || slices.ContainsFunc(objectMetadataHeaders, func(header string) bool {
			return strings.EqualFold(k, header)
		}) {
			h.Set(k, v)
		}
	}

	// TODO: part 1 is the entire object, because the object layer doesn't report parts. It's
	// only a partial response if the object isn't empty.
	if partNumber > 0 && objInfo.Size > 0 {
		rangeSpec = &cmd.HTTPRangeSpec{Start: 0, End: -1}
	}

	start, length, err := rangeOffsetLength(rangeSpec, objInfo.Size)
	if err != nil {
		return false, err
	}
	h.Set(xhttp.ContentLength, strconv.FormatInt(length, 10))
	if rangeSpec != nil {
		h.Set(xhttp.ContentRange, fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, objInfo.Size))
	}

	if objInfo.VersionID != "" {
		h[xhttp.AmzVersionID] = []string{objInfo.VersionID}
	}

	return rangeSpec != nil, nil
}

// responseHeaderOverrides maps the query parameters that GetObject and HeadObject requests may
// use to override response headers to the headers they override.
var responseHeaderOverrides = map[string]string{
	"response-expires":             xhttp.Expires,
	"response-content-type":        xhttp.ContentType,
	"response-cache-control":       xhttp.CacheControl,
	"response-content-encoding":    xhttp.ContentEncoding,
	"response-content-language":    xhttp.ContentLanguage,
	"response-content-disposition": xhttp.ContentDisposition,
}

// setResponseHeaderOverrides sets the response headers requested by the response-* query
// parameters of a GetObject or HeadObject request.
func setResponseHeaderOverrides(w http.ResponseWriter, query url.Values) {
	// Parameter names are case-sensitive, like in S3.
	for param, header := range responseHeaderOverrides {
		if v := query[param]; len(v) > 0 {
			// Repeated query parameters must not produce repeated headers.
			w.Header()[header] = v[:1]
		}
	}
}
