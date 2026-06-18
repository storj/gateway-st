// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.
// This file incorporates code from MinIO Cloud Storage and includes changes made by Storj Labs, Inc.

/*
 * MinIO Cloud Storage, (C) 2015-2020 MinIO, Inc.
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
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/minio/minio-go/v7/pkg/tags"

	"storj.io/common/memory"
	"storj.io/gateway/api/apierr"
	"storj.io/minio/cmd"
	"storj.io/minio/cmd/crypto"
	xhttp "storj.io/minio/cmd/http"
	objectlock "storj.io/minio/pkg/bucket/object/lock"
	"storj.io/minio/pkg/hash"
)

const (
	maxPartSize   = 5 * int64(memory.GiB)
	minPartNumber = 1
	maxPartNumber = 10000
)

// PutObjectAclHandler is the HTTP handler for the PutObjectAcl operation,
// which sets an object's access control list.
func (api *API) PutObjectAclHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "PutObjectAcl")

	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectKey, err := unescapePath(vars["object"])
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	body, err := api.verifyWithBody(r, false)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	_, err = api.objectAPI.GetObjectInfo(ctx, bucketName, objectKey, cmd.ObjectOptions{})
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	aclHeader := r.Header.Get(xhttp.AmzACL)
	if aclHeader == "" {
		acl := &accessControlPolicy{}
		if err = xmlDecoder(body, acl, r.ContentLength); err != nil {
			if errors.Is(err, io.EOF) {
				api.writeErrorResponse(w, r, apierr.CodeMissingSecurityHeader)
				return
			}
			api.writeErrorResponseWithFallback(w, r, err, apierr.CodeMalformedXML)
			return
		}

		if len(acl.AccessControlList.Grants) == 0 {
			api.writeErrorResponse(w, r, apierr.CodeNotImplemented)
			return
		}

		if acl.AccessControlList.Grants[0].Permission != "FULL_CONTROL" {
			api.writeErrorResponse(w, r, apierr.CodeNotImplemented)
			return
		}
	}

	if aclHeader != "" && aclHeader != "private" {
		api.writeErrorResponse(w, r, apierr.CodeNotImplemented)
		return
	}

	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

// PutObjectLegalHoldHandler is the HTTP handler for the PutObjectLegalHold operation,
// which sets an object's legal hold configuration.
func (api *API) PutObjectLegalHoldHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "PutObjectLegalHold")

	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectKey, err := unescapePath(vars["object"])
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	body, err := api.verifyWithBody(r, true)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	legalHold, err := objectlock.ParseObjectLegalHold(body)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	versionID, err := extractVersionID(r.URL.Query())
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if err = api.objectAPI.SetObjectLegalHold(ctx, bucketName, objectKey, versionID, legalHold); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeSuccessResponseHeadersOnly(w, r)
}

// PutObjectRetentionHandler is the HTTP handler for the PutObjectRetention operation,
// which sets an object's retention configuration.
func (api *API) PutObjectRetentionHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "PutObjectRetention")

	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectKey, err := unescapePath(vars["object"])
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	body, err := api.verifyWithBody(r, true)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	retention, err := objectlock.ParseObjectRetention(body)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	// if requesting governance bypass, object layer only removes the active
	// retention if retention is nil.
	governanceBypassSet := objectlock.IsObjectLockGovernanceBypassSet(r.Header)
	if governanceBypassSet && retention.Mode == "" && retention.RetainUntilDate.IsZero() {
		retention = nil
	}

	versionID, err := extractVersionID(r.URL.Query())
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if err = api.objectAPI.SetObjectRetention(ctx, bucketName, objectKey, versionID, cmd.ObjectOptions{
		Retention:                 retention,
		BypassGovernanceRetention: governanceBypassSet,
	}); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeSuccessResponseHeadersOnly(w, r)
}

// PutObjectTaggingHandler is the HTTP handler for the PutObjectTagging operation,
// which places a set of tags on an object.
func (api *API) PutObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "PutObjectTagging")

	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectKey, err := unescapePath(vars["object"])
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	body, err := api.verifyWithBody(r, false)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	tags, err := tags.ParseObjectXML(io.LimitReader(body, r.ContentLength))
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	versionID, err := extractVersionID(r.URL.Query())
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	objInfo, err := api.objectAPI.PutObjectTags(ctx, bucketName, objectKey, tags.String(), cmd.ObjectOptions{
		VersionID: versionID,
	})
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if objInfo.VersionID != "" {
		w.Header()[xhttp.AmzVersionID] = []string{objInfo.VersionID}
	}

	api.writeSuccessResponseHeadersOnly(w, r)
}

// UploadPartHandler is the HTTP handler for the UploadPart operation, which uploads a part
// of a multipart upload.
func (api *API) UploadPartHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "UploadPart")

	// Reject UploadPartCopy requests that may have been routed here due to a misconfiguration.
	if _, ok := r.Header[xhttp.AmzCopySource]; ok {
		api.writeErrorResponse(w, r, apierr.CodeNotImplemented)
		return
	}

	if _, requested := crypto.IsRequested(r.Header); requested {
		api.writeErrorResponse(w, r, apierr.CodeNotImplemented)
		return
	}

	for header := range r.Header {
		if strings.HasPrefix(header, xAmzChecksumPrefix) {
			// TODO: Support checksum options
			api.writeErrorResponse(w, r, apierr.CodeChecksumsUnsupported)
			return
		}
	}

	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectKey, err := unescapePath(vars["object"])
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	body, err := api.verifyWithBody(r, false)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	size := r.ContentLength

	if isStreamingSigV4(r) {
		size, err = strconv.ParseInt(r.Header.Get(xhttp.AmzDecodedContentLength), 10, 64)
		if err != nil {
			// This shouldn't happen here as this case is handled previously by awsig's validation
			api.writeErrorResponse(w, r, err)
			return
		}
	}

	if size == -1 {
		api.writeErrorResponse(w, r, apierr.CodeMissingContentLength)
		return
	}

	if size > maxPartSize {
		api.writeErrorResponse(w, r, apierr.CodeEntityTooLarge)
		return
	}

	uploadID := r.URL.Query().Get(xhttp.UploadID)

	partNumStr := r.URL.Query().Get(xhttp.PartNumber)
	partNumber, err := strconv.Atoi(partNumStr)
	if err != nil || partNumber < minPartNumber || partNumber > maxPartNumber {
		api.writeErrorResponse(w, r, apierr.CodeInvalidPartNumber)
		return
	}

	putObjReader := cmd.NewPutObjReader(hash.NewAwsigReader(body, size, size))
	partInfo, err := api.objectAPI.PutObjectPart(ctx, bucketName, objectKey, uploadID, partNumber, putObjReader, cmd.ObjectOptions{})
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	// We must not use the http.Header().Set method here because some (broken)
	// clients expect the ETag header key to be literally "ETag" - not "Etag" (case-sensitive).
	// Therefore, we have to set the ETag directly as a map entry.
	w.Header()[xhttp.ETag] = []string{"\"" + partInfo.ETag + "\""}

	api.writeSuccessResponseHeadersOnly(w, r)
}

// UploadPartCopyHandler is the HTTP handler for the UploadPartCopyHandler operation,
// which uploads a part of a multipart upload using an existing object as a data source.
func (api *API) UploadPartCopyHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "UploadPartCopy")

	for _, header := range []string{
		http.CanonicalHeaderKey(xhttp.AmzCopySourceIfModifiedSince),
		http.CanonicalHeaderKey(xhttp.AmzCopySourceIfUnmodifiedSince),
		http.CanonicalHeaderKey(xhttp.AmzCopySourceIfNoneMatch),
		http.CanonicalHeaderKey(xhttp.AmzCopySourceIfMatch),
		xhttp.AmzServerSideEncryptionCustomerAlgorithm,
		xhttp.AmzServerSideEncryptionCustomerKey,
		xhttp.AmzServerSideEncryptionCustomerKeyMD5,
		xhttp.AmzServerSideEncryptionCopyCustomerAlgorithm,
		xhttp.AmzServerSideEncryptionCopyCustomerKey,
		xhttp.AmzServerSideEncryptionCopyCustomerKeyMD5,
		"X-Amz-Request-Payer",
		"X-Amz-Expected-Bucket-Owner",
		"X-Amz-Source-Expected-Bucket-Owner",
	} {
		if _, ok := r.Header[header]; ok {
			api.writeErrorResponse(w, r, apierr.CodeNotImplemented)
			return
		}
	}

	for header := range r.Header {
		if strings.HasPrefix(header, xAmzChecksumPrefix) {
			api.writeErrorResponse(w, r, apierr.CodeChecksumsUnsupported)
			return
		}
	}

	vars := mux.Vars(r)
	dstBucket := vars["bucket"]
	dstObject, err := unescapePath(vars["object"])
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	cpSrcPath := r.Header.Get(xhttp.AmzCopySource)
	var srcVersionID string
	if u, err := url.Parse(cpSrcPath); err == nil {
		srcVersionID = strings.TrimSpace(u.Query().Get(xhttp.VersionID))
		// Note that url.Parse does the unescaping
		cpSrcPath = u.Path
	}

	srcBucket, srcObject := splitCopySourcePath(cpSrcPath)
	if srcObject == "" || srcBucket == "" {
		api.writeErrorResponse(w, r, apierr.CodeInvalidCopySource)
		return
	}

	uploadID := r.URL.Query().Get(xhttp.UploadID)

	partNumberStr := r.URL.Query().Get(xhttp.PartNumber)
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < minPartNumber || partNumber > maxPartNumber {
		api.writeErrorResponse(w, r, apierr.CodeInvalidPartNumber)
		return
	}

	startOffset, length := int64(0), int64(-1)

	if rangeHeader := r.Header.Get(xhttp.AmzCopySourceRange); rangeHeader != "" {
		if rangeSpec, err := parseRangeForCopy(rangeHeader); err != nil {
			api.writeErrorResponse(w, r, err)
			return
		} else if rangeSpec != nil {
			startOffset, length = rangeSpec.Start, rangeSpec.End-rangeSpec.Start+1
		}
	}

	if length > maxPartSize {
		api.writeErrorResponse(w, r, apierr.CodeEntityTooLarge)
		return
	}

	partInfo, err := api.objectAPI.CopyObjectPart(
		ctx,
		srcBucket, srcObject, dstBucket, dstObject, uploadID,
		partNumber,
		startOffset, length,
		cmd.ObjectInfo{},
		cmd.ObjectOptions{VersionID: srcVersionID}, cmd.ObjectOptions{},
	)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	response := generateCopyObjectPartResponse(partInfo)
	encodedSuccessResponse, err := encodeResponse(response)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if srcVersionID != "" {
		w.Header().Set(xhttp.AmzCopySourceVersionID, srcVersionID)
	}

	api.writeSuccessResponseXML(w, r, encodedSuccessResponse)
}
