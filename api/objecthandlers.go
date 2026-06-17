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

	"github.com/amwolff/awsig"
	"github.com/gorilla/mux"

	"storj.io/gateway/api/apierr"
	"storj.io/minio/cmd"
	xhttp "storj.io/minio/cmd/http"
	objectlock "storj.io/minio/pkg/bucket/object/lock"
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

	vr, err := api.verifier.Verify(r, getVirtualHostedBucket(r))
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	var checksumReqs []awsig.ChecksumRequest
	checksumReq, hasContentMD5, err := getContentMD5ChecksumRequest(r.Header)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}
	if hasContentMD5 {
		checksumReqs = append(checksumReqs, checksumReq)
	}

	body, err := vr.Reader(checksumReqs...)
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

	vr, err := api.verifier.Verify(r, getVirtualHostedBucket(r))
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	var checksumReqs []awsig.ChecksumRequest
	checksumReq, hasContentMD5, err := getContentMD5ChecksumRequest(r.Header)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}
	if !hasContentMD5 {
		api.writeErrorResponse(w, r, apierr.CodeMissingContentMD5)
		return
	}
	checksumReqs = append(checksumReqs, checksumReq)

	body, err := vr.Reader(checksumReqs...)
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

	writeSuccessResponseHeadersOnly(w)
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

	vr, err := api.verifier.Verify(r, getVirtualHostedBucket(r))
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	var checksumReqs []awsig.ChecksumRequest
	checksumReq, hasContentMD5, err := getContentMD5ChecksumRequest(r.Header)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}
	if !hasContentMD5 {
		api.writeErrorResponse(w, r, apierr.CodeMissingContentMD5)
		return
	}
	checksumReqs = append(checksumReqs, checksumReq)

	body, err := vr.Reader(checksumReqs...)
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

	writeSuccessResponseHeadersOnly(w)
}
