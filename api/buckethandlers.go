// Copyright (C) 2025 Storj Labs, Inc.
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
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/minio/minio-go/v7/pkg/tags"

	"storj.io/common/memory"
	"storj.io/minio/cmd"
	xhttp "storj.io/minio/cmd/http"
	objectlock "storj.io/minio/pkg/bucket/object/lock"
	"storj.io/minio/pkg/bucket/versioning"
)

// maxPutBucketVersioningBodySize is the maximum size of the PutBucketVersioning request body.
const maxPutBucketVersioningBodySize = int64(memory.MiB)

// PutBucketHandler is the HTTP handler for the PutBucket operation, which creates a bucket.
func (api *API) PutBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "PutBucket")

	bucket := mux.Vars(r)["bucket"]

	objectLockEnabled := false
	if vs, found := r.Header[http.CanonicalHeaderKey("x-amz-bucket-object-lock-enabled")]; found {
		switch strings.ToLower(vs[0]) {
		case "true":
			objectLockEnabled = true
		case "false":
		default:
			writeErrorResponse(ctx, w, cmd.GetAPIError(cmd.ErrInvalidRequest), r.URL)
		}
	}

	vr, err := api.verifier.Verify(r, getVirtualHostedBucket(r))
	if err != nil {
		writeErrorResponse(ctx, w, awsigToAPIError(err), r.URL)
		return
	}

	body, err := vr.Reader()
	if err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	// We read the body because, although we don't use the location provided in the body,
	// we must return an error if the body contains malformed XML.
	if r.ContentLength != 0 {
		type createBucketLocationConfiguration struct {
			XMLName  xml.Name `xml:"CreateBucketConfiguration"`
			Location string   `xml:"LocationConstraint"`
		}
		if err = xmlDecoder(body, &createBucketLocationConfiguration{}, r.ContentLength); err != nil {
			writeErrorResponse(ctx, w, cmd.GetAPIError(cmd.ErrMalformedXML), r.URL)
			return
		}
	}

	opts := cmd.BucketOptions{
		LockEnabled: objectLockEnabled,
	}

	if err := api.objectAPI.MakeBucketWithLocation(ctx, bucket, opts); err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	// Return the path where the client can reach the newly-created bucket,
	// which is the same as the path used to perform this operation.
	if cp := pathClean(r.URL.Path); cp != "" {
		w.Header().Set(xhttp.Location, cp)
	}

	writeSuccessResponseHeadersOnly(w)
}

// PutBucketAclHandler is the HTTP handler for the PutBucketAcl operation,
// which sets a bucket's access control list.
func (api *API) PutBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "PutBucketAcl")

	bucketName := mux.Vars(r)["bucket"]

	vr, err := api.verifier.Verify(r, getVirtualHostedBucket(r))
	if err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	body, err := vr.Reader()
	if err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	if _, err = api.objectAPI.GetBucketInfo(ctx, bucketName); err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	aclHeader := r.Header.Get(xhttp.AmzACL)
	if aclHeader == "" {
		acl := &accessControlPolicy{}
		if err = xmlDecoder(body, acl, r.ContentLength); err != nil {
			if errors.Is(err, io.EOF) {
				writeErrorResponse(ctx, w, cmd.GetAPIError(cmd.ErrMissingSecurityHeader), r.URL)
				return
			}
			writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
			return
		}

		if len(acl.AccessControlList.Grants) == 0 {
			writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, cmd.NotImplemented{}), r.URL)
			return
		}

		if acl.AccessControlList.Grants[0].Permission != "FULL_CONTROL" {
			writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, cmd.NotImplemented{}), r.URL)
			return
		}
	}

	if aclHeader != "" && aclHeader != "private" {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, cmd.NotImplemented{}), r.URL)
		return
	}

	w.(http.Flusher).Flush()
}

// PutBucketObjectLockConfigHandler is the HTTP handler for the PutBucketObjectLockConfig operation,
// which places an Object Lock configuration on a bucket.
func (api *API) PutBucketObjectLockConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "PutBucketObjectLockConfig")

	bucketName := mux.Vars(r)["bucket"]

	vr, err := api.verifier.Verify(r, getVirtualHostedBucket(r))
	if err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	body, err := vr.Reader()
	if err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	config, err := objectlock.ParseObjectLockConfig(body)
	if err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	if err = api.objectAPI.SetObjectLockConfig(ctx, bucketName, config); err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	writeSuccessResponseHeadersOnly(w)
}

// PutBucketTaggingHandler is the HTTP handler for the PutBucketTagging operation,
// which places a set of tags on a bucket.
func (api *API) PutBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "PutBucketTagging")

	bucketName := mux.Vars(r)["bucket"]

	vr, err := api.verifier.Verify(r, getVirtualHostedBucket(r))
	if err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	body, err := vr.Reader()
	if err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	tags, err := tags.ParseBucketXML(io.LimitReader(body, r.ContentLength))
	if err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	if err = api.objectAPI.SetBucketTagging(ctx, bucketName, tags); err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	writeSuccessResponseHeadersOnly(w)
}

// PutBucketVersioningHandler is the HTTP handler for the PutBucketVersioning operation,
// which sets a bucket's object versioning configuration.
func (api *API) PutBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "PutBucketVersioning")

	bucketName := mux.Vars(r)["bucket"]

	vr, err := api.verifier.Verify(r, getVirtualHostedBucket(r))
	if err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	body, err := vr.Reader()
	if err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	v, err := versioning.ParseConfig(io.LimitReader(body, maxPutBucketVersioningBodySize))
	if err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	if err = api.objectAPI.SetBucketVersioning(ctx, bucketName, v); err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	writeSuccessResponseHeadersOnly(w)
}

// HeadBucketHandler is the HTTP handler for the HeadBucket operation, which checks if a bucket can be accessed.
func (api *API) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "HeadBucket")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	if _, err := api.objectAPI.GetBucketInfo(ctx, bucketName); err != nil {
		writeErrorResponseHeadersOnly(w, cmd.ToAPIError(ctx, err))
		return
	}

	writeSuccessResponseHeadersOnly(w)
}

// GetBucketAccelerateHandler is the HTTP handler for the GetBucketAccelerate operation,
// which returns a bucket's Transfer Acceleration configuration.
//
// This is a dummy handler. It always returns an empty Transfer Acceleration configuration
// because we do not support placing them on buckets.
func (api *API) GetBucketAccelerateHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "GetBucketAccelerate")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	if _, err := api.objectAPI.GetBucketInfo(ctx, bucketName); err != nil {
		writeErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL)
		return
	}

	const accelerateDefaultConfig = `<?xml version="1.0" encoding="UTF-8"?><AccelerateConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`
	cmd.WriteSuccessResponseXML(w, []byte(accelerateDefaultConfig))
}

// GetBucketAclHandler is the HTTP handler for the GetBucketAcl operation,
// which returns a bucket's access control list.
//
// This is a dummy handler. It always returns a default access control list
// because we do not support placing them on buckets.
func (api *API) GetBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "GetBucketAcl")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		cmd.WriteErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL, false)
		return
	}

	if _, err := api.objectAPI.GetBucketInfo(ctx, bucketName); err != nil {
		cmd.WriteErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL, false)
		return
	}

	acl := &accessControlPolicy{}
	acl.AccessControlList.Grants = append(acl.AccessControlList.Grants, grant{
		Grantee: grantee{
			XMLNS:  "http://www.w3.org/2001/XMLSchema-instance",
			XMLXSI: "CanonicalUser",
			Type:   "CanonicalUser",
		},
		Permission: "FULL_CONTROL",
	})

	if err := xml.NewEncoder(w).Encode(acl); err != nil {
		cmd.WriteErrorResponse(ctx, w, cmd.ToAPIError(ctx, err), r.URL, false)
		return
	}

	w.(http.Flusher).Flush()
}
