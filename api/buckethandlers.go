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
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/amwolff/awsig"
	"github.com/gorilla/mux"
	"github.com/minio/minio-go/v7/pkg/tags"

	"storj.io/common/memory"
	"storj.io/common/uuid"
	"storj.io/gateway/api/apierr"
	"storj.io/minio/cmd"
	"storj.io/minio/cmd/crypto"
	xhttp "storj.io/minio/cmd/http"
	objectlock "storj.io/minio/pkg/bucket/object/lock"
	"storj.io/minio/pkg/bucket/versioning"
	"storj.io/minio/pkg/event"
	"storj.io/minio/pkg/hash"
)

const (
	// maxPutBucketNotificationConfigBodySize is the maximum size of the PutBucketNotificationConfiguration request body.
	maxPutBucketNotificationConfigBodySize = int64(memory.MiB)
	// maxPutBucketVersioningBodySize is the maximum size of the PutBucketVersioning request body.
	maxPutBucketVersioningBodySize = int64(memory.MiB)
	// maxPostObjectSize is the maximum size of the object contents submitted in a POST Object request.
	maxPostObjectSize = 5 * int64(memory.GB)
	// maxDeleteObjectsBodySize is the maximum size of a DeleteObjects request body.
	// This value is a little more than the theoretical worst-case size.
	maxDeleteObjectsBodySize = 21000000

	xAmzChecksumPrefix = "X-Amz-Checksum-"
	nullVersionID      = "null"
)

// CreateBucketHandler is the HTTP handler for the CreateBucket operation, which creates a bucket.
func (api *API) CreateBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "CreateBucket")

	bucket := mux.Vars(r)["bucket"]

	objectLockEnabled := false
	if vs, found := r.Header[http.CanonicalHeaderKey("x-amz-bucket-object-lock-enabled")]; found {
		switch strings.ToLower(vs[0]) {
		case "true":
			objectLockEnabled = true
		case "false":
		default:
			api.writeErrorResponse(w, r, apierr.CodeInvalidRequest)
		}
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

	// We read the body because, although we don't use the location provided in the body,
	// we must return an error if the body contains malformed XML.
	if r.ContentLength != 0 {
		type createBucketLocationConfiguration struct {
			XMLName  xml.Name `xml:"CreateBucketConfiguration"`
			Location string   `xml:"LocationConstraint"`
		}
		if err = xmlDecoder(body, &createBucketLocationConfiguration{}, r.ContentLength); err != nil {
			api.writeErrorResponseWithFallback(w, r, err, apierr.CodeMalformedXML)
			return
		}
	}

	opts := cmd.BucketOptions{
		LockEnabled: objectLockEnabled,
	}

	if err := api.objectAPI.MakeBucketWithLocation(ctx, bucket, opts); err != nil {
		api.writeErrorResponse(w, r, err)
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
		api.writeErrorResponse(w, r, err)
		return
	}

	// Amazon S3's documentation states that the "Content-Md5" header is required for this operation,
	// but testing has shown that this isn't true. Therefore, we don't require it, either.
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

	if _, err = api.objectAPI.GetBucketInfo(ctx, bucketName); err != nil {
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

// PutBucketNotificationConfigurationHandler is the HTTP handler for the PutBucketNotificationConfiguration
// operation, which sets a bucket's notification configuration.
func (api *API) PutBucketNotificationConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "PutBucketNotificationConfiguration")

	bucketName := mux.Vars(r)["bucket"]

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

	config, err := event.ParseConfig(io.LimitReader(body, maxPutBucketNotificationConfigBodySize))
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if err = api.objectAPI.SetBucketNotificationConfig(ctx, bucketName, config); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	writeSuccessResponseHeadersOnly(w)
}

// PutObjectLockConfigurationHandler is the HTTP handler for the PutObjectLockConfiguration
// operation, which places an Object Lock configuration on a bucket.
func (api *API) PutObjectLockConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "PutObjectLockConfiguration")

	bucketName := mux.Vars(r)["bucket"]

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

	config, err := objectlock.ParseObjectLockConfig(body)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if err = api.objectAPI.SetObjectLockConfig(ctx, bucketName, config); err != nil {
		api.writeErrorResponse(w, r, err)
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

	tags, err := tags.ParseBucketXML(io.LimitReader(body, r.ContentLength))
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if err = api.objectAPI.SetBucketTagging(ctx, bucketName, tags); err != nil {
		api.writeErrorResponse(w, r, err)
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

	v, err := versioning.ParseConfig(io.LimitReader(body, maxPutBucketVersioningBodySize))
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if err = api.objectAPI.SetBucketVersioning(ctx, bucketName, v); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	writeSuccessResponseHeadersOnly(w)
}

// HeadBucketHandler is the HTTP handler for the HeadBucket operation, which checks if a bucket can be accessed.
func (api *API) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "HeadBucket")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if _, err := api.objectAPI.GetBucketInfo(ctx, bucketName); err != nil {
		api.writeErrorResponseHeadersOnly(r, w, err)
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
		api.writeErrorResponse(w, r, err)
		return
	}

	if _, err := api.objectAPI.GetBucketInfo(ctx, bucketName); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	const accelerateDefaultConfig = `<?xml version="1.0" encoding="UTF-8"?><AccelerateConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`
	writeSuccessResponseXML(w, []byte(accelerateDefaultConfig))
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
		api.writeErrorResponse(w, r, err)
		return
	}

	if _, err := api.objectAPI.GetBucketInfo(ctx, bucketName); err != nil {
		api.writeErrorResponse(w, r, err)
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
		api.writeErrorResponse(w, r, err)
		return
	}

	w.(http.Flusher).Flush()
}

// GetBucketCorsHandler is the HTTP handler for the GetBucketCors operation,
// which returns a bucket's Cross-Origin Resource Sharing (CORS) configuration.
//
// This is a dummy handler. It always returns a NoSuchCORSConfiguration error.
func (api *API) GetBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "GetBucketCors")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if _, err := api.objectAPI.GetBucketInfo(ctx, bucketName); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeErrorResponse(w, r, apierr.CodeNoSuchCORSConfiguration)
}

// GetBucketLocationHandler is the HTTP handler for the GetBucketLocation operation,
// which returns the location that a bucket resides in.
//
// This is a dummy handler. It always returns an empty Location response.
func (api *API) GetBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "GetBucketLocation")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if _, err := api.objectAPI.GetBucketInfo(ctx, bucketName); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	resp, err := encodeResponse(cmd.LocationResponse{})
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	writeSuccessResponseXML(w, resp)
}

// GetBucketLoggingHandler is the HTTP handler for the GetBucketLogging operation,
// which returns the logging status of a bucket.
//
// This is a dummy handler. It always returns an empty Location response.
func (api *API) GetBucketLoggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "GetBucketLogging")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if _, err := api.objectAPI.GetBucketInfo(ctx, bucketName); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	const loggingDefaultConfig = `<?xml version="1.0" encoding="UTF-8"?><BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><!--<LoggingEnabled><TargetBucket>myLogsBucket</TargetBucket><TargetPrefix>add/this/prefix/to/my/log/files/access_log-</TargetPrefix></LoggingEnabled>--></BucketLoggingStatus>`
	writeSuccessResponseXML(w, []byte(loggingDefaultConfig))
}

// GetBucketNotificationConfigurationHandler is the HTTP handler for the GetBucketNotificationConfiguration
// operation, which returns a bucket's notification configuration.
func (api *API) GetBucketNotificationConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "GetBucketNotificationConfiguration")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	config, err := api.objectAPI.GetBucketNotificationConfig(ctx, bucketName)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	configData, err := xml.Marshal(config)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	writeSuccessResponseXML(w, configData)
}

// GetObjectLockConfigurationHandler is the HTTP handler for the GetObjectLockConfiguration operation,
// which sets a bucket's Object Lock configuration.
func (api *API) GetObjectLockConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "GetObjectLockConfiguration")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	config, err := api.objectAPI.GetObjectLockConfig(ctx, bucketName)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	configData, err := xml.Marshal(config)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	writeSuccessResponseXML(w, configData)
}

// GetBucketPolicyStatusHandler is the HTTP handler for the GetBucketPolicyStatus operation,
// which returns whether a bucket is public.
//
// This is a dummy handler. It always returns a result indicating that a bucket is not public.
func (api *API) GetBucketPolicyStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "GetBucketPolicyStatus")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if _, err := api.objectAPI.GetBucketInfo(ctx, bucketName); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	encodedSuccessResponse, err := encodeResponse(cmd.PolicyStatus{
		// Our buckets are never public.
		IsPublic: "FALSE",
	})
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	writeSuccessResponseXML(w, encodedSuccessResponse)
}

// GetBucketRequestPaymentHandler is the HTTP handler for the GetBucketRequestPayment operation,
// which returns a bucket's Requester Pays configuration.
//
// This is a dummy handler. It always returns an empty Requester Pays configuration.
func (api *API) GetBucketRequestPaymentHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "GetBucketRequestPayment")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if _, err := api.objectAPI.GetBucketInfo(ctx, bucketName); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	const requestPaymentDefaultConfig = `<?xml version="1.0" encoding="UTF-8"?><RequestPaymentConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Payer>BucketOwner</Payer></RequestPaymentConfiguration>`
	writeSuccessResponseXML(w, []byte(requestPaymentDefaultConfig))
}

// GetBucketTaggingHandler is the HTTP handler for the GetBucketTagging operation,
// which returns the set of tags associated with a bucket.
func (api *API) GetBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "GetBucketTagging")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	tags, err := api.objectAPI.GetBucketTagging(ctx, bucketName)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	responseBytes, err := xml.Marshal(tags)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	writeSuccessResponseXML(w, responseBytes)
}

// GetBucketVersioningHandler is the HTTP handler for the GetBucketVersioning operation,
// which returns a bucket's versioning configuration.
func (api *API) GetBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "GetBucketVersioning")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	config, err := api.objectAPI.GetBucketVersioning(ctx, bucketName)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	configData, err := xml.Marshal(config)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	writeSuccessResponseXML(w, configData)
}

// PostObjectHandler uploads an object to a bucket using an HTML form with a policy document.
func (api *API) PostObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "PostObject")

	if _, requested := crypto.IsRequested(r.Header); requested {
		api.writeErrorResponse(w, r, apierr.CodeNotImplemented)
		return
	}

	bucketName := mux.Vars(r)["bucket"]

	if r.ContentLength < 0 {
		api.writeErrorResponse(w, r, apierr.CodeMissingContentLength)
		return
	}

	// Make sure that the URL does not contain an object name.
	resource := getResource(r)
	if bucketName != path.Clean(resource[1:]) {
		api.writeErrorResponse(w, r, apierr.CodeMethodNotAllowed)
		return
	}

	vr, err := api.verifier.Verify(r, getVirtualHostedBucket(r))
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	postForm := vr.PostForm()

	for formKey := range postForm {
		if strings.HasPrefix(http.CanonicalHeaderKey(formKey), xAmzChecksumPrefix) {
			// TODO: Support checksum options
			api.writeErrorResponse(w, r, apierr.CodeChecksumsUnsupported)
			return
		}
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

	awsigReader, err := vr.Reader(checksumReqs...)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	hashReader := hash.NewAwsigReader(NewLimitedAwsigReader(awsigReader, maxPostObjectSize), -1, -1)

	policyBytes, err := base64.StdEncoding.DecodeString(postForm.Get("policy").Value)
	if err != nil {
		api.writeErrorResponse(w, r, apierr.CodeMalformedPOSTRequest)
		return
	}

	var postPolicy PostPolicy
	if err := json.Unmarshal(policyBytes, &postPolicy); err != nil {
		api.writeErrorResponseWithFallback(w, r, err, apierr.CodePostPolicyInvalidJSON)
		return
	}

	if err = CheckPostForm(postPolicy, postForm); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	successRedirect := getPostFormValue(postForm, "success_action_redirect")
	successStatus := getPostFormValue(postForm, "success_action_status")
	var redirectURL *url.URL
	if successRedirect != "" {
		redirectURL, _ = url.Parse(successRedirect)
		if redirectURL != nil && (redirectURL.Host == "" || redirectURL.Scheme == "") {
			redirectURL = nil
		}
	}

	metadata, err := ExtractMetadataFromPostForm(postForm)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if postForm.Has("tagging") {
		tagsReader := strings.NewReader(getPostFormValue(postForm, "tagging"))
		objectTags, err := tags.ParseObjectXML(tagsReader)
		if err != nil {
			api.writeErrorResponse(w, r, err)
			return
		}
		metadata[xhttp.AmzObjectTagging] = objectTags.String()
	}

	opts := cmd.ObjectOptions{
		UserDefined: metadata,
	}

	for _, cond := range postPolicy.Conditions.Items {
		opts.PostPolicy.Conditions.Items = append(opts.PostPolicy.Conditions.Items, cmd.PostPolicyCondition{
			Operator: string(cond.Operator),
			Key:      cond.Key,
			Value:    cond.Value,
		})
	}
	opts.PostPolicy.Conditions.ContentLengthRange = cmd.ContentLengthRange(postPolicy.Conditions.ContentLengthRange)
	opts.PostPolicy.Expiration = postPolicy.Expiration.Time

	pReader := cmd.NewPutObjReader(hashReader)

	objectKey := strings.ReplaceAll(postForm.Get("key").Value, "${filename}", postForm.FileName())
	objInfo, err := api.objectAPI.PutObject(ctx, bucketName, objectKey, pReader, opts)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	// We must not use the http.Header().Set method here because some (broken)
	// clients expect the ETag header key to be literally "ETag" - not "Etag" (case-sensitive).
	// Therefore, we have to set the ETag directly as a map entry.
	w.Header()[xhttp.ETag] = []string{`"` + objInfo.ETag + `"`}

	if objInfo.VersionID != "" {
		w.Header()[xhttp.AmzVersionID] = []string{objInfo.VersionID}
	}

	w.Header().Set(xhttp.Location, GetObjectURL(r, objectKey))

	if successRedirect != "" {
		queryValues := make(url.Values)
		queryValues.Set("bucket", objInfo.Bucket)
		queryValues.Set("key", objInfo.Name)
		queryValues.Set("etag", "\""+objInfo.ETag+"\"")
		redirectURL.RawQuery = queryValues.Encode()
		writeRedirectSeeOther(w, redirectURL.String())
		return
	}

	switch successStatus {
	case "201":
		resp, err := encodeResponse(cmd.PostResponse{
			Bucket:   objInfo.Bucket,
			Key:      objInfo.Name,
			ETag:     `"` + objInfo.ETag + `"`,
			Location: w.Header().Get(xhttp.Location),
		})
		if err != nil {
			api.writeErrorResponse(w, r, err)
			return
		}
		writeResponse(w, http.StatusCreated, resp, mimeXML)
	case "200":
		writeSuccessResponseHeadersOnly(w)
	default:
		writeSuccessNoContent(w)
	}
}

// DeleteObjectsHandler is the HTTP handler for the DeleteObject operation, which deletes multiple objects
// from a bucket.
func (api *API) DeleteObjectsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "DeleteObjects")

	bucketName := mux.Vars(r)["bucket"]

	vr, err := api.verifier.Verify(r, getVirtualHostedBucket(r))
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	for header := range r.Header {
		if strings.HasPrefix(header, xAmzChecksumPrefix) {
			// TODO: Support checksum options
			api.writeErrorResponse(w, r, apierr.CodeChecksumsUnsupported)
			return
		}
	}

	checksumReq, present, err := getContentMD5ChecksumRequest(r.Header)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}
	if !present {
		api.writeErrorResponse(w, r, apierr.CodeMissingContentMD5AndChecksum)
		return
	}

	body, err := vr.Reader(checksumReq)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if r.ContentLength <= 0 {
		api.writeErrorResponse(w, r, apierr.CodeMissingContentLength)
		return
	}

	deleteReq := &cmd.DeleteObjectsRequest{}
	if err := xmlDecoder(body, deleteReq, maxDeleteObjectsBodySize); err != nil {
		api.writeErrorResponseWithFallback(w, r, err, apierr.CodeMalformedXML)
		return
	}

	if len(deleteReq.Objects) == 0 {
		api.writeErrorResponse(w, r, apierr.CodeMalformedXML)
		return
	}

	for i := range deleteReq.Objects {
		deleteReq.Objects[i].ObjectName = trimLeadingSlash(deleteReq.Objects[i].ObjectName)
	}

	type bucketObjectLocation struct {
		key       string
		versionID string
	}

	seen := make(map[bucketObjectLocation]struct{}, len(deleteReq.Objects))

	var filteredIndex int

	deleteErrs := make([]cmd.DeleteError, 0, len(deleteReq.Objects))
	for _, object := range deleteReq.Objects {
		loc := bucketObjectLocation{
			key:       object.ObjectName,
			versionID: object.VersionID,
		}

		if _, ok := seen[loc]; ok {
			continue
		}
		seen[loc] = struct{}{}

		if object.VersionID != "" && object.VersionID != nullVersionID {
			if _, err := uuid.FromString(object.VersionID); err != nil {
				resp, _ := apierr.CodeNoSuchVersion.ToResponse()
				deleteErrs = append(deleteErrs, cmd.DeleteError{
					Code:      resp.Code,
					Message:   resp.Description,
					Key:       object.ObjectName,
					VersionID: object.VersionID,
				})
				continue
			}
		}

		deleteReq.Objects[filteredIndex] = object
		filteredIndex++
	}
	deleteReq.Objects = deleteReq.Objects[:filteredIndex]

	deletedObjects, apiDeleteErrs, err := api.objectAPI.DeleteObjects(ctx, bucketName, deleteReq.Objects, cmd.ObjectOptions{
		BypassGovernanceRetention: objectlock.IsObjectLockGovernanceBypassSet(r.Header),
		Quiet:                     deleteReq.Quiet,
	})
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	var internalErrs []error
	for _, deleteErr := range apiDeleteErrs {
		resp, ok := errToResponse(err)
		if !ok {
			internalErrs = append(internalErrs, err)
			resp, _ = apierr.CodeInternal.ToResponse()
		}
		deleteErrs = append(deleteErrs, cmd.DeleteError{
			Code:      resp.Code,
			Message:   resp.Description,
			Key:       deleteErr.ObjectName,
			VersionID: deleteErr.VersionID,
		})
	}

	if len(internalErrs) > 0 {
		api.log.Error(r, "unexpected errors", internalErrs...)
	}

	encodedSuccessResponse, err := encodeResponse(cmd.DeleteObjectsResponse{
		DeletedObjects: deletedObjects,
		Errors:         deleteErrs,
	})
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	writeSuccessResponseXML(w, encodedSuccessResponse)
}

// DeleteBucketHandler is the HTTP handler for the DeleteBucket operation, which deletes a bucket.
func (api *API) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "DeleteBucket")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	forceDelete := false
	if value := r.Header.Get(xhttp.MinIOForceDelete); value != "" {
		var err error
		forceDelete, err = strconv.ParseBool(value)
		if err != nil {
			api.writeErrorResponse(w, r, apierr.CodeInvalidForceDelete)
			return
		}
	}

	if err := api.objectAPI.DeleteBucket(ctx, bucketName, forceDelete); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	writeSuccessNoContent(w)
}

// DeleteBucketTaggingHandler is the HTTP handler for the DeleteBucketTagging operation,
// which removes the set of tags that have been placed on a bucket.
func (api *API) DeleteBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "DeleteBucketTagging")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if err := api.objectAPI.SetBucketTagging(ctx, bucketName, nil); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	writeSuccessNoContent(w)
}
