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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/gorilla/mux"

	"storj.io/common/memory"
	"storj.io/gateway/api/apierr"
	"storj.io/gateway/api/objectlayer"
	"storj.io/minio/cmd"
	"storj.io/minio/cmd/config/storageclass"
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
	// maxPutBucketTaggingBodySize is the maximum size of the PutBucketTagging request body.
	maxPutBucketTaggingBodySize = int64(memory.MiB)
	// maxPutBucketVersioningBodySize is the maximum size of the PutBucketVersioning request body.
	maxPutBucketVersioningBodySize = int64(memory.MiB)
	// maxConfigBodySize is the maximum size of other XML configuration request bodies,
	// such as ACLs, object lock settings and CreateBucket's location configuration.
	maxConfigBodySize = int64(memory.MiB)
	// maxPostObjectSize is the maximum size of the object contents submitted in a POST Object request.
	maxPostObjectSize = 5 * int64(memory.GB)
	// maxDeleteObjectsBodySize is the maximum size of a DeleteObjects request body.
	// This value is a little more than the theoretical worst-case size.
	maxDeleteObjectsBodySize = 21000000
	// maxDeleteList is the maximum number of objects that can be deleted in a DeleteObjects request.
	maxDeleteList = 1000

	xAmzChecksumPrefix = "X-Amz-Checksum-"
	nullVersionID      = "null"

	// bucketRegion is the region of a bucket whose placement has no location, which
	// GetBucketLocation reports as empty like S3 does for us-east-1. ListBuckets' bucket-region
	// filter assumes it for every bucket, because looking up each bucket's location would take a
	// satellite request per bucket.
	bucketRegion = "us-east-1"
)

// CreateBucketHandler is the HTTP handler for the CreateBucket operation, which creates a bucket.
func (api *API) CreateBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "CreateBucket")

	bucket := mux.Vars(r)["bucket"]

	body, err := api.verifyWithBody(r, false)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	objectLockEnabled := false
	if vs, found := r.Header[http.CanonicalHeaderKey("x-amz-bucket-object-lock-enabled")]; found {
		switch strings.ToLower(vs[0]) {
		case "true":
			objectLockEnabled = true
		case "false":
		default:
			api.writeErrorResponse(w, r, apierr.CodeInvalidRequest)
			return
		}
	}

	data, err := readVerifiedBody(body, maxConfigBodySize)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	// We read the body because, although we don't use the location provided in the body,
	// we must return an error if the body contains malformed XML.
	if len(data) != 0 {
		// Like S3, any root element is accepted, but unknown and repeated elements are refused.
		// The location and tags aren't used.
		var config struct {
			Text               string `xml:",chardata"`
			LocationConstraint []string
			Tags               []createBucketTags
			Unknown            unknownElements `xml:",any"`
		}
		if err = xmlDecoder(bytes.NewReader(data), &config); err != nil {
			api.writeErrorResponseWithFallback(w, r, err, apierr.CodeMalformedXML)
			return
		}
		if len(config.LocationConstraint) > 1 || len(config.Tags) > 1 || len(config.Unknown) > 0 || hasText(config.Text) {
			api.writeErrorResponse(w, r, apierr.CodeMalformedXML)
			return
		}
		for _, bucketTags := range config.Tags {
			if err := bucketTags.validate(); err != nil {
				api.writeErrorResponse(w, r, err)
				return
			}
		}
	}

	opts := objectlayer.BucketOptions{
		LockEnabled: objectLockEnabled,
	}

	if err := api.objectAPI.MakeBucketWithLocation(ctx, bucket, opts); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	// Return the path-style location of the newly-created bucket, also for
	// virtual-hosted-style requests.
	w.Header().Set(xhttp.Location, "/"+bucket)

	api.writeSuccessResponseHeadersOnly(w, r)
}

// PutBucketAclHandler is the HTTP handler for the PutBucketAcl operation,
// which sets a bucket's access control list.
func (api *API) PutBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "PutBucketAcl")

	bucketName := mux.Vars(r)["bucket"]

	// Amazon S3's documentation states that the "Content-Md5" header is required for this operation,
	// but testing has shown that this isn't true. Therefore, we don't require it, either.
	body, err := api.verifyWithBody(r, false)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	data, err := readVerifiedBody(body, maxConfigBodySize)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if _, err = api.objectAPI.GetBucketInfo(ctx, bucketName); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if err := checkACL(r.Header, data); err != nil {
		api.writeErrorResponseWithFallback(w, r, err, apierr.CodeMalformedXML)
		return
	}

	api.writeSuccessResponseHeadersOnly(w, r)
}

// PutBucketNotificationConfigurationHandler is the HTTP handler for the PutBucketNotificationConfiguration
// operation, which sets a bucket's notification configuration.
func (api *API) PutBucketNotificationConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "PutBucketNotificationConfiguration")

	bucketName := mux.Vars(r)["bucket"]

	body, err := api.verifyWithBody(r, false)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	data, err := readVerifiedBody(body, maxPutBucketNotificationConfigBodySize)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}
	if len(data) == 0 {
		api.writeErrorResponse(w, r, apierr.CodeMissingRequestBody)
		return
	}

	config, err := event.ParseConfig(bytes.NewReader(data))
	if err != nil {
		// The parsers also return plain errors, such as for an unsupported XML encoding.
		api.writeErrorResponseWithFallback(w, r, err, apierr.CodeMalformedXML)
		return
	}

	if err = api.objectAPI.SetBucketNotificationConfig(ctx, bucketName, config); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeSuccessResponseHeadersOnly(w, r)
}

// PutObjectLockConfigurationHandler is the HTTP handler for the PutObjectLockConfiguration
// operation, which places an Object Lock configuration on a bucket.
func (api *API) PutObjectLockConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "PutObjectLockConfiguration")

	bucketName := mux.Vars(r)["bucket"]

	// PutObjectLockConfiguration requires either Content-MD5 or an X-Amz-Checksum-<algorithm> header.
	body, err := api.verifyWithBody(r, true)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	data, err := readVerifiedBody(body, maxConfigBodySize)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}
	if len(data) == 0 {
		api.writeErrorResponse(w, r, apierr.CodeMissingRequestBody)
		return
	}

	config, err := parseObjectLockConfigXML(data)
	if err != nil {
		// The parsers also return plain errors, such as for an unsupported XML encoding.
		api.writeErrorResponseWithFallback(w, r, err, apierr.CodeMalformedXML)
		return
	}

	if err = api.objectAPI.SetObjectLockConfig(ctx, bucketName, config); err != nil {
		// The object layers answer a bucket without versioning with a generic message.
		if resp, ok := errToResponse(err); ok && resp.Code == "InvalidBucketState" {
			err = apierr.CodeVersioningRequiredForObjectLock
		}
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeSuccessResponseHeadersOnly(w, r)
}

// PutBucketTaggingHandler is the HTTP handler for the PutBucketTagging operation,
// which places a set of tags on a bucket.
func (api *API) PutBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "PutBucketTagging")

	bucketName := mux.Vars(r)["bucket"]

	// PutBucketTagging requires either Content-MD5 or an X-Amz-Checksum-<algorithm> header.
	body, err := api.verifyWithBody(r, true)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	data, err := readVerifiedBody(body, maxPutBucketTaggingBodySize)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}
	if len(data) == 0 {
		api.writeErrorResponse(w, r, apierr.CodeMissingRequestBody)
		return
	}

	tags, err := parseTaggingXML(bytes.NewReader(data), false)
	if err != nil {
		api.writeErrorResponseWithFallback(w, r, err, apierr.CodeMalformedXML)
		return
	}

	if err = api.objectAPI.SetBucketTagging(ctx, bucketName, tags); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeSuccessNoContent(w, r)
}

// PutBucketVersioningHandler is the HTTP handler for the PutBucketVersioning operation,
// which sets a bucket's object versioning configuration.
func (api *API) PutBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "PutBucketVersioning")

	bucketName := mux.Vars(r)["bucket"]

	body, err := api.verifyWithBody(r, false)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	data, err := readVerifiedBody(body, maxPutBucketVersioningBodySize)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}
	if len(data) == 0 {
		api.writeErrorResponse(w, r, apierr.CodeMissingRequestBody)
		return
	}

	// Like S3, any root element is accepted, but unknown or repeated children and values outside
	// the schema are MalformedXML.
	var config struct {
		Text      string `xml:",chardata"`
		Status    []string
		MfaDelete []string
		Unknown   unknownElements `xml:",any"`
	}
	if err := xmlDecoder(bytes.NewReader(data), &config); err != nil {
		// The decoder also returns plain errors, such as for an unsupported XML encoding.
		api.writeErrorResponseWithFallback(w, r, err, apierr.CodeMalformedXML)
		return
	}
	if len(config.Status) > 1 || len(config.MfaDelete) > 1 || len(config.Unknown) > 0 || hasText(config.Text) {
		api.writeErrorResponse(w, r, apierr.CodeMalformedXML)
		return
	}
	status, mfaDelete := strings.Join(config.Status, ""), strings.Join(config.MfaDelete, "")
	if !slices.Contains([]string{"", "Enabled", "Suspended"}, status) || !slices.Contains([]string{"", "Enabled", "Disabled"}, mfaDelete) {
		api.writeErrorResponse(w, r, apierr.CodeMalformedXML)
		return
	}
	if status == "" {
		api.writeErrorResponse(w, r, apierr.CodeIllegalVersioningConfiguration)
		return
	}
	// MFA delete can't be enabled, because MFA isn't supported.
	if mfaDelete == "Enabled" {
		api.writeErrorResponse(w, r, apierr.CodeMFAAuthenticationRequired)
		return
	}

	if err = api.objectAPI.SetBucketVersioning(ctx, bucketName, &versioning.Versioning{Status: versioning.State(status)}); err != nil {
		// The object layers answer suspending versioning of a bucket with object lock with a
		// generic message.
		if resp, ok := errToResponse(err); ok && resp.Code == "InvalidBucketState" {
			err = apierr.CodeObjectLockConfigurationPresent
		}
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeSuccessResponseHeadersOnly(w, r)
}

// HeadBucketHandler is the HTTP handler for the HeadBucket operation, which checks if a bucket can be accessed.
func (api *API) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "HeadBucket")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	// The location also tells whether the bucket exists, so it's looked up instead of the bucket.
	location, err := api.objectAPI.GetBucketLocation(ctx, bucketName)
	if err != nil {
		// Like S3, a region is sent for a bucket that the caller can't access too, so that clients
		// can find the endpoint to send their requests to. Its location can't be looked up.
		if resp, ok := errToResponse(err); ok && resp.HTTPStatusCode == http.StatusForbidden {
			w.Header().Set(xhttp.AmzBucketRegion, bucketRegion)
		}
		api.writeErrorResponseHeadersOnly(r, w, err)
		return
	}

	if location == "" {
		location = bucketRegion
	}
	w.Header().Set(xhttp.AmzBucketRegion, location)
	api.writeSuccessResponseHeadersOnly(w, r)
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
	api.writeSuccessResponseXML(w, r, []byte(accelerateDefaultConfig))
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

	acl := &accessControlPolicy{
		XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: cmd.Owner{
			ID:          defaultOwnerID,
			DisplayName: defaultOwnerDisplayName,
		},
	}
	acl.AccessControlList.Grants = append(acl.AccessControlList.Grants, grant{
		Grantee: grantee{
			XMLNS:       "http://www.w3.org/2001/XMLSchema-instance",
			XMLXSI:      "CanonicalUser",
			Type:        "CanonicalUser",
			ID:          defaultOwnerID,
			DisplayName: defaultOwnerDisplayName,
		},
		Permission: "FULL_CONTROL",
	})

	resp, err := encodeResponse(acl)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeSuccessResponseXML(w, r, resp)
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

// GetBucketWebsiteHandler is the HTTP handler for the GetBucketWebsite operation, which returns a
// bucket's website configuration. Buckets don't have one.
func (api *API) GetBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "GetBucketWebsite")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if _, err := api.objectAPI.GetBucketInfo(ctx, bucketName); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeErrorResponse(w, r, apierr.CodeNoSuchWebsiteConfiguration)
}

// DeleteBucketWebsiteHandler is the HTTP handler for the DeleteBucketWebsite operation, which
// deletes a bucket's website configuration. Buckets don't have one, so it only checks that the
// bucket exists, and answers 204 like minio's handlers and S3's documentation. S3's answer wasn't
// compared, because it needs the s3:DeleteBucketWebsite permission.
func (api *API) DeleteBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "DeleteBucketWebsite")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if _, err := api.objectAPI.GetBucketInfo(ctx, bucketName); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeSuccessNoContent(w, r)
}

// GetBucketLocationHandler is the HTTP handler for the GetBucketLocation operation,
// which returns the location that a bucket resides in.
func (api *API) GetBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "GetBucketLocation")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	location, err := api.objectAPI.GetBucketLocation(ctx, bucketName)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	resp, err := encodeResponse(cmd.LocationResponse{Location: location})
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeSuccessResponseXML(w, r, resp)
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
	api.writeSuccessResponseXML(w, r, []byte(loggingDefaultConfig))
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

	config.XMLNS = s3XMLNS
	configData, err := encodeResponse(config)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeSuccessResponseXML(w, r, configData)
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

	config.XMLNS = s3XMLNS
	configData, err := encodeResponse(config)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeSuccessResponseXML(w, r, configData)
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
		IsPublic: "false",
	})
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeSuccessResponseXML(w, r, encodedSuccessResponse)
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
	api.writeSuccessResponseXML(w, r, []byte(requestPaymentDefaultConfig))
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

	responseBytes, err := encodeResponse(newTaggingResponse(tags))
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeSuccessResponseXML(w, r, responseBytes)
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

	config.XMLNS = s3XMLNS
	configData, err := encodeResponse(config)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeSuccessResponseXML(w, r, configData)
}

// PostObjectHandler uploads an object to a bucket using an HTML form with a policy document.
func (api *API) PostObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "PostObject")

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

	// The subresources are routed elsewhere, and S3 refuses any other query parameter.
	if r.URL.RawQuery != "" {
		api.writeErrorResponse(w, r, apierr.CodePostObjectQuery)
		return
	}

	vr, err := api.verifier.V2V4.Verify(r, getVirtualHostedBucket(r))
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	postForm := vr.PostForm()

	// Encryption may be requested by form fields as well as headers.
	formHeader := http.Header{}
	for formKey, elems := range postForm {
		for _, elem := range elems {
			formHeader.Add(formKey, elem.Value)
		}
	}
	_, headerRequested := crypto.IsRequested(r.Header)
	_, formRequested := crypto.IsRequested(formHeader)
	if headerRequested || formRequested {
		api.writeErrorResponse(w, r, apierr.CodeNotImplemented)
		return
	}

	// The file is verified against the Content-MD5 header and the x-amz-checksum-* form fields.
	checksumHeader := http.Header{}
	if md5, ok := r.Header[xhttp.ContentMD5]; ok {
		checksumHeader[xhttp.ContentMD5] = md5
	}
	for formKey, elems := range postForm {
		if strings.HasPrefix(formKey, xAmzChecksumPrefix) {
			for _, elem := range elems {
				checksumHeader.Add(formKey, elem.Value)
			}
		}
	}
	checksumReqs, err := getChecksumRequests(checksumHeader)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	awsigReader, err := requestBody(vr.Reader(checksumReqs...))
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

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

	// Substitute ${filename} before checking the policy so that $key conditions apply to the final key.
	keys := postForm.Values("key")
	for i := range keys {
		keys[i].Value = strings.ReplaceAll(keys[i].Value, "${filename}", postForm.FileName())
	}

	if err = CheckPostForm(postPolicy, postForm, bucketName); err != nil {
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

	metadata := ExtractMetadataFromPostForm(postForm)
	if err := checkUserMetadataSize(metadata); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}
	if err := validateStorageClass(metadata[amzStorageClass]); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}
	if metadata[amzStorageClass] == storageclass.ONEZONE {
		delete(metadata, amzStorageClass)
	}

	if postForm.Has("tagging") {
		tagsReader := strings.NewReader(getPostFormValue(postForm, "tagging"))
		objectTags, err := parseTaggingXML(tagsReader, true)
		if err != nil {
			api.writeErrorResponseWithFallback(w, r, err, apierr.CodeMalformedXML)
			return
		}
		metadata[xhttp.AmzObjectTagging] = objectTags.String()
	}

	opts := objectlayer.ObjectOptions{
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

	// Stop reading as soon as the policy's maximum size is exceeded. The minimum is checked by the
	// object layer after the upload.
	sizeLimit := maxPostObjectSize
	if lengthRange := postPolicy.Conditions.ContentLengthRange; lengthRange.Valid {
		sizeLimit = max(min(lengthRange.Max, sizeLimit), 0)
	}
	hashReader := hash.NewAwsigReader(NewLimitedAwsigReader(awsigReader, sizeLimit), -1, -1)
	pReader := objectlayer.NewPutObjReader(hashReader)

	// Like keys in the URL, the key must be valid UTF-8.
	objectKey := postForm.Get("key").Value
	if objectKey == "" {
		api.writeErrorResponse(w, r, apierr.CodeKeyMustBeSpecified)
		return
	}
	if !utf8.ValidString(objectKey) {
		api.writeErrorResponse(w, r, apierr.CodeInvalidURI)
		return
	}
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

	// Like S3, ignore a redirect URL that can't be interpreted and fall back to success_action_status.
	if redirectURL != nil {
		queryValues := redirectURL.Query()
		queryValues.Set("bucket", objInfo.Bucket)
		queryValues.Set("key", objInfo.Name)
		queryValues.Set("etag", "\""+objInfo.ETag+"\"")
		redirectURL.RawQuery = queryValues.Encode()
		api.writeRedirectSeeOther(w, r, redirectURL.String())
		return
	}

	switch successStatus {
	case "201":
		resp, err := encodeResponse(postResponse{PostResponse: cmd.PostResponse{
			Bucket:   objInfo.Bucket,
			Key:      objInfo.Name,
			ETag:     `"` + objInfo.ETag + `"`,
			Location: w.Header().Get(xhttp.Location),
		}})
		if err != nil {
			api.writeErrorResponse(w, r, err)
			return
		}
		api.writeResponse(w, r, http.StatusCreated, resp, mimeXML)
	case "200":
		api.writeSuccessResponseHeadersOnly(w, r)
	default:
		api.writeSuccessNoContent(w, r)
	}
}

// DeleteObjectsHandler is the HTTP handler for the DeleteObject operation, which deletes multiple objects
// from a bucket.
func (api *API) DeleteObjectsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "DeleteObjects")

	bucketName := mux.Vars(r)["bucket"]

	// DeleteObjects requires either Content-MD5 or an X-Amz-Checksum-<algorithm> header.
	body, err := api.verifyWithBody(r, true)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if r.ContentLength < 0 {
		api.writeErrorResponse(w, r, apierr.CodeMissingContentLength)
		return
	}
	if r.ContentLength == 0 {
		api.writeErrorResponse(w, r, apierr.CodeMissingRequestBody)
		return
	}

	// The elements that S3 allows once are slices, and unknown ones are collected, so that the
	// request can be refused as MalformedXML like S3 does instead of decoded leniently. An empty
	// VersionId or ETag element is then also told apart from a missing one. Like S3, Size and
	// LastModifiedTime are unknown elements, because they are only allowed in directory buckets.
	var req struct {
		Text    string `xml:",chardata"`
		Quiet   []string
		Objects []struct {
			Text      string `xml:",chardata"`
			Key       []string
			VersionID []string        `xml:"VersionId"`
			ETag      []string        `xml:"ETag"`
			Unknown   unknownElements `xml:",any"`
		} `xml:"Object"`
		Unknown unknownElements `xml:",any"`
	}
	if err := decodeVerifiedXML(body, &req, maxDeleteObjectsBodySize); err != nil {
		api.writeErrorResponseWithFallback(w, r, err, apierr.CodeMalformedXML)
		return
	}

	if len(req.Objects) == 0 || len(req.Objects) > maxDeleteList || len(req.Quiet) > 1 || len(req.Unknown) > 0 || hasText(req.Text) {
		api.writeErrorResponse(w, r, apierr.CodeMalformedXML)
		return
	}
	for _, object := range req.Objects {
		if len(object.Key) != 1 || len(object.VersionID) > 1 || len(object.ETag) > 1 || len(object.Unknown) > 0 || hasText(object.Text) {
			api.writeErrorResponse(w, r, apierr.CodeMalformedXML)
			return
		}
	}

	// S3 only treats a case-insensitive "true" as quiet and ignores any other value.
	quiet := len(req.Quiet) == 1 && strings.EqualFold(req.Quiet[0], "true")

	type bucketObjectLocation struct {
		key          string
		versionID    string
		hasVersionID bool
	}

	seen := make(map[bucketObjectLocation]struct{}, len(req.Objects))

	objects := make([]objectlayer.ObjectToDelete, 0, len(req.Objects))
	deleteErrs := make([]deleteError, 0, len(req.Objects))
	// S3 refuses the whole request for an empty key, before any entry is deleted. Its 1024-byte
	// limit isn't applied, because the satellite limits the encrypted key and allows longer keys
	// written through uplink.
	for _, object := range req.Objects {
		if object.Key[0] == "" {
			api.writeErrorResponse(w, r, apierr.CodeUserKeyMustBeSpecified)
			return
		}
	}

	for _, object := range req.Objects {
		loc := bucketObjectLocation{
			key:          object.Key[0],
			hasVersionID: len(object.VersionID) == 1,
		}
		if loc.hasVersionID {
			loc.versionID = object.VersionID[0]
		}

		if _, ok := seen[loc]; ok {
			continue
		}
		seen[loc] = struct{}{}

		if len(object.ETag) == 1 {
			// S3 reports an empty ETag, even a blank one, as an error of the entry, before its
			// version ID is checked.
			if strings.TrimSpace(object.ETag[0]) == "" {
				resp, _ := apierr.CodeEmptyDeleteETag.ToResponse()
				deleteErrs = append(deleteErrs, deleteError{
					Key:       loc.key,
					VersionID: requestedVersionID(loc.versionID, loc.hasVersionID),
					Code:      resp.Code,
					Message:   resp.Description,
				})
				continue
			}
			// Conditional deletes are refused like If-Match on DeleteObject, because the object
			// layer can't check their conditions.
			api.writeErrorResponse(w, r, apierr.CodeNotImplemented)
			return
		}

		if loc.hasVersionID {
			err := validateVersionID(loc.versionID)
			if errors.Is(err, apierr.CodeInvalidVersionID) {
				// Unlike the single-object operations, S3 reports an empty or malformed version ID
				// as a version that doesn't exist.
				err = apierr.CodeNoSuchVersion
			}
			if err != nil {
				resp, _ := errToResponse(err)
				deleteErrs = append(deleteErrs, deleteError{
					Key:       loc.key,
					VersionID: requestedVersionID(loc.versionID, loc.hasVersionID),
					Code:      resp.Code,
					Message:   resp.Description,
				})
				continue
			}
		}

		objects = append(objects, objectlayer.ObjectToDelete{ObjectName: loc.key, VersionID: loc.versionID})
	}

	var deletedObjects []objectlayer.DeletedObject
	var apiDeleteErrs []objectlayer.DeleteObjectsError
	if len(objects) > 0 {
		var err error
		deletedObjects, apiDeleteErrs, err = api.objectAPI.DeleteObjects(ctx, bucketName, objects, objectlayer.ObjectOptions{
			BypassGovernanceRetention: objectlock.IsObjectLockGovernanceBypassSet(r.Header),
			Quiet:                     quiet,
		})
		if err != nil {
			api.writeErrorResponse(w, r, err)
			return
		}
	}

	var internalErrs []error
	for _, deleteErr := range apiDeleteErrs {
		resp, ok := errToResponse(deleteErr.Error)
		if !ok {
			internalErrs = append(internalErrs, deleteErr.Error)
			resp, _ = apierr.CodeInternal.ToResponse()
		}
		deleteErrs = append(deleteErrs, deleteError{
			Key:       deleteErr.ObjectName,
			VersionID: requestedVersionID(deleteErr.VersionID, deleteErr.VersionID != ""),
			Code:      resp.Code,
			Message:   resp.Description,
		})
	}

	if len(internalErrs) > 0 {
		api.log.Error(r, "unexpected errors", internalErrs...)
	}

	// The object layer may ignore Quiet, so enforce it here.
	var deleted []deletedObject
	if !quiet {
		deleted = make([]deletedObject, 0, len(deletedObjects))
		for _, object := range deletedObjects {
			// The object layers report the version they removed, but S3 only reports a VersionId
			// that was requested.
			versionID := object.VersionID
			if _, ok := seen[bucketObjectLocation{key: object.ObjectName, versionID: versionID, hasVersionID: true}]; !ok {
				versionID = ""
			}
			deleted = append(deleted, deletedObject{
				Key:                   object.ObjectName,
				VersionID:             versionID,
				DeleteMarker:          object.DeleteMarker,
				DeleteMarkerVersionID: object.DeleteMarkerVersionID,
			})
		}
	}

	encodedSuccessResponse, err := encodeResponse(deleteObjectsResponse{
		DeletedObjects: deleted,
		Errors:         deleteErrs,
	})
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeSuccessResponseXML(w, r, encodedSuccessResponse)
}

// requestedVersionID returns a pointer to versionID if the request had a version ID, so that an empty
// one is still reported.
func requestedVersionID(versionID string, requested bool) *string {
	if !requested {
		return nil
	}
	return &versionID
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

	api.writeSuccessNoContent(w, r)
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

	api.writeSuccessNoContent(w, r)
}
