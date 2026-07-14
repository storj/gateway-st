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
	"context"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/amwolff/awsig"
	"github.com/gorilla/mux"

	"storj.io/gateway/api/apierr"
	"storj.io/minio/cmd"
	xhttp "storj.io/minio/cmd/http"
)

// Config contains configuration parameters for an API.
type Config struct {
	Domains []string
}

// AuthData contains additional auth data provided by awsig.CredentialsProvider.
type AuthData struct {
	AccessKeyID string
}

// API is an S3-compatible HTTP API.
type API struct {
	log       Logger
	objectAPI cmd.ObjectLayer
	verifier  *awsig.V2V4[AuthData]

	config Config
}

// New constructs a new S3-compatible HTTP API.
func New(objectAPI cmd.ObjectLayer, credsProvider awsig.CredentialsProvider[AuthData], config Config, opts ...Option) *API {
	v2v4 := awsig.NewV2V4(credsProvider, awsig.V4Config{
		Service:                "s3",
		SkipRegionVerification: true,
	})

	api := &API{
		objectAPI: objectAPI,
		verifier:  v2v4,
		config:    config,
	}

	for _, opt := range opts {
		opt(api)
	}
	if api.log == nil {
		api.log = nopLogger{}
	}

	return api
}

// Option is an option for constructing an API.
type Option func(*API)

// RegisterHandlers registers S3-compatible HTTP handlers on the provided router.
func (api *API) RegisterHandlers(router *mux.Router) {
	apiRouter := router.PathPrefix(cmd.SlashSeparator).Subrouter()
	// UseEncodedPath configures the router to match routes using the raw (percent-encoded) form of
	// the request path instead of the unescaped one. This is required because object keys which
	// are present in the URL may contain characters that, when decoded, break request routing.
	// For example, an object key may contain a newline character, which isn't matched by the
	// "." in regular expressions used for route matching. This would cause the the request to be
	// misrouted. (The MinIO issue suggests that more cases exist, but only the newline case has
	// been verified by us.)
	// See: https://github.com/minio/minio/issues/8950
	apiRouter.UseEncodedPath()
	// SkipClean disables path cleaning. This is required because signature verification must use
	// the raw path.
	// See: https://github.com/minio/minio/issues/3256
	apiRouter.SkipClean(true)

	apiRouter.Use(requestIDMiddleware)

	var subrouters []*mux.Router
	for _, domain := range api.config.Domains {
		subrouter := apiRouter.Host("{bucket:.+}." + domain).Subrouter()
		subrouter.Use(withVirtualHostedStyleMiddleware)
		subrouters = append(subrouters, subrouter)
	}
	subrouters = append(subrouters, apiRouter.PathPrefix("/{bucket}").Subrouter())

	for _, subrouter := range subrouters {
		api.registerUnsupportedHandlers(subrouter)

		// Object-level operations
		objRouter := subrouter.Path("/{object:.+}").Subrouter()

		objRouter.Methods(http.MethodPut).Queries("acl", "").HandlerFunc(api.PutObjectAclHandler)
		objRouter.Methods(http.MethodPut).Queries("legal-hold", "").HandlerFunc(api.PutObjectLegalHoldHandler)
		objRouter.Methods(http.MethodPut).Queries("retention", "").HandlerFunc(api.PutObjectRetentionHandler)
		objRouter.Methods(http.MethodPut).Queries("tagging", "").HandlerFunc(api.PutObjectTaggingHandler)
		objRouter.Methods(http.MethodPut).Queries("partNumber", "", "uploadId", "").Headers(xhttp.AmzCopySource, "").HandlerFunc(api.UploadPartCopyHandler)
		objRouter.Methods(http.MethodPut).Queries("partNumber", "", "uploadId", "").HandlerFunc(api.UploadPartHandler)
		objRouter.Methods(http.MethodPut).HandlerFunc(api.PutObjectHandler)

		objRouter.Methods(http.MethodGet).Queries("acl", "").HandlerFunc(api.GetObjectAclHandler)
		objRouter.Methods(http.MethodGet).Queries("attributes", "").HandlerFunc(api.GetObjectAttributesHandler)
		objRouter.Methods(http.MethodGet).Queries("legal-hold", "").HandlerFunc(api.GetObjectLegalHoldHandler)

		// Bucket-level operations
		subrouter.Methods(http.MethodPut).Queries("acl", "").HandlerFunc(api.PutBucketAclHandler)
		subrouter.Methods(http.MethodPut).Queries("notification", "").HandlerFunc(api.PutBucketNotificationConfigurationHandler)
		subrouter.Methods(http.MethodPut).Queries("object-lock", "").HandlerFunc(api.PutObjectLockConfigurationHandler)
		subrouter.Methods(http.MethodPut).Queries("tagging", "").HandlerFunc(api.PutBucketTaggingHandler)
		subrouter.Methods(http.MethodPut).Queries("versioning", "").HandlerFunc(api.PutBucketVersioningHandler)
		subrouter.Methods(http.MethodPut).HandlerFunc(api.CreateBucketHandler)

		subrouter.Methods(http.MethodHead).HandlerFunc(api.HeadBucketHandler)

		subrouter.Methods(http.MethodGet).Queries("accelerate", "").HandlerFunc(api.GetBucketAccelerateHandler)
		subrouter.Methods(http.MethodGet).Queries("acl", "").HandlerFunc(api.GetBucketAclHandler)
		subrouter.Methods(http.MethodGet).Queries("cors", "").HandlerFunc(api.GetBucketCorsHandler)
		subrouter.Methods(http.MethodGet).Queries("location", "").HandlerFunc(api.GetBucketLocationHandler)
		subrouter.Methods(http.MethodGet).Queries("logging", "").HandlerFunc(api.GetBucketLoggingHandler)
		subrouter.Methods(http.MethodGet).Queries("notification", "").HandlerFunc(api.GetBucketNotificationConfigurationHandler)
		subrouter.Methods(http.MethodGet).Queries("object-lock", "").HandlerFunc(api.GetObjectLockConfigurationHandler)
		subrouter.Methods(http.MethodGet).Queries("policyStatus", "").HandlerFunc(api.GetBucketPolicyStatusHandler)
		subrouter.Methods(http.MethodGet).Queries("requestPayment", "").HandlerFunc(api.GetBucketRequestPaymentHandler)
		subrouter.Methods(http.MethodGet).Queries("tagging", "").HandlerFunc(api.GetBucketTaggingHandler)
		subrouter.Methods(http.MethodGet).Queries("versioning", "").HandlerFunc(api.GetBucketVersioningHandler)

		subrouter.Methods(http.MethodGet).HandlerFunc(api.ListObjectVersionsHandler).Queries("versions", "")
		subrouter.Methods(http.MethodGet).HandlerFunc(api.ListObjectsV2Handler).Queries("list-type", "2")
		subrouter.Methods(http.MethodGet).HandlerFunc(api.ListObjectsHandler)
		subrouter.Methods(http.MethodGet).HandlerFunc(api.ListMultipartUploadsHandler).Queries("uploads", "2")

		subrouter.Methods(http.MethodPost).HeadersRegexp(xhttp.ContentType, "multipart/form-data").HandlerFunc(api.PostObjectHandler)
		subrouter.Methods(http.MethodPost).Queries("delete", "").HandlerFunc(api.DeleteObjectsHandler)

		subrouter.Methods(http.MethodDelete).Queries("tagging", "").HandlerFunc(api.DeleteBucketTaggingHandler)
		subrouter.Methods(http.MethodDelete).HandlerFunc(api.DeleteBucketHandler)
	}

	apiRouter.Methods(http.MethodGet).Path(cmd.SlashSeparator).HandlerFunc((api.ListBucketsHandler))
}

func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set(xhttp.AmzRequestID, fmt.Sprintf("%X", time.Now().UnixNano()))
		next.ServeHTTP(w, r)
	})
}

type contextKey int

const isVHostKey contextKey = iota

func withVirtualHostedStyleMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), isVHostKey, struct{}{})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func getVirtualHostedBucket(r *http.Request) string {
	if r.Context().Value(isVHostKey) != nil {
		return mux.Vars(r)["bucket"]
	}
	return ""
}

// WithVirtualHostedStyle returns a new context that indicates that the request is
// using a virtual-hosted-style URL, where the bucket name is part of the subdomain.
// This is intended to be used in tests.
func WithVirtualHostedStyle(ctx context.Context) context.Context {
	return context.WithValue(ctx, isVHostKey, struct{}{})
}

var unsupportedEndpoints = []struct {
	query   string
	methods []string
}{
	{
		query:   "encryption",
		methods: []string{http.MethodPut, http.MethodGet, http.MethodDelete},
	},
	{
		query:   "lifecycle",
		methods: []string{http.MethodPut, http.MethodGet, http.MethodDelete},
	},
	{
		query:   "policy",
		methods: []string{http.MethodPut, http.MethodGet, http.MethodDelete},
	},
	{
		query:   "replication",
		methods: []string{http.MethodPut, http.MethodGet, http.MethodDelete},
	},
	{
		query:   "website",
		methods: []string{http.MethodPut, http.MethodGet, http.MethodDelete},
	},
}

func (api *API) registerUnsupportedHandlers(router *mux.Router) {
	for _, endpoint := range unsupportedEndpoints {
		// The slice of methods is required to be cloned because the mux library
		// modifies it (specifically, it replaces each method with its uppercase form).
		// Concurrent tests may each call registerUnsupportedHandlers, and if one or
		// more modifies the same slice that the others access, a data race occurs.
		methods := slices.Clone(endpoint.methods)
		router.Methods(methods...).Queries(endpoint.query, "").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			api.writeErrorResponse(w, r, apierr.CodeOperationNotSupported)
		})
	}
}
