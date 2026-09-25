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
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"unicode"

	"github.com/amwolff/awsig"
	"github.com/gorilla/mux"

	"storj.io/gateway/api/apierr"
	"storj.io/minio/cmd"
	xhttp "storj.io/minio/cmd/http"
)

// Config contains configuration parameters for an API.
type Config struct {
	// Domains are the domains under which buckets are addressed in virtual-hosted-style requests.
	// When one domain is a subdomain of another, such as example.com and s3.example.com, the
	// longer one wins: b.s3.example.com addresses bucket b, so a bucket named b.s3 can't be
	// addressed under example.com.
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
	// Cleaning happens in the ServeHTTP of the outermost router, so it must be disabled there too.
	router.SkipClean(true)
	apiRouter.SkipClean(true)

	apiRouter.Use(requestIDMiddleware)

	// Middleware only runs for matched routes, so these handlers set the request ID themselves.
	router.NotFoundHandler = requestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.writeErrorResponse(w, r, apierr.CodeOperationNotSupported)
	}))
	router.MethodNotAllowedHandler = requestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.writeErrorResponse(w, r, apierr.CodeMethodNotAllowed)
	}))

	// Match the most specific domain first so that, with both example.com and s3.example.com
	// configured, b.s3.example.com addresses bucket b rather than b.s3. A host that is itself a
	// configured domain (s3.example.com) addresses no bucket. Host names are case-insensitive.
	domains := make([]string, len(api.config.Domains))
	for i, domain := range api.config.Domains {
		domains[i] = strings.ToLower(domain)
	}
	slices.SortFunc(domains, func(a, b string) int { return len(b) - len(a) })
	notDomain := func(r *http.Request, _ *mux.RouteMatch) bool {
		host, _, _ := strings.Cut(r.Host, ":")
		return !slices.Contains(domains, strings.ToLower(host))
	}

	type bucketSubrouter struct {
		*mux.Router
		vHost bool
	}
	var subrouters []bucketSubrouter
	for _, domain := range domains {
		// The domain is a case-insensitive pattern, because mux matches hosts case-sensitively.
		subrouter := apiRouter.Host("{bucket:.+}.{domain:" + caseInsensitivePattern(domain) + "}").MatcherFunc(notDomain).Subrouter()
		subrouter.Use(withVirtualHostedStyleMiddleware)
		subrouters = append(subrouters, bucketSubrouter{Router: subrouter, vHost: true})
	}
	// A virtual-hosted-style request that no route of its domain matches must not fall through to
	// path-style routing, where its path would address another bucket.
	notVirtualHosted := func(r *http.Request, _ *mux.RouteMatch) bool {
		host, _, _ := strings.Cut(r.Host, ":")
		return !isVirtualHostedHost(host, domains)
	}
	subrouters = append(subrouters, bucketSubrouter{Router: apiRouter.PathPrefix("/{bucket}").MatcherFunc(notVirtualHosted).Subrouter()})

	for _, subrouter := range subrouters {
		// Object-level operations
		objRouter := subrouter.Path("/{object:.+}").Subrouter()

		objRouter.Methods(http.MethodPut).Queries("acl", "").HandlerFunc(api.PutObjectAclHandler)
		objRouter.Methods(http.MethodPut).Queries("legal-hold", "").HandlerFunc(api.PutObjectLegalHoldHandler)
		objRouter.Methods(http.MethodPut).Queries("retention", "").HandlerFunc(api.PutObjectRetentionHandler)
		objRouter.Methods(http.MethodPut).Queries("tagging", "").HandlerFunc(api.PutObjectTaggingHandler)
		objRouter.Methods(http.MethodPut).Queries("partNumber", "", "uploadId", "").Headers(xhttp.AmzCopySource, "").HandlerFunc(api.UploadPartCopyHandler)
		objRouter.Methods(http.MethodPut).Queries("partNumber", "", "uploadId", "").HandlerFunc(api.UploadPartHandler)
		objRouter.Methods(http.MethodPut).Headers(xhttp.AmzCopySource, "").HandlerFunc(api.CopyObjectHandler)
		objRouter.Methods(http.MethodPut).HandlerFunc(api.PutObjectHandler)

		objRouter.Methods(http.MethodHead).HandlerFunc(api.HeadObjectHandler)

		objRouter.Methods(http.MethodGet).Queries("uploadId", "").HandlerFunc(api.ListPartsHandler)
		objRouter.Methods(http.MethodGet).Queries("acl", "").HandlerFunc(api.GetObjectAclHandler)
		objRouter.Methods(http.MethodGet).Queries("attributes", "").HandlerFunc(api.GetObjectAttributesHandler)
		objRouter.Methods(http.MethodGet).Queries("legal-hold", "").HandlerFunc(api.GetObjectLegalHoldHandler)
		objRouter.Methods(http.MethodGet).Queries("tagging", "").HandlerFunc(api.GetObjectTaggingHandler)
		objRouter.Methods(http.MethodGet).Queries("retention", "").HandlerFunc(api.GetObjectRetentionHandler)
		objRouter.Methods(http.MethodGet).HandlerFunc(api.GetObjectHandler)

		objRouter.Methods(http.MethodDelete).Queries("uploadId", "").HandlerFunc(api.AbortMultipartUploadHandler)
		objRouter.Methods(http.MethodDelete).Queries("tagging", "").HandlerFunc(api.DeleteObjectTaggingHandler)
		objRouter.Methods(http.MethodDelete).HandlerFunc(api.DeleteObjectHandler)

		objRouter.Methods(http.MethodPost).Queries("uploads", "").HandlerFunc(api.CreateMultipartUploadHandler)
		objRouter.Methods(http.MethodPost).Queries("uploadId", "").HandlerFunc(api.CompleteMultipartUploadHandler)

		// Registered after the object-level operations so that they only match bucket-level requests.
		api.registerUnsupportedHandlers(subrouter.Router)

		// Bucket-level operations
		bucketRouter := subrouter.MatcherFunc(bucketPathMatcher(subrouter.vHost)).Subrouter()

		bucketRouter.Methods(http.MethodPut).Queries("acl", "").HandlerFunc(api.PutBucketAclHandler)
		bucketRouter.Methods(http.MethodPut).Queries("notification", "").HandlerFunc(api.PutBucketNotificationConfigurationHandler)
		bucketRouter.Methods(http.MethodPut).Queries("object-lock", "").HandlerFunc(api.PutObjectLockConfigurationHandler)
		bucketRouter.Methods(http.MethodPut).Queries("tagging", "").HandlerFunc(api.PutBucketTaggingHandler)
		bucketRouter.Methods(http.MethodPut).Queries("versioning", "").HandlerFunc(api.PutBucketVersioningHandler)
		bucketRouter.Methods(http.MethodPut).HandlerFunc(api.CreateBucketHandler)

		bucketRouter.Methods(http.MethodHead).HandlerFunc(api.HeadBucketHandler)

		bucketRouter.Methods(http.MethodGet).Queries("accelerate", "").HandlerFunc(api.GetBucketAccelerateHandler)
		bucketRouter.Methods(http.MethodGet).Queries("acl", "").HandlerFunc(api.GetBucketAclHandler)
		bucketRouter.Methods(http.MethodGet).Queries("cors", "").HandlerFunc(api.GetBucketCorsHandler)
		bucketRouter.Methods(http.MethodGet).Queries("location", "").HandlerFunc(api.GetBucketLocationHandler)
		bucketRouter.Methods(http.MethodGet).Queries("logging", "").HandlerFunc(api.GetBucketLoggingHandler)
		bucketRouter.Methods(http.MethodGet).Queries("notification", "").HandlerFunc(api.GetBucketNotificationConfigurationHandler)
		bucketRouter.Methods(http.MethodGet).Queries("object-lock", "").HandlerFunc(api.GetObjectLockConfigurationHandler)
		bucketRouter.Methods(http.MethodGet).Queries("policyStatus", "").HandlerFunc(api.GetBucketPolicyStatusHandler)
		bucketRouter.Methods(http.MethodGet).Queries("requestPayment", "").HandlerFunc(api.GetBucketRequestPaymentHandler)
		bucketRouter.Methods(http.MethodGet).Queries("tagging", "").HandlerFunc(api.GetBucketTaggingHandler)
		bucketRouter.Methods(http.MethodGet).Queries("versioning", "").HandlerFunc(api.GetBucketVersioningHandler)

		bucketRouter.Methods(http.MethodGet).HandlerFunc(api.ListMultipartUploadsHandler).Queries("uploads", "")
		bucketRouter.Methods(http.MethodGet).HandlerFunc(api.ListObjectVersionsHandler).Queries("versions", "")
		bucketRouter.Methods(http.MethodGet).HandlerFunc(api.ListObjectsV2Handler).Queries("list-type", "2")
		bucketRouter.Methods(http.MethodGet).HandlerFunc(api.ListObjectsHandler)

		bucketRouter.Methods(http.MethodPost).HeadersRegexp(xhttp.ContentType, "multipart/form-data").HandlerFunc(api.PostObjectHandler)
		bucketRouter.Methods(http.MethodPost).Queries("delete", "").HandlerFunc(api.DeleteObjectsHandler)

		bucketRouter.Methods(http.MethodDelete).Queries("tagging", "").HandlerFunc(api.DeleteBucketTaggingHandler)
		bucketRouter.Methods(http.MethodDelete).HandlerFunc(api.DeleteBucketHandler)
	}

	apiRouter.Methods(http.MethodGet).Path(cmd.SlashSeparator).HandlerFunc((api.ListBucketsHandler))
}

// amzID2 is the header carrying the extended request ID.
const amzID2 = "x-amz-id-2"

func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var id [8]byte
		var hostID [32]byte
		_, _ = rand.Read(id[:]) // never returns an error
		_, _ = rand.Read(hostID[:])
		w.Header().Set(xhttp.AmzRequestID, strings.ToUpper(hex.EncodeToString(id[:])))
		w.Header().Set(amzID2, base64.StdEncoding.EncodeToString(hostID[:]))
		next.ServeHTTP(w, r)
	})
}

// caseInsensitivePattern returns a regular expression that matches s case-insensitively. It spells
// out each letter as a character class instead of using (?i), because mux only ignores the port of
// a host when the host pattern has no ':'.
func caseInsensitivePattern(s string) string {
	var b strings.Builder
	for _, r := range s {
		if lower, upper := unicode.ToLower(r), unicode.ToUpper(r); lower != upper {
			b.WriteString("[" + string(lower) + string(upper) + "]")
		} else {
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	return b.String()
}

// isVirtualHostedHost returns whether host addresses a bucket as a subdomain of one of domains,
// which must be lowercase. Host names are case-insensitive.
func isVirtualHostedHost(host string, domains []string) bool {
	host = strings.ToLower(host)
	if slices.Contains(domains, host) {
		return false
	}
	for _, domain := range domains {
		if strings.HasSuffix(host, "."+domain) {
			return true
		}
	}
	return false
}

// bucketPathMatcher matches requests whose path addresses a bucket rather than an object:
// "/" for virtual-hosted-style requests and "/bucket" or "/bucket/" for path-style ones.
func bucketPathMatcher(vHost bool) mux.MatcherFunc {
	return func(r *http.Request, _ *mux.RouteMatch) bool {
		p := r.URL.EscapedPath()
		if vHost {
			return p == "/"
		}
		return !strings.Contains(strings.TrimSuffix(strings.TrimPrefix(p, "/"), "/"), "/")
	}
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
	{
		query:   "ownershipControls",
		methods: []string{http.MethodPut, http.MethodGet, http.MethodDelete},
	},
	{
		query:   "publicAccessBlock",
		methods: []string{http.MethodPut, http.MethodGet, http.MethodDelete},
	},
	{
		query:   "metrics",
		methods: []string{http.MethodPut, http.MethodGet, http.MethodDelete},
	},
	{
		query:   "analytics",
		methods: []string{http.MethodPut, http.MethodGet, http.MethodDelete},
	},
	{
		query:   "inventory",
		methods: []string{http.MethodPut, http.MethodGet, http.MethodDelete},
	},
	{
		query:   "intelligent-tiering",
		methods: []string{http.MethodPut, http.MethodGet, http.MethodDelete},
	},
	{
		query:   "restore",
		methods: []string{http.MethodPost},
	},
	{
		query:   "select",
		methods: []string{http.MethodPost},
	},
	// GET requests for the following are served by dedicated handlers.
	{
		query:   "cors",
		methods: []string{http.MethodPut, http.MethodDelete},
	},
	{
		query:   "accelerate",
		methods: []string{http.MethodPut, http.MethodDelete},
	},
	{
		query:   "logging",
		methods: []string{http.MethodPut, http.MethodDelete},
	},
	{
		query:   "requestPayment",
		methods: []string{http.MethodPut, http.MethodDelete},
	},
	{
		query:   "metadataTable",
		methods: []string{http.MethodPost, http.MethodDelete},
	},
	{
		query:   "metadataConfiguration",
		methods: []string{http.MethodPost, http.MethodDelete},
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
