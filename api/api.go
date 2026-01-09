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
	"net/http"

	"github.com/amwolff/awsig"
	"github.com/gorilla/mux"

	"storj.io/minio/cmd"
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
	objectAPI cmd.ObjectLayer
	verifier  *awsig.V2V4[AuthData]

	config Config
}

// New constructs a new S3-compatible HTTP API.
func New(objectAPI cmd.ObjectLayer, credsProvider awsig.CredentialsProvider[AuthData], config Config) *API {
	v2v4 := awsig.NewV2V4(credsProvider, awsig.V4Config{
		Service:                "s3",
		SkipRegionVerification: true,
	})
	return &API{
		objectAPI: objectAPI,
		verifier:  v2v4,
		config:    config,
	}
}

// RegisterHandlers registers S3-compatible HTTP handlers on the provided router.
func (api *API) RegisterHandlers(router *mux.Router) {
	apiRouter := router.PathPrefix(cmd.SlashSeparator).Subrouter()

	var subrouters []*mux.Router
	for _, domain := range api.config.Domains {
		subrouter := apiRouter.Host("{bucket:.+}." + domain).Subrouter()
		subrouter.Use(withVirtualHostedStyle())
		subrouters = append(subrouters, subrouter)
	}
	subrouters = append(subrouters, apiRouter.PathPrefix("/{bucket}").Subrouter())

	for _, subrouter := range subrouters {
		// Bucket-level operations
		subrouter.Methods(http.MethodPut).Queries("acl", "").HandlerFunc(api.PutBucketAclHandler)
		subrouter.Methods(http.MethodPut).Queries("object-lock", "").HandlerFunc(api.PutBucketObjectLockConfigHandler)
		subrouter.Methods(http.MethodPut).Queries("tagging", "").HandlerFunc(api.PutBucketTaggingHandler)
		subrouter.Methods(http.MethodPut).Queries("versioning", "").HandlerFunc(api.PutBucketVersioningHandler)
		subrouter.Methods(http.MethodPut).HandlerFunc(api.PutBucketHandler)

		subrouter.Methods(http.MethodHead).HandlerFunc(api.HeadBucketHandler)

		subrouter.Methods(http.MethodGet).Queries("accelerate", "").HandlerFunc(api.GetBucketAccelerateHandler)
		subrouter.Methods(http.MethodGet).Queries("acl", "").HandlerFunc(api.GetBucketAclHandler)
		subrouter.Methods(http.MethodGet).Queries("cors", "").HandlerFunc(api.GetBucketCorsHandler)
		subrouter.Methods(http.MethodGet).Queries("location", "").HandlerFunc(api.GetBucketLocationHandler)
		subrouter.Methods(http.MethodGet).Queries("logging", "").HandlerFunc(api.GetBucketLoggingHandler)
		subrouter.Methods(http.MethodGet).Queries("object-lock", "").HandlerFunc(api.GetBucketObjectLockConfigHandler)
		subrouter.Methods(http.MethodGet).Queries("policyStatus", "").HandlerFunc(api.GetBucketPolicyStatusHandler)
		subrouter.Methods(http.MethodGet).Queries("requestPayment", "").HandlerFunc(api.GetBucketRequestPaymentHandler)
		subrouter.Methods(http.MethodGet).Queries("tagging", "").HandlerFunc(api.GetBucketTaggingHandler)
		subrouter.Methods(http.MethodGet).Queries("versioning", "").HandlerFunc(api.GetBucketVersioningHandler)
		subrouter.Methods(http.MethodGet).Queries("website", "").HandlerFunc(api.GetBucketWebsiteHandler)
	}
}

type contextKey int

const isVHostKey contextKey = iota

func withVirtualHostedStyle() mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := context.WithValue(r.Context(), isVHostKey, struct{}{})
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func getVirtualHostedBucket(r *http.Request) string {
	if r.Context().Value(isVHostKey) != nil {
		return mux.Vars(r)["bucket"]
	}
	return ""
}
