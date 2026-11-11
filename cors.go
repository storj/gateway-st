// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.
// This file incorporates code from MinIO Cloud Storage and includes changes made by Storj Labs, Inc.

/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
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

package main

import (
	"net/http"
	"os"
	"strings"

	"github.com/rs/cors"

	xhttp "storj.io/minio/cmd/http"
	"storj.io/minio/pkg/wildcard"
)

// corsAllowedMethods are the methods that corsHandler allows, like minio's.
var corsAllowedMethods = []string{
	http.MethodGet,
	http.MethodPut,
	http.MethodHead,
	http.MethodPost,
	http.MethodDelete,
	http.MethodOptions,
	http.MethodPatch,
}

// corsAllowedOrigins returns the origins in MINIO_API_CORS_ALLOW_ORIGIN, which minio's handlers also
// read, or every origin if it isn't set.
func corsAllowedOrigins() []string {
	value, ok := os.LookupEnv("MINIO_API_CORS_ALLOW_ORIGIN")
	if !ok {
		return []string{"*"}
	}
	return strings.Split(value, ",")
}

// corsHandler answers CORS (Cross Origin Resource Sharing) requests for allowedOrigins like minio's
// handlers, which the api package leaves to the server in front of it.
func corsHandler(allowedOrigins []string, handler http.Handler) http.Handler {
	commonS3Headers := []string{
		xhttp.Date,
		xhttp.ETag,
		xhttp.ServerInfo,
		xhttp.Connection,
		xhttp.AcceptRanges,
		xhttp.ContentRange,
		xhttp.ContentEncoding,
		xhttp.ContentLength,
		xhttp.ContentType,
		xhttp.ContentDisposition,
		xhttp.LastModified,
		xhttp.ContentLanguage,
		xhttp.CacheControl,
		xhttp.RetryAfter,
		xhttp.AmzBucketRegion,
		xhttp.Expires,
		"X-Amz*",
		"x-amz*",
		"*",
	}

	return cors.New(cors.Options{
		AllowOriginFunc: func(origin string) bool {
			for _, allowedOrigin := range allowedOrigins {
				if wildcard.MatchSimple(allowedOrigin, origin) {
					return true
				}
			}
			return false
		},
		AllowedMethods:   corsAllowedMethods,
		AllowedHeaders:   commonS3Headers,
		ExposedHeaders:   commonS3Headers,
		AllowCredentials: true,
	}).Handler(handler)
}
