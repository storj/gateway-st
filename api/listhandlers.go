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
	"encoding/base64"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/gorilla/mux"

	"storj.io/gateway/api/apierr"
	"storj.io/minio/cmd"
)

const maxObjectList = 1000

// ListObjectsHandler is the HTTP handler for the ListObjects operation, which lists the objects in a bucket.
func (api *API) ListObjectsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "ListObjects")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	params, err := getListObjectsParams(r.URL.Query())
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if err := validateListObjectsParams(params.maxKeys, params.encodingType); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	listObjectsInfo, err := api.objectAPI.ListObjects(ctx, bucketName, params.prefix, params.marker, params.delimiter, params.maxKeys)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	response, err := encodeResponse(generateListObjectsResponse(bucketName, params, listObjectsInfo))
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	writeSuccessResponseXML(w, response)
}

type listObjectsParams struct {
	prefix       string
	marker       string
	delimiter    string
	maxKeys      int
	encodingType string
}

func getListObjectsParams(values url.Values) (params listObjectsParams, err error) {
	var maxKeys int
	if maxKeysStr := values.Get("max-keys"); maxKeysStr != "" {
		if maxKeys, err = strconv.Atoi(maxKeysStr); err != nil {
			return listObjectsParams{}, apierr.CodeInvalidMaxKeys
		}
	} else {
		maxKeys = maxObjectList
	}
	return listObjectsParams{
		prefix:       trimLeadingSlash(values.Get("prefix")),
		marker:       trimLeadingSlash(values.Get("marker")),
		delimiter:    values.Get("delimiter"),
		maxKeys:      maxKeys,
		encodingType: values.Get("encoding-type"),
	}, nil
}

func validateListObjectsParams(maxKeys int, encodingType string) error {
	if maxKeys < 0 {
		return apierr.CodeInvalidMaxKeys
	}
	if encodingType != "" && strings.ToLower(encodingType) != urlEncodingType {
		return apierr.CodeInvalidEncodingMethod
	}
	return nil
}

// ListObjectsV2Handler is the HTTP handler for the ListObjectsV2 operation, which lists the objects in a bucket.
func (api *API) ListObjectsV2Handler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "ListObjectsV2")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	params, err := getListObjectsV2Params(r.URL.Query())
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if err := validateListObjectsParams(params.maxKeys, params.encodingType); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	listObjectsV2Info, err := api.objectAPI.ListObjectsV2(ctx, bucketName, params.prefix, params.token, params.delimiter,
		params.maxKeys, params.fetchOwner, params.startAfter)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	response := generateListObjectsV2Response(bucketName, params, listObjectsV2Info)

	resp, err := encodeResponse(response)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	writeSuccessResponseXML(w, resp)
}

type listObjectsV2Params struct {
	prefix       string
	token        string
	delimiter    string
	maxKeys      int
	fetchOwner   bool
	startAfter   string
	encodingType string
}

func getListObjectsV2Params(values url.Values) (params listObjectsV2Params, err error) {
	tokenStr := values.Get("continuation-token")
	if tokenStr == "" {
		return listObjectsV2Params{}, apierr.CodeIncorrectContinuationToken
	}

	decodedToken, err := base64.StdEncoding.DecodeString(tokenStr)
	if err != nil {
		return listObjectsV2Params{}, apierr.CodeIncorrectContinuationToken
	}

	var maxKeys int
	if maxKeysStr := values.Get("max-keys"); maxKeysStr != "" {
		if maxKeys, err = strconv.Atoi(maxKeysStr); err != nil {
			return listObjectsV2Params{}, apierr.CodeInvalidMaxKeys
		}
	} else {
		maxKeys = maxObjectList
	}

	return listObjectsV2Params{
		prefix:       trimLeadingSlash(values.Get("prefix")),
		token:        string(decodedToken),
		delimiter:    values.Get("delimiter"),
		maxKeys:      maxKeys,
		fetchOwner:   values.Get("fetch-owner") == "true",
		startAfter:   trimLeadingSlash(values.Get("start-after")),
		encodingType: values.Get("encoding-type"),
	}, nil
}
