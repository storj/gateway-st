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
	"slices"
	"strconv"
	"strings"

	"github.com/gorilla/mux"

	"storj.io/gateway/api/apierr"
	"storj.io/minio/cmd"
	xhttp "storj.io/minio/cmd/http"
)

const (
	maxObjectList = 1000
	maxPartsList  = 1000
	maxBucketList = 10000
)

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

	api.writeSuccessResponseXML(w, r, response)
}

type listObjectsParams struct {
	prefix       string
	marker       string
	delimiter    string
	maxKeys      int
	encodingType string
}

func getListObjectsParams(values url.Values) (params listObjectsParams, err error) {
	maxKeys, err := getMaxKeys(values)
	if err != nil {
		return listObjectsParams{}, err
	}
	return listObjectsParams{
		prefix:       values.Get("prefix"),
		marker:       values.Get("marker"),
		delimiter:    values.Get("delimiter"),
		maxKeys:      maxKeys,
		encodingType: values.Get("encoding-type"),
	}, nil
}

// getMaxKeys returns the max-keys parameter, defaulting to and capped at maxObjectList.
func getMaxKeys(values url.Values) (int, error) {
	maxKeysStr := values.Get("max-keys")
	if maxKeysStr == "" {
		return maxObjectList, nil
	}
	maxKeys, ok := parseListInt(maxKeysStr)
	if !ok {
		return 0, apierr.CodeInvalidMaxKeys
	}
	return min(maxKeys, maxObjectList), nil
}

// parseListInt parses a list limit or marker, accepting 0..math.MaxInt32 as S3 does.
func parseListInt(s string) (int, bool) {
	n, err := strconv.ParseInt(s, 10, 32)
	return int(n), err == nil && n >= 0
}

func validateListObjectsParams(maxKeys int, encodingType string) error {
	if maxKeys < 0 {
		return apierr.CodeInvalidMaxKeys
	}
	return validateEncodingType(encodingType)
}

func validateEncodingType(encodingType string) error {
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

	api.writeSuccessResponseXML(w, r, resp)
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
	var decodedToken []byte
	if tokenStr := values.Get("continuation-token"); tokenStr != "" {
		if decodedToken, err = base64.StdEncoding.DecodeString(tokenStr); err != nil {
			return listObjectsV2Params{}, apierr.CodeIncorrectContinuationToken
		}
	}

	maxKeys, err := getMaxKeys(values)
	if err != nil {
		return listObjectsV2Params{}, err
	}

	return listObjectsV2Params{
		prefix:       values.Get("prefix"),
		token:        string(decodedToken),
		delimiter:    values.Get("delimiter"),
		maxKeys:      maxKeys,
		fetchOwner:   values.Get("fetch-owner") == "true",
		startAfter:   values.Get("start-after"),
		encodingType: values.Get("encoding-type"),
	}, nil
}

// ListObjectVersionsHandler is the HTTP handler for the ListObjectVersions operation,
// which lists the object versions in a bucket.
func (api *API) ListObjectVersionsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "ListObjectVersions")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	params, err := getListObjectVersionsParams(r.URL.Query())
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if err := validateListObjectsParams(params.maxKeys, params.encodingType); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	listObjectVersionsInfo, err := api.objectAPI.ListObjectVersions(ctx, bucketName, params.prefix, params.marker,
		params.versionIDMarker, params.delimiter, params.maxKeys)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	resp, err := encodeResponse(generateListVersionsResponse(bucketName, params, listObjectVersionsInfo))
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeSuccessResponseXML(w, r, resp)
}

type listObjectVersionsParams struct {
	prefix          string
	marker          string
	versionIDMarker string
	delimiter       string
	maxKeys         int
	encodingType    string
}

func getListObjectVersionsParams(values url.Values) (params listObjectVersionsParams, err error) {
	maxKeys, err := getMaxKeys(values)
	if err != nil {
		return listObjectVersionsParams{}, err
	}

	if marker := values.Get("version-id-marker"); marker != "" && validateVersionID(marker) != nil {
		return listObjectVersionsParams{}, apierr.CodeInvalidVersionID
	}

	return listObjectVersionsParams{
		prefix:          values.Get("prefix"),
		marker:          values.Get("key-marker"),
		versionIDMarker: values.Get("version-id-marker"),
		delimiter:       values.Get("delimiter"),
		maxKeys:         maxKeys,
		encodingType:    values.Get("encoding-type"),
	}, nil
}

// ListMultipartUploadsHandler is the HTTP handler for the ListMultipartUploads operation,
// which lists the incomplete multipart uploads in a bucket.
func (api *API) ListMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "ListMultipartUploads")

	bucketName := mux.Vars(r)["bucket"]

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	params, err := getListMultipartUploadsParams(r.URL.Query())
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if params.maxUploads < 0 {
		api.writeErrorResponse(w, r, apierr.CodeInvalidMaxUploads)
		return
	}

	if err := validateEncodingType(params.encodingType); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	// The layer expects key-marker to start with prefix. A marker sorting before
	// prefix is ignored, and one sorting after it is past every matching key. The
	// layer is called either way, so that bucket and permission errors are reported.
	keyMarker := params.keyMarker
	pastPrefix := !strings.HasPrefix(keyMarker, params.prefix) && keyMarker > params.prefix
	if !strings.HasPrefix(keyMarker, params.prefix) {
		keyMarker = ""
	}
	listMultipartsInfo, err := api.objectAPI.ListMultipartUploads(ctx, bucketName, params.prefix, keyMarker,
		params.uploadIDMarker, params.delimiter, params.maxUploads)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}
	listMultipartsInfo.KeyMarker = params.keyMarker
	if pastPrefix {
		listMultipartsInfo.Uploads = nil
		listMultipartsInfo.CommonPrefixes = nil
		listMultipartsInfo.IsTruncated = false
		listMultipartsInfo.NextKeyMarker = ""
		listMultipartsInfo.NextUploadIDMarker = ""
	}

	response := generateListMultipartUploadsResponse(bucketName, listMultipartsInfo, params.encodingType)
	encodedSuccessResponse, err := encodeResponse(response)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeSuccessResponseXML(w, r, encodedSuccessResponse)
}

type listMultipartUploadsParams struct {
	prefix         string
	keyMarker      string
	uploadIDMarker string
	delimiter      string
	maxUploads     int
	encodingType   string
}

func getListMultipartUploadsParams(values url.Values) (params listMultipartUploadsParams, err error) {
	var maxUploads int
	if maxUploadsStr := values.Get("max-uploads"); maxUploadsStr != "" {
		var ok bool
		if maxUploads, ok = parseListInt(maxUploadsStr); !ok {
			return listMultipartUploadsParams{}, apierr.CodeInvalidMaxUploads
		}
		maxUploads = min(maxUploads, maxObjectList)
	} else {
		maxUploads = maxObjectList
	}

	return listMultipartUploadsParams{
		prefix:         values.Get("prefix"),
		keyMarker:      values.Get("key-marker"),
		uploadIDMarker: values.Get("upload-id-marker"),
		delimiter:      values.Get("delimiter"),
		maxUploads:     maxUploads,
		encodingType:   values.Get("encoding-type"),
	}, nil
}

// ListBucketsHandler is the HTTP handler for the ListBuckets operation,
// which lists the buckets owned by the requester.
func (api *API) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "ListBuckets")

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	values := r.URL.Query()
	prefix := values.Get("prefix")

	maxBuckets := 0 // unlimited
	if maxBucketsStr := values.Get("max-buckets"); maxBucketsStr != "" {
		n, err := strconv.Atoi(maxBucketsStr)
		if err != nil || n < 1 || n > maxBucketList {
			api.writeErrorResponse(w, r, apierr.CodeInvalidMaxBuckets)
			return
		}
		maxBuckets = n
	}

	var startAfter string
	if token := values.Get("continuation-token"); token != "" {
		decoded, err := base64.StdEncoding.DecodeString(token)
		if err != nil {
			api.writeErrorResponse(w, r, apierr.CodeIncorrectContinuationToken)
			return
		}
		startAfter = string(decoded)
	}

	bucketInfos, err := api.objectAPI.ListBuckets(ctx)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	// Every bucket is in bucketRegion, so any other region matches none.
	if region := values.Get("bucket-region"); region != "" && region != bucketRegion {
		bucketInfos = nil
	}

	bucketInfos = slices.DeleteFunc(bucketInfos, func(b cmd.BucketInfo) bool {
		return !strings.HasPrefix(b.Name, prefix) || b.Name <= startAfter
	})
	slices.SortFunc(bucketInfos, func(a, b cmd.BucketInfo) int {
		return strings.Compare(a.Name, b.Name)
	})

	var continuationToken string
	if maxBuckets > 0 && len(bucketInfos) > maxBuckets {
		bucketInfos = bucketInfos[:maxBuckets]
		continuationToken = base64.StdEncoding.EncodeToString([]byte(bucketInfos[maxBuckets-1].Name))
	}

	resp, err := encodeResponse(generateListBucketsResponse(bucketInfos, prefix, continuationToken))
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeSuccessResponseXML(w, r, resp)
}

// ListPartsHandler is the HTTP handler for the ListParts operation, which lists the parts that
// have been uploaded for a multipart upload.
func (api *API) ListPartsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := cmd.NewContext(r, w, "ListParts")

	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectKey, err := unescapePath(vars["object"])
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if _, err := api.verifier.Verify(r, getVirtualHostedBucket(r)); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	params, err := getListPartsParams(r.URL.Query())
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	if err := validateEncodingType(params.encodingType); err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	listPartsInfo, err := api.objectAPI.ListObjectParts(ctx, bucketName, objectKey, params.uploadID,
		params.partNumberMarker, params.maxParts, cmd.ObjectOptions{})
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	response := generateListPartsResponse(listPartsInfo, params.encodingType)
	encodedSuccessResponse, err := encodeResponse(response)
	if err != nil {
		api.writeErrorResponse(w, r, err)
		return
	}

	api.writeSuccessResponseXML(w, r, encodedSuccessResponse)
}

type listPartsParams struct {
	uploadID         string
	partNumberMarker int
	maxParts         int
	encodingType     string
}

func getListPartsParams(values url.Values) (params listPartsParams, err error) {
	var ok bool
	params.maxParts = maxPartsList
	if maxPartsStr := values.Get("max-parts"); maxPartsStr != "" {
		if params.maxParts, ok = parseListInt(maxPartsStr); !ok {
			return listPartsParams{}, apierr.CodeInvalidMaxParts
		}
		params.maxParts = min(params.maxParts, maxPartsList)
	}

	if markerStr := values.Get("part-number-marker"); markerStr != "" {
		if params.partNumberMarker, ok = parseListInt(markerStr); !ok {
			return listPartsParams{}, apierr.CodeInvalidPartNumberMarker
		}
	}

	params.uploadID = values.Get(xhttp.UploadID)
	params.encodingType = values.Get("encoding-type")

	return params, nil
}
