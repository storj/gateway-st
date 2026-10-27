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
	"encoding/xml"
	"slices"
	"strings"
	"time"

	"storj.io/gateway/api/objectlayer"
	"storj.io/minio/cmd"
	"storj.io/minio/pkg/tags"
)

// s3XMLNS is the XML namespace of S3 responses.
const s3XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"

type grantee struct {
	XMLNS       string `xml:"xmlns:xsi,attr"`
	XMLXSI      string `xml:"xsi:type,attr"`
	Type        string `xml:"Type"`
	ID          string `xml:"ID,omitempty"`
	DisplayName string `xml:"DisplayName,omitempty"`
	URI         string `xml:"URI,omitempty"`
}

type grant struct {
	Grantee    grantee `xml:"Grantee"`
	Permission string  `xml:"Permission"`
}

type accessControlPolicy struct {
	XMLName           xml.Name  `xml:"AccessControlPolicy"`
	XMLNS             string    `xml:"xmlns,attr,omitempty"`
	Owner             cmd.Owner `xml:"Owner"`
	AccessControlList struct {
		Grants []grant `xml:"Grant"`
	} `xml:"AccessControlList"`
}

// listObjectsV2Response is cmd.ListObjectsV2Response with objects whose owner is optional.
//
// TODO: S3 echoes an empty continuation-token as an empty ContinuationToken, but omitempty drops
// it (ceph's test_bucket_listv2_continuationtoken_empty).
type listObjectsV2Response struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`

	Name                  string
	Prefix                string
	StartAfter            string `xml:"StartAfter,omitempty"`
	ContinuationToken     string `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string `xml:"NextContinuationToken,omitempty"`
	KeyCount              int
	MaxKeys               int
	Delimiter             string `xml:"Delimiter,omitempty"`
	IsTruncated           bool
	Contents              []listObjectsV2Object
	CommonPrefixes        []cmd.CommonPrefix
	EncodingType          string `xml:"EncodingType,omitempty"`
}

// listObjectsV2Object is cmd.Object whose owner is only encoded when requested with fetch-owner.
type listObjectsV2Object struct {
	Key          string
	LastModified string
	ETag         string
	Size         int64
	Owner        *cmd.Owner `xml:",omitempty"`
	StorageClass string
}

// listVersionsResponse is cmd.ListVersionsResponse whose delete markers only contain the
// elements S3 returns for them.
type listVersionsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListVersionsResult"`

	Name                string
	Prefix              string
	KeyMarker           string
	NextKeyMarker       string `xml:"NextKeyMarker,omitempty"`
	NextVersionIDMarker string `xml:"NextVersionIdMarker,omitempty"`
	VersionIDMarker     string `xml:"VersionIdMarker"`
	MaxKeys             int
	Delimiter           string `xml:"Delimiter,omitempty"`
	IsTruncated         bool
	CommonPrefixes      []cmd.CommonPrefix
	Versions            []objectVersion
	EncodingType        string `xml:"EncodingType,omitempty"`
}

// objectVersion is cmd.ObjectVersion that is encoded as a Version or DeleteMarker element
// containing only the elements S3 returns for it.
type objectVersion cmd.ObjectVersion

// MarshalXML implements xml.Marshaler.
func (o objectVersion) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type deleteMarker struct {
		Key          string
		VersionID    string `xml:"VersionId"`
		IsLatest     bool
		LastModified string
		Owner        cmd.Owner
	}
	marker := deleteMarker{
		Key:          o.Key,
		VersionID:    o.VersionID,
		IsLatest:     o.IsLatest,
		LastModified: o.LastModified,
		Owner:        o.Owner,
	}

	if o.IsDeleteMarker {
		start.Name.Local = "DeleteMarker"
		return e.EncodeElement(marker, start)
	}

	// Unlike in DeleteMarker, S3 puts Owner last in Version.
	start.Name.Local = "Version"
	return e.EncodeElement(struct {
		Key          string
		VersionID    string `xml:"VersionId"`
		IsLatest     bool
		LastModified string
		ETag         string
		Size         int64
		StorageClass string
		Owner        cmd.Owner
	}{o.Key, o.VersionID, o.IsLatest, o.LastModified, o.ETag, o.Size, o.StorageClass, o.Owner}, start)
}

// listBucketsResponse is cmd.ListBucketsResponse with pagination.
type listBucketsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult"`

	Owner   cmd.Owner
	Buckets struct {
		Buckets []cmd.Bucket `xml:"Bucket"`
	}
	ContinuationToken string `xml:"ContinuationToken,omitempty"`
	Prefix            string `xml:"Prefix,omitempty"`
}

// taggingResponse is tags.Tags with a namespace and its tags sorted by key. tags.Tags encodes
// them in map order.
type taggingResponse struct {
	XMLName xml.Name `xml:"Tagging"`
	XMLNS   string   `xml:"xmlns,attr"`
	TagSet  struct {
		Tags []tags.Tag `xml:"Tag"`
	}
}

func newTaggingResponse(t *tags.Tags) taggingResponse {
	resp := taggingResponse{XMLNS: s3XMLNS}
	if t == nil || t.TagSet == nil {
		return resp
	}
	for key, value := range t.ToMap() {
		resp.TagSet.Tags = append(resp.TagSet.Tags, tags.Tag{Key: key, Value: value})
	}
	slices.SortFunc(resp.TagSet.Tags, func(a, b tags.Tag) int { return strings.Compare(a.Key, b.Key) })
	return resp
}

// postResponse is cmd.PostResponse with a namespace.
type postResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ PostResponse"`
	cmd.PostResponse
}

// deleteError is cmd.DeleteError without an empty VersionId.
type deleteError struct {
	Code      string
	Message   string
	Key       string
	VersionID string `xml:"VersionId,omitempty"`
}

// deleteObjectsResponse is cmd.DeleteObjectsResponse with deleteError errors.
type deleteObjectsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DeleteResult"`

	DeletedObjects []objectlayer.DeletedObject `xml:"Deleted,omitempty"`
	Errors         []deleteError               `xml:"Error,omitempty"`
}

// objectRetentionResponse is objectlayer.ObjectRetention with a RetainUntilDate in milliseconds.
type objectRetentionResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Retention"`

	Mode            objectlayer.RetentionMode `xml:"Mode,omitempty"`
	RetainUntilDate string                    `xml:"RetainUntilDate,omitempty"`
}

// newObjectRetentionResponse encodes r's RetainUntilDate in milliseconds, like S3. Dates are
// truncated to the millisecond when they're set, but older objects may have a fraction of one, so
// it's rounded up: a client can then delete the object once the reported date has passed, and
// setting the reported date again doesn't shorten a compliance mode retention.
func newObjectRetentionResponse(r *objectlayer.ObjectRetention) objectRetentionResponse {
	resp := objectRetentionResponse{Mode: r.Mode}
	if date := r.RetainUntilDate.Time; !date.IsZero() {
		if rounded := date.Truncate(time.Millisecond); rounded.Before(date) {
			date = rounded.Add(time.Millisecond)
		}
		resp.RetainUntilDate = date.UTC().Format(iso8601Milli)
	}
	return resp
}
