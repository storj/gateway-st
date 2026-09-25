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

	"storj.io/minio/cmd"
)

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
