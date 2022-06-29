// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

// MinioConfig is a configuration struct that keeps details about starting Minio.
type MinioConfig struct {
	AccessKey string `help:"Minio Access Key to use" default:"insecure-dev-access-key" basic-help:"true"`
	SecretKey string `help:"Minio Secret Key to use" default:"insecure-dev-secret-key" basic-help:"true"`
	Dir       string `help:"Minio generic server config path" default:"$CONFDIR/minio"`
}

// ServerConfig determines how minio listens for requests.
type ServerConfig struct {
	Address string `help:"address to serve S3 api over" default:"127.0.0.1:7777" basic-help:"true"`
}

// S3CompatibilityConfig is a configuration struct that determines details about
// how strict the gateway should be S3-compatible.
type S3CompatibilityConfig struct {
	IncludeCustomMetadataListing bool  `help:"include custom metadata in S3's ListObjects, ListObjectsV2 and ListMultipartUploads responses" default:"true"`
	MaxKeysLimit                 int   `help:"MaxKeys parameter limit for S3's ListObjects and ListObjectsV2 responses" default:"1000"`
	MaxKeysExhaustiveLimit       int   `help:"maximum number of items to list for gateway-side filtering using arbitrary delimiter/prefix" default:"100000"`
	MaxUploadsLimit              int   `help:"MaxUploads parameter limit for S3's ListMultipartUploads responses" default:"1000"`
	FullyCompatibleListing       bool  `help:"make ListObjects(V2) fully S3-compatible (specifically: always return lexicographically ordered results) but slow" default:"false"`
	DisableCopyObject            bool  `help:"return 501 (Not Implemented) for CopyObject calls" default:"false"`
	MinPartSize                  int64 `help:"minimum part size for multipart uploads" default:"5242880"` // 5 MiB
}
