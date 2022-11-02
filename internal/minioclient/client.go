// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package minioclient

import (
	"bytes"
	"errors"
	"io"

	minio "github.com/minio/minio-go/v6"
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"
)

var (
	// MinioError is class for minio errors.
	MinioError = errs.Class("minio error")
	mon        = monkit.Package()
)

// Config is the setup for a particular client.
type Config struct {
	S3Gateway     string
	Satellite     string
	AccessKey     string
	SecretKey     string
	APIKey        string
	EncryptionKey string
	NoSSL         bool
	ConfigDir     string
}

// Client is the common interface for different implementations.
type Client interface {
	MakeBucket(bucket, location string) error
	RemoveBucket(bucket string) error
	ListBuckets() ([]string, error)

	Upload(bucket, objectName string, data []byte, metadata map[string]string) error
	Download(bucket, objectName string, buffer []byte) ([]byte, error)
	Delete(bucket, objectName string) error
	ListObjects(bucket, prefix string) ([]string, error)
}

// Minio implements basic S3 Client with minio.
type Minio struct {
	API *minio.Client
}

// NewMinio creates new Client.
func NewMinio(conf Config) (Client, error) {
	api, err := minio.New(conf.S3Gateway, conf.AccessKey, conf.SecretKey, !conf.NoSSL)
	if err != nil {
		return nil, MinioError.Wrap(err)
	}
	return &Minio{api}, nil
}

// MakeBucket makes a new bucket.
func (client *Minio) MakeBucket(bucket, location string) (err error) {
	defer mon.Task()(nil)(&err)

	err = client.API.MakeBucket(bucket, location)
	if err != nil {
		return MinioError.Wrap(err)
	}
	return nil
}

// RemoveBucket removes a bucket.
func (client *Minio) RemoveBucket(bucket string) (err error) {
	defer mon.Task()(nil)(&err)

	err = client.API.RemoveBucket(bucket)
	if err != nil {
		return MinioError.Wrap(err)
	}
	return nil
}

// ListBuckets lists all buckets.
func (client *Minio) ListBuckets() (names []string, err error) {
	defer mon.Task()(nil)(&err)

	buckets, err := client.API.ListBuckets()
	if err != nil {
		return nil, MinioError.Wrap(err)
	}

	for _, bucket := range buckets {
		names = append(names, bucket.Name)
	}
	return names, nil
}

// Upload uploads object data to the specified path.
func (client *Minio) Upload(bucket, objectName string, data []byte, metadata map[string]string) (err error) {
	defer mon.Task()(nil)(&err)

	_, err = client.API.PutObject(
		bucket, objectName,
		bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{
			ContentType:  "application/octet-stream",
			UserMetadata: metadata,
		})
	if err != nil {
		return MinioError.Wrap(err)
	}
	return nil
}

// UploadMultipart uses multipart uploads, has hardcoded threshold.
func (client *Minio) UploadMultipart(bucket, objectName string, data []byte, partSize int, threshold int, metadata map[string]string) (err error) {
	defer mon.Task()(nil)(&err)

	_, err = client.API.PutObject(
		bucket, objectName,
		bytes.NewReader(data), -1,
		minio.PutObjectOptions{
			ContentType:  "application/octet-stream",
			PartSize:     uint64(partSize),
			UserMetadata: metadata,
		})
	if err != nil {
		return MinioError.Wrap(err)
	}
	return nil
}

// Download downloads object data.
func (client *Minio) Download(bucket, objectName string, buffer []byte) (_ []byte, err error) {
	defer mon.Task()(nil)(&err)

	reader, err := client.API.GetObject(bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, MinioError.Wrap(err)
	}
	defer func() { _ = reader.Close() }()

	n, err := reader.Read(buffer[:cap(buffer)])
	if !errors.Is(err, io.EOF) {
		rest, err := io.ReadAll(reader)
		if errors.Is(err, io.EOF) {
			err = nil
		}
		if err != nil {
			return nil, MinioError.Wrap(err)
		}
		buffer = append(buffer, rest...)
		n = len(buffer)
	}

	buffer = buffer[:n]
	return buffer, nil
}

// Delete deletes object.
func (client *Minio) Delete(bucket, objectName string) (err error) {
	defer mon.Task()(nil)(&err)

	err = client.API.RemoveObject(bucket, objectName)
	if err != nil {
		return MinioError.Wrap(err)
	}
	return nil
}

// ListObjects lists objects.
func (client *Minio) ListObjects(bucket, prefix string) (names []string, err error) {
	defer mon.Task()(nil)(&err)

	doneCh := make(chan struct{})
	defer close(doneCh)

	for message := range client.API.ListObjects(bucket, prefix, false, doneCh) {
		names = append(names, message.Key)
	}

	return names, nil
}
