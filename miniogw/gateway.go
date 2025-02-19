// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"sync"
	"time"

	"github.com/spacemonkeygo/monkit/v3"

	"storj.io/common/version"
	minio "storj.io/minio/cmd"
	"storj.io/minio/pkg/auth"
	objectlock "storj.io/minio/pkg/bucket/object/lock"
	"storj.io/minio/pkg/madmin"
)

var (
	mon = monkit.Package()
)

// Gateway is the implementation of cmd.Gateway.
type Gateway struct {
	dataDir string
}

// NewStorjGateway creates a new Storj S3 gateway.
func NewStorjGateway(dataDir string) *Gateway {
	return &Gateway{dataDir: dataDir}
}

// Name implements cmd.Gateway.
func (gateway *Gateway) Name() string {
	return "storj"
}

// NewGatewayLayer implements cmd.Gateway.
func (gateway *Gateway) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	return &gatewayLayer{dataDir: gateway.dataDir}, nil
}

// Production implements cmd.Gateway.
func (gateway *Gateway) Production() bool {
	return version.Build.Release
}

type gatewayLayer struct {
	dataDir string
	lockMap sync.Map

	minio.GatewayUnsupported
}

// Shutdown is a no-op.
func (layer *gatewayLayer) Shutdown(ctx context.Context) (err error) {
	return nil
}

func (layer *gatewayLayer) StorageInfo(ctx context.Context) (minio.StorageInfo, []error) {
	return minio.StorageInfo{
		Backend: madmin.BackendInfo{
			Type:          madmin.Gateway,
			GatewayOnline: true,
		},
	}, nil
}

func (layer *gatewayLayer) MakeBucketWithLocation(ctx context.Context, name string, opts minio.BucketOptions) (err error) {
	defer mon.Task()(&ctx)(&err)

	err = os.Mkdir(path.Join(layer.dataDir, name), 0755)
	if os.IsExist(err) {
		return minio.BucketAlreadyExists{Bucket: name}
	}

	return err
}

func (layer *gatewayLayer) GetBucketInfo(ctx context.Context, bucketName string) (bucketInfo minio.BucketInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	info, err := os.Stat(path.Join(layer.dataDir, bucketName))
	if os.IsNotExist(err) {
		return minio.BucketInfo{}, minio.BucketNotFound{Bucket: bucketName}
	}
	if err != nil {
		return minio.BucketInfo{}, err
	}

	return minio.BucketInfo{
		Name:    bucketName,
		Created: info.ModTime(),
	}, nil
}

func (layer *gatewayLayer) ListBuckets(ctx context.Context) (items []minio.BucketInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	return nil, minio.NotImplemented{}
}

func (layer *gatewayLayer) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) (err error) {
	defer mon.Task()(&ctx)(&err)

	return minio.NotImplemented{}
}

func (layer *gatewayLayer) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (_ minio.ListObjectsInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	return minio.ListObjectsInfo{}, minio.NotImplemented{}
}

func (layer *gatewayLayer) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (_ minio.ListObjectsV2Info, err error) {
	defer mon.Task()(&ctx)(&err)

	return minio.ListObjectsV2Info{}, minio.NotImplemented{}
}

func (layer *gatewayLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (reader *minio.GetObjectReader, err error) {
	defer mon.Task()(&ctx)(&err)

	info, content, err := layer.syncReadFile(bucket, object)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, minio.ObjectNotFound{Bucket: bucket, Object: object}
		}
		return nil, err
	}

	objectInfo := minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}

	return minio.NewGetObjectReaderFromReader(bytes.NewReader(content), objectInfo, opts)
}

func (layer *gatewayLayer) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	info, err := layer.syncStatFile(bucket, object)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return minio.ObjectInfo{}, minio.ObjectNotFound{Bucket: bucket, Object: object}
		}
		return minio.ObjectInfo{}, err
	}

	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}, nil
}

func (layer *gatewayLayer) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	content, err := io.ReadAll(data)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	err = layer.syncWriteFile(bucket, object, content)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		Size:    int64(len(content)),
		ModTime: time.Now(),
	}, nil
}

func (layer *gatewayLayer) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, destOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)
	return minio.ObjectInfo{}, minio.NotImplemented{}
}

func (layer *gatewayLayer) DeleteObject(ctx context.Context, bucket, objectPath string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)
	return minio.ObjectInfo{}, minio.NotImplemented{}
}

func (layer *gatewayLayer) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) ([]minio.DeletedObject, []error) {
	return nil, []error{minio.NotImplemented{}}
}

func (layer *gatewayLayer) GetObjectLockConfig(ctx context.Context, bucket string) (*objectlock.Config, error) {
	return &objectlock.Config{}, nil
}

func (layer *gatewayLayer) SetObjectLockConfig(ctx context.Context, bucket string, config *objectlock.Config) error {
	return nil
}

func (layer *gatewayLayer) getFilePath(bucket, object string) string {
	return path.Join(layer.dataDir, bucket, object)
}

func (layer *gatewayLayer) getLock(bucket, object string) *sync.RWMutex {
	key := path.Join(bucket, object)
	actual, _ := layer.lockMap.LoadOrStore(key, &sync.RWMutex{})
	return actual.(*sync.RWMutex)
}

func (layer *gatewayLayer) syncStatFile(bucket, object string) (fs.FileInfo, error) {
	filePath := layer.getFilePath(bucket, object)

	lock := layer.getLock(bucket, object)
	lock.RLock()
	defer lock.RUnlock()

	return os.Stat(filePath)
}

func (layer *gatewayLayer) syncReadFile(bucket, object string) (fs.FileInfo, []byte, error) {
	filePath := layer.getFilePath(bucket, object)

	lock := layer.getLock(bucket, object)
	lock.RLock()
	defer lock.RUnlock()

	info, err := os.Stat(filePath)
	if err != nil {
		return nil, nil, err
	}

	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, nil, err
	}

	return info, content, nil
}

func (layer *gatewayLayer) syncWriteFile(bucket, object string, content []byte) error {
	filePath := layer.getFilePath(bucket, object)

	lock := layer.getLock(bucket, object)
	lock.Lock()
	defer lock.Unlock()

	err := os.WriteFile(filePath, content, 0644)
	if errors.Is(err, os.ErrNotExist) {
		err = os.MkdirAll(path.Dir(filePath), 0755)
		if err != nil {
			return err
		}
		err = os.WriteFile(filePath, content, 0644)
	}
	return err
}
