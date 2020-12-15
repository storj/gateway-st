// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"context"
	"crypto/md5" /* #nosec G501 */ // Is only used for calculating a hash of the ETags of the all the parts of a multipart upload.
	"encoding/hex"
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/hash"
	"github.com/zeebo/errs"

	"storj.io/common/context2"
	"storj.io/uplink"
	"storj.io/uplink/private/storage/streams"
)

func (layer *gatewayLayer) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	ctx = context2.WithoutCancellation(ctx)

	defer mon.Task()(&ctx)(&err)
	if err := uplink.CustomMetadata(opts.UserDefined).Verify(); err != nil {
		return "", err
	}

	uploads := layer.multipart

	upload, err := uploads.Create(bucket, object, opts.UserDefined)
	if err != nil {
		return "", err
	}

	// Scenario: if a client starts uploading an object and then dies, when
	// is it safe to restart uploading?
	// * with libuplink natively, it's immediately safe. the client died, so
	//   it stopped however far it got, and it can start over.
	// * with the gateway, unless we do the following line it is impossible
	//   to know when it's safe to start uploading again. it might be up to
	//   30 minutes later that it's safe! the reason is if the client goes
	//   away, the gateway keeps running, and may down the road decide the
	//   request was canceled, and so the object should get deleted.
	// So, to make clients of the gateway's behavior match libuplink, we are
	// disabling the cleanup on cancel that libuplink tries to do. we may
	// want to consider disabling this for libuplink entirely.
	// The following line currently only impacts UploadObject calls.
	ctx = streams.DisableDeleteOnCancel(ctx)

	// TODO: this can now be done without this separate goroutine
	stream, err := layer.project.UploadObject(ctx, bucket, object, nil)
	if err != nil {
		uploads.RemoveByID(upload.ID)
		upload.fail(err)
		return "", err
	}

	go func() {
		_, err = io.Copy(stream, upload.Stream)
		if err != nil {
			uploads.RemoveByID(upload.ID)
			abortErr := stream.Abort()
			upload.fail(errs.Combine(err, abortErr))
			return
		}

		etag, etagErr := upload.etag()
		if etagErr != nil {
			uploads.RemoveByID(upload.ID)
			abortErr := stream.Abort()
			upload.fail(errs.Combine(etagErr, abortErr))
			return
		}

		metadata := uplink.CustomMetadata(opts.UserDefined).Clone()
		metadata["s3:etag"] = etag

		err = stream.SetCustomMetadata(ctx, metadata)
		if err != nil {
			uploads.RemoveByID(upload.ID)
			abortErr := stream.Abort()
			upload.fail(errs.Combine(err, abortErr))
			return
		}

		err = stream.Commit()
		uploads.RemoveByID(upload.ID)
		if err != nil {
			upload.fail(errs.Combine(err, err))
			return
		}

		upload.complete(minioObjectInfo(bucket, etag, stream.Info()))
	}()

	return upload.ID, nil
}

func (layer *gatewayLayer) GetMultipartInfo(ctx context.Context, bucket string, object string, uploadID string, opts minio.ObjectOptions) (info minio.MultipartInfo, err error) {
	info.Bucket = bucket
	info.Object = object
	info.UploadID = uploadID
	return info, nil
}

func (layer *gatewayLayer) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	uploads := layer.multipart

	upload, err := uploads.Get(bucket, object, uploadID)
	if err != nil {
		return minio.PartInfo{}, err
	}

	part, err := upload.Stream.AddPart(partID, data.Reader)
	if err != nil {
		return minio.PartInfo{}, err
	}

	err = <-part.Done
	if err != nil {
		return minio.PartInfo{}, err
	}

	partInfo := minio.PartInfo{
		PartNumber:   part.Number,
		LastModified: time.Now(),
		ETag:         data.MD5CurrentHexString(),
		Size:         atomic.LoadInt64(&part.Size),
	}

	upload.addCompletedPart(partInfo)

	return partInfo, nil
}

func (layer *gatewayLayer) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, _ minio.ObjectOptions) (err error) {
	defer mon.Task()(&ctx)(&err)

	uploads := layer.multipart

	upload, err := uploads.Remove(bucket, object, uploadID)
	if err != nil {
		return err
	}

	if upload != nil {
		errAbort := Error.New("abort")
		upload.Stream.Abort(errAbort)
		r := <-upload.Done
		if !errors.Is(r.Error, errAbort) {
			return r.Error
		}
	}
	return nil
}

func (layer *gatewayLayer) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	uploads := layer.multipart
	upload, err := uploads.Remove(bucket, object, uploadID)
	if err != nil {
		return minio.ObjectInfo{}, err
	}
	if upload == nil {
		return minio.ObjectInfo{}, nil
	}

	// notify stream that there aren't more parts coming
	upload.Stream.Close()
	// wait for completion
	result := <-upload.Done
	// return the final info
	return result.Info, result.Error
}

func (layer *gatewayLayer) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	uploads := layer.multipart
	upload, err := uploads.Get(bucket, object, uploadID)
	if err != nil {
		return minio.ListPartsInfo{}, err
	}

	list := minio.ListPartsInfo{}

	list.Bucket = bucket
	list.Object = object
	list.UploadID = uploadID
	list.PartNumberMarker = partNumberMarker
	list.MaxParts = maxParts
	list.UserDefined = upload.Metadata
	list.Parts = upload.getCompletedParts()

	sort.Slice(list.Parts, func(i, k int) bool {
		return list.Parts[i].PartNumber < list.Parts[k].PartNumber
	})

	var first int
	for i, p := range list.Parts {
		first = i
		if partNumberMarker <= p.PartNumber {
			break
		}
	}

	list.Parts = list.Parts[first:]
	if len(list.Parts) > maxParts {
		list.NextPartNumberMarker = list.Parts[maxParts].PartNumber
		list.Parts = list.Parts[:maxParts]
		list.IsTruncated = true
	}

	return list, nil
}

// ListMultipartUploads lists all multipart uploads.
func (layer *gatewayLayer) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi minio.ListMultipartsInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	uploads := layer.multipart

	lmi.Prefix = prefix
	lmi.KeyMarker = keyMarker
	lmi.UploadIDMarker = uploadIDMarker
	lmi.Delimiter = delimiter
	lmi.MaxUploads = maxUploads
	lmi.IsTruncated = false

	uploads.mu.RLock()
	defer uploads.mu.RUnlock()

	for _, upload := range uploads.pending {
		// TODO support markers
		if upload.Bucket == bucket && strings.HasPrefix(upload.Object, prefix) {
			lmi.Uploads = append(lmi.Uploads, minio.MultipartInfo{
				UploadID: upload.ID,
				Object:   upload.Object,
			})

			if len(lmi.Uploads) > maxUploads {
				lmi.Uploads = lmi.Uploads[:maxUploads]
				lmi.IsTruncated = true
				break
			}
		}
	}

	return lmi, nil
}

// TODO: implement
// func (layer *gatewayLayer) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64, srcInfo minio.ObjectInfo) (info minio.PartInfo, err error) {

// MultipartUploads manages pending multipart uploads.
type MultipartUploads struct {
	mu      sync.RWMutex
	lastID  int
	pending map[string]*MultipartUpload
}

// NewMultipartUploads creates new MultipartUploads.
func NewMultipartUploads() *MultipartUploads {
	return &MultipartUploads{
		pending: map[string]*MultipartUpload{},
	}
}

// Create creates a new upload.
func (uploads *MultipartUploads) Create(bucket, object string, metadata map[string]string) (*MultipartUpload, error) {
	uploads.mu.Lock()
	defer uploads.mu.Unlock()

	for id, upload := range uploads.pending {
		if upload.Bucket == bucket && upload.Object == object {
			upload.Stream.Abort(Error.New("aborted by another upload to the same location"))
			delete(uploads.pending, id)
		}
	}

	uploads.lastID++
	uploadID := "Upload" + strconv.Itoa(uploads.lastID)

	upload := NewMultipartUpload(uploadID, bucket, object, metadata)
	uploads.pending[uploadID] = upload

	return upload, nil
}

// Get finds a pending upload.
func (uploads *MultipartUploads) Get(bucket, object, uploadID string) (*MultipartUpload, error) {
	uploads.mu.RLock()
	defer uploads.mu.RUnlock()

	upload, ok := uploads.pending[uploadID]
	if !ok {
		return nil, Error.New("pending upload %q missing", uploadID)
	}
	if upload.Bucket != bucket || upload.Object != object {
		return nil, Error.New("pending upload %q bucket/object name mismatch", uploadID)
	}

	return upload, nil
}

// Remove returns and removes a pending upload.
func (uploads *MultipartUploads) Remove(bucket, object, uploadID string) (*MultipartUpload, error) {
	uploads.mu.Lock()
	defer uploads.mu.Unlock()

	upload, ok := uploads.pending[uploadID]
	if !ok {
		// The multipart upload may have been removed automatically due to finishing.
		// Ideally this should return an error as well, however due to the concurrent merging
		// of parts, the implementation cannot be stateless.
		return nil, nil
	}
	if upload.Bucket != bucket || upload.Object != object {
		return nil, Error.New("pending upload %q bucket/object name mismatch", uploadID)
	}

	delete(uploads.pending, uploadID)

	return upload, nil
}

// RemoveByID removes pending upload by id.
func (uploads *MultipartUploads) RemoveByID(uploadID string) {
	uploads.mu.Lock()
	defer uploads.mu.Unlock()

	delete(uploads.pending, uploadID)
}

// MultipartUpload is partial info about a pending upload.
type MultipartUpload struct {
	ID       string
	Bucket   string
	Object   string
	Metadata map[string]string
	Done     chan (*MultipartUploadResult)
	Stream   *MultipartStream

	mu        sync.Mutex
	completed []minio.PartInfo
}

// MultipartUploadResult contains either an Error or the uploaded ObjectInfo.
type MultipartUploadResult struct {
	Error error
	Info  minio.ObjectInfo
}

// NewMultipartUpload creates a new MultipartUpload.
func NewMultipartUpload(uploadID string, bucket, object string, metadata map[string]string) *MultipartUpload {
	upload := &MultipartUpload{
		ID:       uploadID,
		Bucket:   bucket,
		Object:   object,
		Metadata: metadata,
		Done:     make(chan *MultipartUploadResult, 1),
		Stream:   NewMultipartStream(),
	}
	return upload
}

// addCompletedPart adds a completed part to the list.
func (upload *MultipartUpload) addCompletedPart(part minio.PartInfo) {
	upload.mu.Lock()
	defer upload.mu.Unlock()

	upload.completed = append(upload.completed, part)
}

func (upload *MultipartUpload) getCompletedParts() []minio.PartInfo {
	upload.mu.Lock()
	defer upload.mu.Unlock()

	return append([]minio.PartInfo{}, upload.completed...)
}

// fail aborts the upload with an error.
func (upload *MultipartUpload) fail(err error) {
	upload.Done <- &MultipartUploadResult{Error: err}
	close(upload.Done)
}

// complete completes the upload.
func (upload *MultipartUpload) complete(info minio.ObjectInfo) {
	upload.Done <- &MultipartUploadResult{Info: info}
	close(upload.Done)
}

func (upload *MultipartUpload) etag() (string, error) {
	var hashes []byte
	parts := upload.getCompletedParts()
	for _, part := range parts {
		md5, err := hex.DecodeString(canonicalEtag(part.ETag))
		if err != nil {
			hashes = append(hashes, []byte(part.ETag)...)
		} else {
			hashes = append(hashes, md5...)
		}
	}

	/* #nosec G401 */ // ETags aren't security sensitive
	sum := md5.Sum(hashes)
	return hex.EncodeToString(sum[:]) + "-" + strconv.Itoa(len(parts)), nil
}

func canonicalEtag(etag string) string {
	etag = strings.Trim(etag, `"`)
	p := strings.IndexByte(etag, '-')
	if p >= 0 {
		return etag[:p]
	}
	return etag
}

// MultipartStream serializes multiple readers into a single reader.
type MultipartStream struct {
	mu          sync.Mutex
	moreParts   sync.Cond
	err         error
	closed      bool
	finished    bool
	nextID      int
	nextNumber  int
	currentPart *StreamPart
	parts       []*StreamPart
}

// StreamPart is a reader waiting in MultipartStream.
type StreamPart struct {
	Number int
	ID     int
	Size   int64
	Reader *hash.Reader
	Done   chan error
}

// NewMultipartStream creates a new MultipartStream.
func NewMultipartStream() *MultipartStream {
	stream := &MultipartStream{}
	stream.moreParts.L = &stream.mu
	stream.nextID = 1
	return stream
}

// Abort aborts the stream reading.
func (stream *MultipartStream) Abort(err error) {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	if stream.finished {
		return
	}

	if stream.err == nil {
		stream.err = err
	}
	stream.finished = true
	stream.closed = true

	for _, part := range stream.parts {
		part.Done <- err
		close(part.Done)
	}
	stream.parts = nil

	stream.moreParts.Broadcast()
}

// Close closes the stream, but lets it complete.
func (stream *MultipartStream) Close() {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	stream.closed = true
	stream.moreParts.Broadcast()
}

// Read implements io.Reader interface, blocking when there's no part.
func (stream *MultipartStream) Read(data []byte) (n int, err error) {
	stream.mu.Lock()
	for {
		// has an error occurred?
		if stream.err != nil {
			stream.mu.Unlock()
			return 0, Error.Wrap(err)
		}
		// still uploading the current part?
		if stream.currentPart != nil {
			break
		}
		// do we have the next part?
		if len(stream.parts) > 0 && stream.nextID == stream.parts[0].ID {
			stream.currentPart = stream.parts[0]
			stream.parts = stream.parts[1:]
			stream.nextID++
			break
		}
		// we don't have the next part and are closed, hence we are complete
		if stream.closed {
			stream.finished = true
			stream.mu.Unlock()
			return 0, io.EOF
		}

		stream.moreParts.Wait()
	}
	stream.mu.Unlock()

	// read as much as we can
	n, err = stream.currentPart.Reader.Read(data)
	atomic.AddInt64(&stream.currentPart.Size, int64(n))

	if err == io.EOF {
		// the part completed, hence advance to the next one
		err = nil
		close(stream.currentPart.Done)
		stream.currentPart = nil
	} else if err != nil {
		// something bad happened, abort the whole thing
		stream.Abort(err)
		return n, Error.Wrap(err)
	}

	return n, err
}

// AddPart adds a new part to the stream to wait.
func (stream *MultipartStream) AddPart(partID int, data *hash.Reader) (*StreamPart, error) {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	if partID < stream.nextID {
		return nil, Error.New("part %d already uploaded, next part ID is %d", partID, stream.nextID)
	}

	for _, p := range stream.parts {
		if p.ID == partID {
			// Replace the reader of this part with the new one.
			// This could happen if the read timeout for this part has expired
			// and the client tries to upload the part again.
			p.Reader = data
			return p, nil
		}
	}

	stream.nextNumber++
	part := &StreamPart{
		Number: stream.nextNumber - 1,
		ID:     partID,
		Size:   0,
		Reader: data,
		Done:   make(chan error, 1),
	}

	stream.parts = append(stream.parts, part)
	sort.Slice(stream.parts, func(i, k int) bool {
		return stream.parts[i].ID < stream.parts[k].ID
	})

	stream.moreParts.Broadcast()

	return part, nil
}
