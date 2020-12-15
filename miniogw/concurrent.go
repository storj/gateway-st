// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"sync"
)

// activeUploads tracks all active uploads in the current gateway.
type activeUploads struct {
	mu    sync.Mutex
	items map[activeUpload]struct{}
}

type activeUpload struct {
	bucketName string
	objectKey  string
}

func newActiveUploads() *activeUploads {
	return &activeUploads{
		items: map[activeUpload]struct{}{},
	}
}

func (uploads *activeUploads) tryAdd(bucketName, objectKey string) (ok bool) {
	uploads.mu.Lock()
	defer uploads.mu.Unlock()

	id := activeUpload{bucketName, objectKey}
	if _, exists := uploads.items[id]; exists {
		return false
	}

	uploads.items[id] = struct{}{}
	return true
}

func (uploads *activeUploads) remove(bucketName, objectKey string) {
	uploads.mu.Lock()
	defer uploads.mu.Unlock()

	id := activeUpload{bucketName, objectKey}
	delete(uploads.items, id)
}
