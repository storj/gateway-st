// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package fsobjects

import (
	"context"
	"maps"
	"path/filepath"
	"strings"
	"time"

	"storj.io/gateway/api/apierr"
	"storj.io/gateway/api/objectlayer"
)

func newRetention(r *objectlayer.ObjectRetention) *versionRetention {
	if r == nil {
		return nil
	}
	return &versionRetention{Mode: r.Mode, RetainUntil: r.RetainUntilDate.UTC()}
}

func newLegalHold(status *objectlayer.LegalHoldStatus) *bool {
	if status == nil {
		return nil
	}
	on := *status == objectlayer.LegalHoldOn
	return &on
}

// applyObjectLock checks the object lock settings of a new version against its bucket and fills
// in the bucket's defaults, like the satellite.
func applyObjectLock(bucket bucketMeta, version *versionMeta) error {
	if !bucket.ObjectLock {
		if version.Retention != nil || version.LegalHold != nil {
			return errObjectLockNotEnabled
		}
		return nil
	}
	if d := bucket.DefaultRetention; version.Retention == nil && d != nil {
		until := time.Now().UTC()
		if d.Days != nil {
			until = until.AddDate(0, 0, int(*d.Days))
		}
		if d.Years != nil {
			until = until.AddDate(int(*d.Years), 0, 0)
		}
		version.Retention = &versionRetention{Mode: d.Mode, RetainUntil: until}
	}
	if version.LegalHold == nil {
		version.LegalHold = new(bool)
	}
	return nil
}

// isProtected returns whether object lock prevents removing a version.
func isProtected(version versionMeta, bypassGovernance bool) bool {
	if version.LegalHold != nil && *version.LegalHold {
		return true
	}
	r := version.Retention
	return r != nil && r.RetainUntil.After(time.Now()) && (r.Mode == objectlayer.RetentionCompliance || !bypassGovernance)
}

// withoutObjectLock returns metadata without the object lock settings that objectInfo adds.
func withoutObjectLock(metadata map[string]string) map[string]string {
	metadata = maps.Clone(metadata)
	for _, key := range []string{amzObjectLockMode, amzObjectLockRetainUntilDate, amzObjectLockLegalHold} {
		delete(metadata, strings.ToLower(key))
	}
	return metadata
}

// GetObjectLockConfig returns a bucket's object lock configuration.
func (l *Layer) GetObjectLockConfig(ctx context.Context, bucket string) (*objectlayer.ObjectLockConfig, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	meta, err := l.readBucket(bucket)
	if err != nil {
		return nil, err
	}
	if !meta.ObjectLock {
		return nil, objectlayer.BucketObjectLockConfigNotFound{Bucket: bucket}
	}
	config := &objectlayer.ObjectLockConfig{ObjectLockEnabled: "Enabled"}
	if meta.DefaultRetention != nil {
		config.Rule = &struct {
			DefaultRetention objectlayer.DefaultRetention `xml:"DefaultRetention"`
		}{*meta.DefaultRetention}
	}
	return config, nil
}

// SetObjectLockConfig enables object lock and sets its default retention. Like in Storj, object
// lock can be enabled on a bucket with versioning enabled.
func (l *Layer) SetObjectLockConfig(ctx context.Context, bucket string, config *objectlayer.ObjectLockConfig) error {
	if config == nil || config.ObjectLockEnabled != "Enabled" {
		return apierr.CodeMalformedXML
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	meta, err := l.readBucket(bucket)
	if err != nil {
		return err
	}
	if meta.Versioning != objectlayer.VersioningEnabled {
		return errInvalidBucketStateObjectLock
	}
	meta.ObjectLock, meta.DefaultRetention = true, nil
	if config.Rule != nil {
		switch config.Rule.DefaultRetention.Mode {
		case objectlayer.RetentionGovernance, objectlayer.RetentionCompliance:
		default:
			return apierr.CodeMalformedXML
		}
		meta.DefaultRetention = &config.Rule.DefaultRetention
	}
	return writeJSON(filepath.Join(l.bucketDir(bucket), "bucket.json"), meta)
}

// readLockedVersion is readVersion for object lock requests, which need object lock enabled. The
// caller must hold l.mu.
func (l *Layer) readLockedVersion(bucket, key, versionID string) (objectMeta, int, error) {
	bucketMeta, err := l.readBucket(bucket)
	if err != nil {
		return objectMeta{}, 0, err
	}
	if !bucketMeta.ObjectLock {
		return objectMeta{}, 0, errObjectLockNotEnabled
	}
	return l.readVersion(bucket, key, versionID)
}

// SetObjectRetention sets a version's retention, or removes it if opts.Retention is nil. Like S3,
// a compliance mode retention can only be extended, and a governance mode one can only be
// shortened, removed or changed to the other mode with opts.BypassGovernanceRetention.
func (l *Layer) SetObjectRetention(ctx context.Context, bucket, object, versionID string, opts objectlayer.ObjectOptions) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	meta, i, err := l.readLockedVersion(bucket, object, versionID)
	if err != nil {
		return err
	}
	version := &meta.Versions[i]
	retention := newRetention(opts.Retention)
	if current := version.Retention; current != nil && current.RetainUntil.After(time.Now()) {
		weakened := retention == nil || retention.Mode != current.Mode || retention.RetainUntil.Before(current.RetainUntil)
		if weakened && (current.Mode == objectlayer.RetentionCompliance || !opts.BypassGovernanceRetention) {
			return errObjectProtected
		}
	}
	version.Retention = retention
	return l.writeObject(bucket, meta)
}

// GetObjectRetention returns a version's retention.
func (l *Layer) GetObjectRetention(ctx context.Context, bucket, object, versionID string) (*objectlayer.ObjectRetention, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	meta, i, err := l.readLockedVersion(bucket, object, versionID)
	if err != nil {
		return nil, err
	}
	r := meta.Versions[i].Retention
	if r == nil {
		return nil, errRetentionNotFound
	}
	return &objectlayer.ObjectRetention{Mode: r.Mode, RetainUntilDate: objectlayer.RetentionDate{Time: r.RetainUntil}}, nil
}

// SetObjectLegalHold sets a version's legal hold.
func (l *Layer) SetObjectLegalHold(ctx context.Context, bucket, object, versionID string, legalHold *objectlayer.ObjectLegalHold) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	meta, i, err := l.readLockedVersion(bucket, object, versionID)
	if err != nil {
		return err
	}
	meta.Versions[i].LegalHold = newLegalHold(&legalHold.Status)
	return l.writeObject(bucket, meta)
}

// GetObjectLegalHold returns a version's legal hold.
func (l *Layer) GetObjectLegalHold(ctx context.Context, bucket, object, versionID string) (*objectlayer.ObjectLegalHold, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	meta, i, err := l.readLockedVersion(bucket, object, versionID)
	if err != nil {
		return nil, err
	}
	status := objectlayer.LegalHoldOff
	if on := meta.Versions[i].LegalHold; on != nil && *on {
		status = objectlayer.LegalHoldOn
	}
	return &objectlayer.ObjectLegalHold{Status: status}, nil
}
