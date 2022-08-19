// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"context"
	"regexp"
	"strings"

	"github.com/zeebo/errs"
)

var ipRegexp = regexp.MustCompile(`^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$`)

// validateBucket will check the given bucket conforms to our validation criteria.
// Note that these rules are copied from metainfo endpoint.validateBucket() which
// is the source of truth for these rules, and any changes there need to also be
// made here.
func validateBucket(ctx context.Context, bucket string) (err error) {
	defer mon.Task()(&ctx)(&err)

	if len(bucket) == 0 {
		return errs.New("no bucket specified")
	}

	if len(bucket) < 3 || len(bucket) > 63 {
		return errs.New("bucket name must be at least 3 and no more than 63 characters long")
	}

	// Regexp not used because benchmark shows it will be slower for valid bucket names
	// https://gist.github.com/mniewrzal/49de3af95f36e63e88fac24f565e444c
	labels := strings.Split(bucket, ".")
	for _, label := range labels {
		err = validateBucketLabel(label)
		if err != nil {
			return err
		}
	}

	if ipRegexp.MatchString(bucket) {
		return errs.New("bucket name cannot be formatted as an IP address")
	}

	return nil
}

func validateBucketLabel(label string) error {
	if len(label) == 0 {
		return errs.New("bucket label cannot be empty")
	}

	if !isLowerLetter(label[0]) && !isDigit(label[0]) {
		return errs.New("bucket label must start with a lowercase letter or number")
	}

	if label[0] == '-' || label[len(label)-1] == '-' {
		return errs.New("bucket label cannot start or end with a hyphen")
	}

	for i := 1; i < len(label)-1; i++ {
		if !isLowerLetter(label[i]) && !isDigit(label[i]) && (label[i] != '-') && (label[i] != '.') {
			return errs.New("bucket name must contain only lowercase letters, numbers or hyphens")
		}
	}

	return nil
}

func isLowerLetter(r byte) bool {
	return r >= 'a' && r <= 'z'
}

func isDigit(r byte) bool {
	return r >= '0' && r <= '9'
}
