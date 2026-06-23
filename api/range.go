// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api

import (
	"strconv"
	"strings"

	"storj.io/gateway/api/apierr"
	"storj.io/minio/cmd"
)

const byteRangePrefix = "bytes="

// ParseRange parses an HTTP range string as an HTTPRangeSpec. It returns apierr.CodeMalformedCopySourceRange
// if the range string is malformed and apierr.CodeInvalidCopySourceRange if it is well-formed but
// semantically invalid.
func ParseRange(rangeString string) (*cmd.HTTPRangeSpec, error) {
	byteRangeStr, ok := strings.CutPrefix(rangeString, byteRangePrefix)
	if !ok {
		return nil, apierr.CodeMalformedCopySourceRange
	}

	beginOffsetStr, endOffsetStr, ok := strings.Cut(byteRangeStr, "-")
	if !ok {
		return nil, apierr.CodeMalformedCopySourceRange
	}

	beginOffset, hasBeginOffset, err := parseRangeOffset(beginOffsetStr)
	if err != nil {
		return nil, apierr.CodeMalformedCopySourceRange
	}

	endOffset, hasEndOffset, err := parseRangeOffset(endOffsetStr)
	if err != nil {
		return nil, apierr.CodeMalformedCopySourceRange
	}

	switch {
	case hasBeginOffset && hasEndOffset:
		if beginOffset > endOffset {
			return nil, apierr.CodeInvalidCopySourceRange
		}
		return &cmd.HTTPRangeSpec{
			IsSuffixLength: false,
			Start:          beginOffset,
			End:            endOffset,
		}, nil
	case hasBeginOffset:
		return &cmd.HTTPRangeSpec{
			IsSuffixLength: false,
			Start:          beginOffset,
			End:            -1,
		}, nil
	case hasEndOffset:
		if endOffset == 0 {
			return nil, apierr.CodeInvalidCopySourceRange
		}
		return &cmd.HTTPRangeSpec{
			IsSuffixLength: true,
			Start:          -endOffset,
			End:            -1,
		}, nil
	default:
		return nil, apierr.CodeMalformedCopySourceRange
	}
}

func parseRangeOffset(s string) (offset int64, present bool, err error) {
	if s == "" {
		return 0, false, nil
	}
	if s[0] == '+' || s[0] == '-' {
		return 0, false, apierr.CodeMalformedCopySourceRange
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, false, apierr.CodeMalformedCopySourceRange
	}
	return n, true, nil
}

// parseRangeForCopy parses an HTTP range string as an HTTPRangeSpec. It functions identically to
// parseRange with the exception of returning apierr.CodeInvalidCopySourceRange for range strings
// that are not of the form "bytes=first-last", where first and last are zero-based offsets.
func parseRangeForCopy(rangeStr string) (rangeSpec *cmd.HTTPRangeSpec, err error) {
	rangeSpec, err = ParseRange(rangeStr)
	if err != nil {
		return nil, err
	}
	if rangeSpec.IsSuffixLength || rangeSpec.Start < 0 || rangeSpec.End < 0 {
		return nil, apierr.CodeInvalidCopySourceRange
	}
	return rangeSpec, nil
}
