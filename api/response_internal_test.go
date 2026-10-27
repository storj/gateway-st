// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api

import (
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/gateway/api/objectlayer"
)

func TestSetResponseHeaderOverridesRepeated(t *testing.T) {
	w := httptest.NewRecorder()
	setResponseHeaderOverrides(w, url.Values{"response-content-type": {"a", "b"}})
	require.Equal(t, []string{"a"}, w.Header().Values("Content-Type"))
}

func TestObjectRetentionResponseRoundsUp(t *testing.T) {
	encode := func(date time.Time) string {
		resp, err := encodeResponse(newObjectRetentionResponse(&objectlayer.ObjectRetention{
			Mode:            objectlayer.RetentionGovernance,
			RetainUntilDate: objectlayer.RetentionDate{Time: date},
		}))
		require.NoError(t, err)
		return string(resp)
	}
	date := time.Date(2100, 1, 2, 3, 4, 5, 0, time.UTC)
	require.Contains(t, encode(date), "<Mode>GOVERNANCE</Mode><RetainUntilDate>2100-01-02T03:04:05.000Z</RetainUntilDate>")
	require.Contains(t, encode(date.Add(123*time.Millisecond)), "<RetainUntilDate>2100-01-02T03:04:05.123Z</RetainUntilDate>")
	require.Contains(t, encode(date.Add(123456*time.Microsecond)), "<RetainUntilDate>2100-01-02T03:04:05.124Z</RetainUntilDate>")
}
