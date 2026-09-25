// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api

import (
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetResponseHeaderOverridesRepeated(t *testing.T) {
	w := httptest.NewRecorder()
	setResponseHeaderOverrides(w, url.Values{"response-content-type": {"a", "b"}})
	require.Equal(t, []string{"a"}, w.Header().Values("Content-Type"))
}
