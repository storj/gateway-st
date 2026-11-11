// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"

	"storj.io/gateway/api"
	"storj.io/gateway/api/internal/fsobjects"
)

func TestAnonymousReads(t *testing.T) {
	objectAPI := fsobjects.New(t.TempDir())
	require.Equal(t, http.StatusOK, serve(t, objectAPI, http.MethodPut, "/bucket", nil, nil).StatusCode)
	require.Equal(t, http.StatusOK, serve(t, objectAPI, http.MethodPut, "/bucket/key", nil, []byte("data")).StatusCode)

	anonymous := func(t *testing.T, router *mux.Router, method, target string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), method, target, nil))
		return rec
	}

	t.Run("allowed", func(t *testing.T) {
		router := mux.NewRouter()
		api.New(objectAPI, testCredentialsProvider{}, api.Config{}, api.WithAnonymousReads()).RegisterHandlers(router)

		for _, tc := range []struct{ method, target string }{
			{http.MethodGet, "/bucket/key"},
			{http.MethodHead, "/bucket/key"},
			{http.MethodGet, "/bucket"},
			{http.MethodGet, "/bucket?list-type=2"},
			{http.MethodHead, "/bucket"},
			{http.MethodGet, "/bucket?location"},
		} {
			require.Equal(t, http.StatusOK, anonymous(t, router, tc.method, tc.target).Code, "%s %s", tc.method, tc.target)
		}
		require.Equal(t, "data", anonymous(t, router, http.MethodGet, "/bucket/key").Body.String())

		// Other operations are still verified.
		for _, tc := range []struct{ method, target string }{
			{http.MethodPut, "/bucket/other"},
			{http.MethodDelete, "/bucket/key"},
			{http.MethodGet, "/bucket?versions"},
			{http.MethodGet, "/bucket/key?tagging"},
			{http.MethodGet, "/"},
		} {
			require.Equal(t, http.StatusForbidden, anonymous(t, router, tc.method, tc.target).Code, "%s %s", tc.method, tc.target)
		}

		// So are requests with a signature, even an incomplete or empty one.
		for _, target := range []string{
			"/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256",
			"/bucket/key?AWSAccessKeyId=access&Signature=bad&Expires=9999999999",
		} {
			require.NotEqual(t, http.StatusOK, anonymous(t, router, http.MethodGet, target).Code, target)
		}
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/bucket/key", nil)
		req.Header.Set("Authorization", "")
		router.ServeHTTP(rec, req)
		require.NotEqual(t, http.StatusOK, rec.Code)
	})

	t.Run("response header overrides like S3", func(t *testing.T) {
		public := mux.NewRouter()
		api.New(objectAPI, testCredentialsProvider{}, api.Config{}, api.WithAnonymousReads()).RegisterHandlers(public)
		private := newTestRouter(objectAPI)

		// S3 refuses the overrides in unsigned requests before checking whether the key exists or
		// anyone may read it, for GetObject and HeadObject, also with an empty value.
		for _, router := range []*mux.Router{public, private} {
			for _, tc := range []struct{ method, target string }{
				{http.MethodGet, "/bucket/key?response-content-type=text%2Fhtml"},
				{http.MethodGet, "/bucket/key?response-content-language=fr"},
				{http.MethodGet, "/bucket/key?response-expires=Thu%2C%2001%20Jan%202099%2000%3A00%3A00%20GMT"},
				{http.MethodGet, "/bucket/key?response-cache-control=no-store"},
				{http.MethodGet, "/bucket/key?response-content-disposition=inline"},
				{http.MethodGet, "/bucket/key?response-content-encoding=identity"},
				{http.MethodGet, "/bucket/key?response-content-type="},
				{http.MethodGet, "/bucket/missing?response-content-type=text%2Fhtml"},
				{http.MethodHead, "/bucket/key?response-content-type=text%2Fhtml"},
			} {
				rec := anonymous(t, router, tc.method, tc.target)
				require.Equal(t, http.StatusBadRequest, rec.Code, "%s %s", tc.method, tc.target)
				if tc.method == http.MethodGet {
					require.Contains(t, rec.Body.String(), "<Code>InvalidRequest</Code><Message>Request specific response headers cannot be used for anonymous GET requests.</Message>")
				}
			}
		}

		// The parameter names are case-sensitive, and listings ignore them.
		rec := anonymous(t, public, http.MethodGet, "/bucket/key?Response-Content-Type=text%2Fhtml")
		require.Equal(t, http.StatusOK, rec.Code)
		require.NotEqual(t, "text/html", rec.Header().Get("Content-Type"))
		require.Equal(t, http.StatusForbidden, anonymous(t, private, http.MethodGet, "/bucket/key?Response-Content-Type=text%2Fhtml").Code)
		require.Equal(t, http.StatusOK, anonymous(t, public, http.MethodGet, "/bucket?response-content-type=text%2Fhtml").Code)
		require.Equal(t, http.StatusForbidden, anonymous(t, private, http.MethodGet, "/bucket?list-type=2&response-content-type=text%2Fhtml").Code)

		// Signed requests may override.
		resp := serve(t, objectAPI, http.MethodGet, "/bucket/key?response-content-type=text%2Fhtml", nil, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, "text/html", resp.Header.Get("Content-Type"))
	})

	t.Run("not allowed", func(t *testing.T) {
		for _, target := range []string{"/bucket/key", "/bucket", "/bucket?location"} {
			require.Equal(t, http.StatusForbidden, anonymous(t, newTestRouter(objectAPI), http.MethodGet, target).Code, target)
		}
	})
}
