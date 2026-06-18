// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

/*
 * MinIO Cloud Storage, (C) 2015-2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package api_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/gateway/api"
	"storj.io/gateway/api/apierr"
	"storj.io/minio/cmd"
)

func TestParseRange(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		for _, tt := range []struct {
			rangeStr     string
			expRangeSpec cmd.HTTPRangeSpec
		}{
			{
				rangeStr: "bytes=3-5",
				expRangeSpec: cmd.HTTPRangeSpec{
					IsSuffixLength: false,
					Start:          3,
					End:            5,
				},
			},
			{
				rangeStr: "bytes=3-",
				expRangeSpec: cmd.HTTPRangeSpec{
					IsSuffixLength: false,
					Start:          3,
					End:            -1,
				},
			},
			{
				rangeStr: "bytes=-5",
				expRangeSpec: cmd.HTTPRangeSpec{
					IsSuffixLength: true,
					Start:          -5,
					End:            -1,
				},
			},
		} {
			t.Run(tt.rangeStr, func(t *testing.T) {
				rangeSpec, err := api.ParseRange(tt.rangeStr)
				require.NoError(t, err)
				require.Equal(t, tt.expRangeSpec, *rangeSpec)
			})
		}
	})

	t.Run("Invalid", func(t *testing.T) {
		for _, rangeStr := range []string{
			"bytes=5-3",
			"bytes=-0",
		} {
			t.Run(rangeStr, func(t *testing.T) {
				rs, err := api.ParseRange(rangeStr)
				require.ErrorIs(t, err, apierr.CodeInvalidCopySourceRange)
				require.Nil(t, rs)
			})
		}
	})

	t.Run("Malformed", func(t *testing.T) {
		for _, rangeStr := range []string{
			"bytes=-",
			"bytes==",
			"bytes==1-10",
			"bytes=",
			"bytes=aa",
			"aa",
			"",
			"bytes=1-10-",
			"bytes=1--10",
			"bytes=-1-10",
			"bytes=0-+3",
			"bytes=+3-+5",
			"bytes=10-11,12-10",
		} {
			t.Run(rangeStr, func(t *testing.T) {
				rs, err := api.ParseRange(rangeStr)
				require.ErrorIs(t, err, apierr.CodeMalformedCopySourceRange)
				require.Nil(t, rs)
			})
		}
	})
}
