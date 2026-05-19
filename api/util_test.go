// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/amwolff/awsig"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"

	"storj.io/gateway/api"
	"storj.io/gateway/api/apierr"
)

func TestGetObjectURL(t *testing.T) {
	for _, tt := range []struct {
		name           string
		host           string
		object         string
		bucket         string
		forwardedProto string
		tls            bool
		vHost          bool
		expectedURL    string
	}{
		{
			name:        "Path-style URL with http",
			host:        "localhost:9000",
			bucket:      "my-bucket",
			object:      "my-object",
			expectedURL: "http://localhost:9000/my-bucket/my-object",
		},
		{
			name:        "Path-style URL with TLS",
			host:        "localhost:9000",
			bucket:      "my-bucket",
			object:      "my-object",
			tls:         true,
			expectedURL: "https://localhost:9000/my-bucket/my-object",
		},
		{
			name:           "X-Forwarded-Proto=https takes precedence over http",
			host:           "localhost:9000",
			bucket:         "my-bucket",
			object:         "my-object",
			forwardedProto: "https",
			expectedURL:    "https://localhost:9000/my-bucket/my-object",
		},
		{
			name:           "X-Forwarded-Proto=http takes precedence over TLS",
			host:           "localhost:9000",
			bucket:         "my-bucket",
			object:         "my-object",
			forwardedProto: "http",
			tls:            true,
			expectedURL:    "http://localhost:9000/my-bucket/my-object",
		},
		{
			name:        "Virtual-hosted-style URL omits bucket from path",
			host:        "my-bucket.localhost:9000",
			bucket:      "my-bucket",
			object:      "my-object",
			vHost:       true,
			expectedURL: "http://my-bucket.localhost:9000/my-object",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.vHost {
				ctx = api.WithVirtualHostedStyle(ctx)
			}

			req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
			req.Host = tt.host

			if tt.forwardedProto != "" {
				req.Header.Set("X-Forwarded-Proto", tt.forwardedProto)
			}
			if tt.tls {
				req.TLS = &tls.ConnectionState{}
			}

			req = mux.SetURLVars(req, map[string]string{"bucket": tt.bucket})

			require.Equal(t, tt.expectedURL, api.GetObjectURL(req, tt.object))
		})
	}
}

func TestLimitedAwsigReader(t *testing.T) {
	contents := []byte("Hello world!")
	underlyingErr := errors.New("underlying reader error")

	t.Run("Limit zero with non-empty underlying reader", func(t *testing.T) {
		src := newMockAwsigReader(contents)
		dest := bytes.NewBuffer(nil)

		reader := api.NewLimitedAwsigReader(src, 0)
		n, err := io.Copy(dest, reader)
		require.EqualValues(t, 0, n)
		require.ErrorIs(t, err, apierr.CodeEntityTooLarge)
		require.Empty(t, dest.Bytes())
	})

	t.Run("Limit zero with empty underlying reader", func(t *testing.T) {
		src := newMockAwsigReader([]byte{})
		dest := bytes.NewBuffer(nil)

		reader := api.NewLimitedAwsigReader(src, 0)
		n, err := io.Copy(dest, reader)
		require.EqualValues(t, 0, n)
		require.NoError(t, err)
		require.Empty(t, dest.Bytes())
	})

	t.Run("Limit greater than size of underlying reader", func(t *testing.T) {
		src := newMockAwsigReader(contents)
		dest := bytes.NewBuffer(nil)

		reader := api.NewLimitedAwsigReader(src, 50)
		n, err := io.Copy(dest, reader)
		require.EqualValues(t, len(contents), n)
		require.NoError(t, err)
		require.Equal(t, contents, dest.Bytes())
	})

	t.Run("Limit equal to size of underlying reader", func(t *testing.T) {
		src := newMockAwsigReader(contents)
		dest := bytes.NewBuffer(nil)

		reader := api.NewLimitedAwsigReader(src, int64(len(contents)))
		n, err := io.Copy(dest, reader)
		require.EqualValues(t, len(contents), n)
		require.NoError(t, err)
		require.Equal(t, contents, dest.Bytes())
	})

	t.Run("Limit less than size of underlying reader", func(t *testing.T) {
		src := newMockAwsigReader(contents)
		dest := bytes.NewBuffer(nil)

		reader := api.NewLimitedAwsigReader(src, 5)
		n, err := io.Copy(dest, reader)
		require.EqualValues(t, 5, n)
		require.ErrorIs(t, err, apierr.CodeEntityTooLarge)
		require.Equal(t, contents[:5], dest.Bytes())
	})

	t.Run("Underlying reader is not used again after it returns an error", func(t *testing.T) {
		src := newErrorAwsigReader(contents, underlyingErr)
		reader := api.NewLimitedAwsigReader(src, 50)

		buf := make([]byte, len(contents)+1)
		n, err := reader.Read(buf)
		require.EqualValues(t, len(contents), n)
		require.ErrorIs(t, err, underlyingErr)
		// Confirm that the LimitedAwsigReader didn't try probing the underlying reader
		// after the error was returned.
		require.False(t, src.readAfterError)

		n, err = reader.Read(buf)
		require.Zero(t, n)
		require.ErrorIs(t, err, underlyingErr)
		require.False(t, src.readAfterError)
	})

	t.Run("Probe returning (0, nil) does not trigger EntityTooLarge", func(t *testing.T) {
		src := newScriptedAwsigReader([]readResult{
			{data: contents[:5]},
			{data: nil}, // produces a (0, nil) probe result
		})

		reader := api.NewLimitedAwsigReader(src, 5)
		buf := make([]byte, 5)
		n, err := reader.Read(buf)
		require.EqualValues(t, 5, n)
		require.NoError(t, err)
		require.Equal(t, contents[:5], buf[:n])
	})

	t.Run("Probe returning (0, err) does not trigger EntityTooLarge", func(t *testing.T) {
		src := newScriptedAwsigReader([]readResult{
			{data: contents[:5]},
			{err: underlyingErr}, // produces a (0, err) probe result
		})

		dest := bytes.NewBuffer(nil)
		reader := api.NewLimitedAwsigReader(src, 5)
		n, err := io.Copy(dest, reader)
		require.EqualValues(t, 5, n)
		require.ErrorIs(t, err, underlyingErr)
	})

	t.Run("Probe returning (n, err) does not trigger EntityTooLarge", func(t *testing.T) {
		src := newScriptedAwsigReader([]readResult{
			{data: contents[:5]},
			{data: contents[5:6], err: underlyingErr}, // produces a (1, err) probe result
		})

		dest := bytes.NewBuffer(nil)
		reader := api.NewLimitedAwsigReader(src, 5)
		n, err := io.Copy(dest, reader)
		require.EqualValues(t, 5, n)
		require.ErrorIs(t, err, underlyingErr)
	})

	t.Run("Checksum propagation", func(t *testing.T) {
		src := newMockAwsigReader(contents)
		reader := api.NewLimitedAwsigReader(src, 50)

		srcChecksums, err := src.Checksums()
		require.NoError(t, err)
		readerChecksums, err := reader.Checksums()
		require.NoError(t, err)
		require.Equal(t, srcChecksums, readerChecksums)
	})
}

type mockAwsigReader struct {
	buf *bytes.Buffer
}

func newMockAwsigReader(contents []byte) awsig.Reader {
	return &mockAwsigReader{
		buf: bytes.NewBuffer(contents),
	}
}

func (reader *mockAwsigReader) Read(p []byte) (n int, err error) {
	return reader.buf.Read(p)
}

func (reader *mockAwsigReader) Checksums() (map[awsig.ChecksumAlgorithm][]byte, error) {
	return map[awsig.ChecksumAlgorithm][]byte{
		awsig.AlgorithmMD5: []byte("checksum"),
	}, nil
}

// errorAwsigReader is an awsig.Reader implementation that returns an error after all data has been read.
type errorAwsigReader struct {
	buf            *bytes.Buffer
	err            error
	done           bool
	readAfterError bool
}

func newErrorAwsigReader(contents []byte, err error) *errorAwsigReader {
	return &errorAwsigReader{
		buf: bytes.NewBuffer(contents),
		err: err,
	}
}

func (reader *errorAwsigReader) Read(p []byte) (int, error) {
	if reader.done {
		reader.readAfterError = true
		return 0, reader.err
	}
	n, _ := reader.buf.Read(p)
	if reader.buf.Len() == 0 {
		reader.done = true
		return n, reader.err
	}
	return n, nil
}

func (reader *errorAwsigReader) Checksums() (map[awsig.ChecksumAlgorithm][]byte, error) {
	return nil, nil
}

type readResult struct {
	data []byte
	err  error
}

// scriptedAwsigReader is an awsig.Reader implementation that allows for specifying the results
// returned from each call to Read.
type scriptedAwsigReader struct {
	reads     []readResult
	readIndex int
}

func newScriptedAwsigReader(readResults []readResult) *scriptedAwsigReader {
	return &scriptedAwsigReader{reads: readResults}
}

func (s *scriptedAwsigReader) Read(p []byte) (n int, err error) {
	if s.readIndex >= len(s.reads) {
		return 0, io.EOF
	}
	r := s.reads[s.readIndex]
	s.readIndex++
	if r.data != nil {
		n = copy(p, r.data)
	}
	return n, r.err
}

func (s *scriptedAwsigReader) Checksums() (map[awsig.ChecksumAlgorithm][]byte, error) {
	return nil, nil
}
