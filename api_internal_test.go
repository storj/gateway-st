// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/common/testcontext"
)

func TestServeShutdown(t *testing.T) {
	t.Run("idle", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := testcontext.New(t)
			serveCtx, cancel := context.WithCancel(ctx)
			ctx.Go(func() error {
				return serve(serveCtx, &http.Server{Handler: http.NotFoundHandler()}, newPipeListener())
			})

			start := time.Now()
			cancel()
			ctx.Wait()
			require.Less(t, time.Since(start), shutdownTimeout)
		})
	})

	t.Run("request in flight", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			listener := newPipeListener()
			// The request never finishes by itself, like a stuck download.
			server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				<-r.Context().Done()
			})}
			ctx := testcontext.New(t)
			serveCtx, cancel := context.WithCancel(ctx)
			var serveErr error
			var returned atomic.Bool
			ctx.Go(func() error {
				serveErr = serve(serveCtx, server, listener)
				returned.Store(true)
				return nil
			})

			conn := listener.dial()
			defer func() { _ = conn.Close() }()
			ctx.Go(func() error {
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://pipe/", nil)
				if err != nil {
					return err
				}
				return request.Write(conn)
			})
			synctest.Wait()

			cancel()
			time.Sleep(shutdownTimeout - time.Nanosecond)
			synctest.Wait()
			require.False(t, returned.Load(), "serve returned before the shutdown timeout")

			time.Sleep(time.Nanosecond)
			synctest.Wait()
			returnedInTime := returned.Load()
			if !returnedInTime {
				// Without a timeout, Shutdown waits for the request forever.
				require.NoError(t, server.Close())
			}
			ctx.Wait()
			require.True(t, returnedInTime, "serve didn't return after the shutdown timeout")
			require.Error(t, serveErr)
		})
	})
}

// pipeListener is a net.Listener of net.Pipe connections, which synctest knows are blocked.
type pipeListener struct {
	conns     chan net.Conn
	closed    chan struct{}
	closeOnce sync.Once
}

func newPipeListener() *pipeListener {
	return &pipeListener{conns: make(chan net.Conn), closed: make(chan struct{})}
}

// dial returns the client end of a new connection to the listener.
func (l *pipeListener) dial() net.Conn {
	client, server := net.Pipe()
	l.conns <- server
	return client
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (l *pipeListener) Addr() net.Addr { return pipeAddr{} }

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }

func TestSlowDownRetryAfter(t *testing.T) {
	// t.Setenv restores the variable after the test, which os.Unsetenv doesn't.
	t.Setenv("STORJ_MINIO_RETRY_AFTER", "")
	require.NoError(t, os.Unsetenv("STORJ_MINIO_RETRY_AFTER"))
	retryAfter, err := slowDownRetryAfter()
	require.NoError(t, err)
	require.Equal(t, 12*time.Second, retryAfter)

	t.Setenv("STORJ_MINIO_RETRY_AFTER", "30")
	retryAfter, err = slowDownRetryAfter()
	require.NoError(t, err)
	require.Equal(t, 30*time.Second, retryAfter)

	for _, value := range []string{"", "1.5", "Wed, 21 Oct 2015 07:28:00 GMT"} {
		t.Setenv("STORJ_MINIO_RETRY_AFTER", value)
		_, err = slowDownRetryAfter()
		require.Error(t, err, value)
	}
}

func TestCORSAllowedOrigins(t *testing.T) {
	t.Setenv("MINIO_API_CORS_ALLOW_ORIGIN", "")
	require.NoError(t, os.Unsetenv("MINIO_API_CORS_ALLOW_ORIGIN"))
	require.Equal(t, []string{"*"}, corsAllowedOrigins())

	t.Setenv("MINIO_API_CORS_ALLOW_ORIGIN", "https://a.example,https://*.b.example")
	require.Equal(t, []string{"https://a.example", "https://*.b.example"}, corsAllowedOrigins())
}
