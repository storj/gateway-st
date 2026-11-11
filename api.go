// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/amwolff/awsig"
	"github.com/gorilla/mux"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"storj.io/gateway/api"
	"storj.io/gateway/api/objectlayer"
	"storj.io/gateway/miniogw"
	"storj.io/minio/pkg/auth"
)

const shutdownTimeout = 5 * time.Second

// runAPI serves S3 requests with the api package over miniogw until ctx is canceled.
func (flags GatewayFlags) runAPI(ctx context.Context) (err error) {
	defer mon.Task()(&ctx)(&err)

	access, err := flags.GetAccess()
	if err != nil {
		return Error.Wrap(err)
	}
	project, err := flags.newUplinkConfig(ctx).OpenProject(ctx, access)
	if err != nil {
		return Error.Wrap(err)
	}
	defer func() { err = errs.Combine(err, project.Close()) }()

	layer, err := miniogw.NewStorjGateway(flags.S3).NewGatewayLayer(auth.Credentials{})
	if err != nil {
		return Error.Wrap(err)
	}
	defer func() { err = errs.Combine(err, layer.Shutdown(context.WithoutCancel(ctx))) }()
	// minio's ObjectLayer, which miniogw's layer is returned as, doesn't declare the api package's
	// additions to it, such as GetBucketLocation.
	objectLayer, ok := layer.(objectlayer.ObjectLayer)
	if !ok {
		return Error.New("miniogw's layer doesn't implement the api package's object layer")
	}

	retryAfter, err := slowDownRetryAfter()
	if err != nil {
		return err
	}
	allowedOrigins := corsAllowedOrigins()
	config := api.Config{
		CORS: &api.CORSConfiguration{Rules: []api.CORSRule{{
			AllowedHeaders: []string{"*"},
			AllowedMethods: corsAllowedMethods,
			AllowedOrigins: allowedOrigins,
			ExposeHeaders:  []string{"*"},
		}}},
		SlowDownRetryAfter: retryAfter,
	}
	// minio's handlers take the domains for virtual-hosted-style requests from MINIO_DOMAIN.
	if domains := os.Getenv("MINIO_DOMAIN"); domains != "" {
		config.Domains = strings.Split(domains, ",")
	}

	options := []api.Option{api.WithLogger(api.NewZapLogger(zap.L()))}
	if flags.Website {
		options = append(options, api.WithAnonymousReads())
	}

	router := mux.NewRouter()
	api.New(objectLayer, credentials{flags.Minio.AccessKey: flags.Minio.SecretKey}, config, options...).RegisterHandlers(router)

	info := miniogw.CredentialsInfo{Access: access}
	server := &http.Server{
		// miniogw finds the project in the request's context.
		Handler: corsHandler(allowedOrigins, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			router.ServeHTTP(w, r.WithContext(miniogw.WithCredentials(r.Context(), project, info)))
		})),
	}
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", flags.Server.Address)
	if err != nil {
		return Error.Wrap(err)
	}

	return serve(ctx, server, listener)
}

// serve serves listener with server until ctx is canceled, then shuts server down, closing the
// connections of requests still in flight after shutdownTimeout.
func serve(ctx context.Context, server *http.Server, listener net.Listener) error {
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		if err := server.Serve(listener); !errs.Is(err, http.ErrServerClosed) {
			return Error.Wrap(err)
		}
		return nil
	})
	group.Go(func() error {
		<-groupCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return Error.New("shutdown: %w", errs.Combine(err, server.Close()))
		}
		return nil
	})
	return group.Wait()
}

// slowDownRetryAfter returns the seconds in STORJ_MINIO_RETRY_AFTER, which minio's handlers also
// read, or 12 seconds, their default, if it isn't set.
func slowDownRetryAfter() (time.Duration, error) {
	value, ok := os.LookupEnv("STORJ_MINIO_RETRY_AFTER")
	if !ok {
		return 12 * time.Second, nil
	}
	seconds, err := strconv.Atoi(value)
	if err != nil {
		return 0, ConfigError.New("STORJ_MINIO_RETRY_AFTER must be whole seconds, not %q", value)
	}
	return time.Duration(seconds) * time.Second, nil
}

// credentials maps access key IDs to secret access keys.
type credentials map[string]string

func (c credentials) Provide(_ context.Context, accessKeyID string) (string, api.AuthData, error) {
	secret, ok := c[accessKeyID]
	if !ok {
		return "", api.AuthData{}, awsig.ErrInvalidAccessKeyID
	}
	return secret, api.AuthData{AccessKeyID: accessKeyID}, nil
}
