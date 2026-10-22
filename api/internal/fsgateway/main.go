// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

// Command fsgateway serves the api package over a local directory, for running S3 compatibility
// tests without a satellite.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/amwolff/awsig"
	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"storj.io/gateway/api"
	"storj.io/gateway/api/internal/fsobjects"
)

func main() {
	address := flag.String("address", "127.0.0.1:7777", "address to listen on")
	dir := flag.String("dir", "", "directory to store buckets in (required)")
	domains := flag.String("domains", "", "comma-separated domains for virtual-hosted-style requests")
	flag.Parse()

	// The credentials come from the environment, like the AWS CLI's.
	creds := credentials{}
	for _, prefix := range []string{"AWS", "ALT_AWS"} {
		if id := os.Getenv(prefix + "_ACCESS_KEY_ID"); id != "" {
			creds[id] = os.Getenv(prefix + "_SECRET_ACCESS_KEY")
		}
	}
	if *dir == "" || len(creds) == 0 {
		log.Fatal("-dir and AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY are required")
	}
	if err := os.MkdirAll(*dir, 0o700); err != nil {
		log.Fatal(err)
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal(err)
	}

	var config api.Config
	if *domains != "" {
		config.Domains = strings.Split(*domains, ",")
	}

	router := mux.NewRouter()
	api.New(fsobjects.New(*dir), creds, config, api.WithLogger(api.NewZapLogger(logger))).RegisterHandlers(router)

	logger.Info("serving", zap.String("address", *address), zap.String("dir", *dir))
	// TODO: set read timeouts. Without them, a request whose body is shorter than its
	// Content-Length waits for the client to give up, where S3 returns RequestTimeout (ceph's
	// test_object_create_bad_contentlength_mismatch_above).
	log.Fatal(http.ListenAndServe(*address, router))
}

// credentials maps access key IDs to secret access keys. Every key can access every bucket.
type credentials map[string]string

func (c credentials) Provide(_ context.Context, accessKeyID string) (string, api.AuthData, error) {
	secret, ok := c[accessKeyID]
	if !ok {
		return "", api.AuthData{}, awsig.ErrInvalidAccessKeyID
	}
	return secret, api.AuthData{AccessKeyID: accessKeyID}, nil
}
