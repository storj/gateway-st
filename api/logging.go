// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api

import (
	"net/http"

	"go.uber.org/zap"

	"storj.io/minio/cmd/logger"
)

// WithLogger returns an option for constructing an API with a logger.
func WithLogger(log Logger) Option {
	return func(api *API) {
		api.log = log
	}
}

// Logger logs events that occur during HTTP request handling.
type Logger interface {
	Error(r *http.Request, msg string, errs ...error)
}

type zapLogger struct {
	log *zap.Logger
}

// NewZapLogger returns a new Logger that logs on the provided zap.Logger.
func NewZapLogger(log *zap.Logger) Logger {
	return &zapLogger{log: log}
}

// Error logs one or more errors that occurred during the processing of an HTTP request.
func (log *zapLogger) Error(r *http.Request, msg string, errs ...error) {
	fields := log.getCommonFields(r)
	switch len(errs) {
	case 0:
	case 1:
		fields = append(fields, zap.Error(errs[0]))
	default:
		fields = append(fields, zap.Errors("errors", errs))
	}
	log.log.Error(msg, fields...)
}

func (log *zapLogger) getCommonFields(r *http.Request) []zap.Field {
	reqInfo := logger.GetReqInfo(r.Context())
	if reqInfo == nil {
		return nil
	}
	fields := []zap.Field{zap.String("amz_request_id", reqInfo.RequestID)}
	// TODO: the operation is never set yet, because handlers keep the context from
	// cmd.NewContext, which names it, to themselves. Pass it on with the request.
	if reqInfo.API != "" {
		fields = append(fields, zap.String("operation", reqInfo.API))
	}
	return fields
}

type nopLogger struct{}

// Error is a no-op and implements the Logger interface.
func (log nopLogger) Error(r *http.Request, msg string, errs ...error) {}
