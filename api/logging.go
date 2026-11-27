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
	// Error logs a failure of the server.
	Error(r *http.Request, msg string, errs ...error)
	// Debug logs an event that isn't a failure of the server but helps debugging, such as a
	// client going away.
	Debug(r *http.Request, msg string, errs ...error)
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
	log.log.Error(msg, log.fields(r, errs)...)
}

// Debug logs an event with its errors that occurred during the processing of an HTTP request.
func (log *zapLogger) Debug(r *http.Request, msg string, errs ...error) {
	log.log.Debug(msg, log.fields(r, errs)...)
}

func (log *zapLogger) fields(r *http.Request, errs []error) []zap.Field {
	fields := log.getCommonFields(r)
	switch len(errs) {
	case 0:
	case 1:
		fields = append(fields, zap.Error(errs[0]))
	default:
		fields = append(fields, zap.Errors("errors", errs))
	}
	return fields
}

func (log *zapLogger) getCommonFields(r *http.Request) []zap.Field {
	reqInfo := logger.GetReqInfo(r.Context())
	if reqInfo == nil {
		return nil
	}
	fields := []zap.Field{zap.String("amz_request_id", reqInfo.RequestID)}
	if reqInfo.API != "" {
		fields = append(fields, zap.String("operation", reqInfo.API))
	}
	return fields
}

type nopLogger struct{}

// Error is a no-op and implements the Logger interface.
func (log nopLogger) Error(r *http.Request, msg string, errs ...error) {}

// Debug is a no-op and implements the Logger interface.
func (log nopLogger) Debug(r *http.Request, msg string, errs ...error) {}
