// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package apierr

import "fmt"

//go:generate go run ../../cmd/apierrgen -i errors.yaml -o errors_gen.go -p apierr

// Code is an internal error identifier.
type Code int

// Error implements the error interface.
func (code Code) Error() string {
	if resp, ok := codeToResponse[code]; ok {
		return resp.Description
	}
	return fmt.Sprintf("error code %d", code)
}

// ToResponse returns the error response corresponding to the error code if it exists.
func (code Code) ToResponse() (resp Response, ok bool) {
	resp, ok = codeToResponse[code]
	return resp, ok
}

// Response is the public representation of an error.
type Response struct {
	Code           string
	Description    string
	HTTPStatusCode int
}

// Error implements the error interface.
func (resp Response) Error() string {
	return resp.Description
}
