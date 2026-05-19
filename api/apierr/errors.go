// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package apierr

import (
	"fmt"
	"net/http"
)

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

// ResponseProvider is the interface implemented by types that can represent themselves
// as API error responses.
type ResponseProvider interface {
	ToResponse() Response
}

// PostFormConditionFailedError indicates that a POST form did not satisfy a condition of a POST policy.
type PostFormConditionFailedError struct {
	Condition string
}

// Error implements the error interface.
func (err PostFormConditionFailedError) Error() string {
	return fmt.Sprintf("Invalid according to Policy: Policy Condition failed: %s", err.Condition)
}

// ToResponse implements the ResponseProvider interface.
func (err PostFormConditionFailedError) ToResponse() Response {
	return Response{
		Code:           "AccessDenied",
		Description:    err.Error(),
		HTTPStatusCode: http.StatusForbidden,
	}
}

// PostFormExtraFieldsError indicates that an input field was found in a POST form that was
// not included in a POST policy.
type PostFormExtraFieldsError struct {
	FieldName string
}

// Error implements the error interface.
func (err PostFormExtraFieldsError) Error() string {
	return "Invalid according to Policy: Extra input fields: " + err.FieldName
}

// ToResponse implements the ResponseProvider interface.
func (err PostFormExtraFieldsError) ToResponse() Response {
	return Response{
		Code:           "AccessDenied",
		Description:    err.Error(),
		HTTPStatusCode: http.StatusForbidden,
	}
}

// PostFormMissingFieldError indicates that a POST form was missing a field.
type PostFormMissingFieldError struct {
	FieldName string
}

// Error implements the error interface.
func (err PostFormMissingFieldError) Error() string {
	return "Bucket POST must contain a field named '" + err.FieldName + "'. If it is specified, please check the order of the fields."
}

// ToResponse implements the ResponseProvider interface.
func (err PostFormMissingFieldError) ToResponse() Response {
	return Response{
		Code:           "InvalidArgument",
		Description:    err.Error(),
		HTTPStatusCode: http.StatusBadRequest,
	}
}

// PostPolicyConditionInvalidArgumentCountError indicates that an incorrect number
// of arguments were specified in a condition of a POST policy.
type PostPolicyConditionInvalidArgumentCountError struct {
	OperationName string
}

// Error implements the error interface.
func (err PostPolicyConditionInvalidArgumentCountError) Error() string {
	return fmt.Sprintf("Invalid Policy: Invalid %s: wrong number of arguments.", err.OperationName)
}

// ToResponse implements the ResponseProvider interface.
func (err PostPolicyConditionInvalidArgumentCountError) ToResponse() Response {
	return Response{
		Code:           "InvalidPolicyDocument",
		Description:    err.Error(),
		HTTPStatusCode: http.StatusBadRequest,
	}
}

// PostPolicyConditionUnknownOperationError indicates that the condition of a POST policy
// contains an unknown operation.
type PostPolicyConditionUnknownOperationError struct {
	OperationName string
}

// Error implements the error interface.
func (err PostPolicyConditionUnknownOperationError) Error() string {
	return fmt.Sprintf("Invalid Policy: Invalid Condition: unknown operation %q.", err.OperationName)
}

// ToResponse implements the ResponseProvider interface.
func (err PostPolicyConditionUnknownOperationError) ToResponse() Response {
	return Response{
		Code:           "InvalidPolicyDocument",
		Description:    err.Error(),
		HTTPStatusCode: http.StatusBadRequest,
	}
}

// PostPolicyInvalidExpirationError indicates that the expiration specified in a POST policy was not a valid timestamp.
type PostPolicyInvalidExpirationError struct {
	Value string
}

// Error implements the error interface.
func (err PostPolicyInvalidExpirationError) Error() string {
	return fmt.Sprintf("Invalid Policy: Invalid 'expiration' value: %q", err.Value)
}

// ToResponse implements the ResponseProvider interface.
func (err PostPolicyInvalidExpirationError) ToResponse() Response {
	return Response{
		Code:           "InvalidPolicyDocument",
		Description:    err.Error(),
		HTTPStatusCode: http.StatusBadRequest,
	}
}

// PostPolicyUnexpectedElementError indicates that an unexpected top-level field was included in a POST policy.
type PostPolicyUnexpectedElementError struct {
	ElementName string
}

// Error implements the error interface.
func (err PostPolicyUnexpectedElementError) Error() string {
	return fmt.Sprintf("Invalid Policy: Unexpected: '%q'", err.ElementName)
}

// ToResponse implements the ResponseProvider interface.
func (err PostPolicyUnexpectedElementError) ToResponse() Response {
	return Response{
		Code:           "InvalidPolicyDocument",
		Description:    err.Error(),
		HTTPStatusCode: http.StatusBadRequest,
	}
}
