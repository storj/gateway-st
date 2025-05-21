// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"context"

	"storj.io/uplink"
)

type credentialsKey struct{}

// Credentials contains credentials used to access the project.
type Credentials struct {
	Access          *uplink.Access
	Project         *uplink.Project
	PublicProjectID string
}

// CredentialsInfo is supplementary information about credentials used
// to access the project.
type CredentialsInfo struct {
	Access          *uplink.Access
	PublicProjectID string
}

// WithCredentials injects Credentials into ctx under a specific key.
// Use GetCredentials to retrieve Credentials from ctx.
func WithCredentials(ctx context.Context, project *uplink.Project, info CredentialsInfo) context.Context {
	return context.WithValue(ctx, credentialsKey{}, Credentials{
		Access:          info.Access,
		Project:         project,
		PublicProjectID: info.PublicProjectID,
	})
}

// GetCredentials retrieves Credentials from ctx and reports whether it
// was successful.
func GetCredentials(ctx context.Context) (Credentials, bool) {
	c, ok := ctx.Value(credentialsKey{}).(Credentials)
	return c, ok
}
