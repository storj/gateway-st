// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"time"

	"github.com/spacemonkeygo/monkit/v3"

	"storj.io/uplink"
)

var mon = monkit.Package()

// ClientConfig is a configuration struct for the uplink that controls how
// to talk to the rest of the network.
type ClientConfig struct {
	UserAgent           string        `help:"User-Agent used for connecting to the satellite" default:""`
	AdditionalUserAgent string        `help:"additional value appended to User-Agent" default:""`
	DialTimeout         time.Duration `help:"timeout for dials" default:"0h2m00s"`
}

// Config uplink configuration.
type Config struct {
	AccessConfig
	Client ClientConfig
}

// AccessConfig holds information about which accesses exist and are selected.
type AccessConfig struct {
	Accesses map[string]string `internal:"true"`
	Access   string            `help:"the serialized access, or name of the access to use" default:"" basic-help:"true"`
}

// GetAccess returns the appropriate access for the config.
func (a AccessConfig) GetAccess() (_ *uplink.Access, err error) {
	defer mon.Task()(nil)(&err)

	access, err := a.GetNamedAccess(a.Access)
	if err != nil {
		return nil, err
	}
	if access != nil {
		return access, nil
	}

	// Otherwise, try to load the access name as a serialized access.
	return uplink.ParseAccess(a.Access)
}

// GetNamedAccess returns named access if exists.
func (a AccessConfig) GetNamedAccess(name string) (_ *uplink.Access, err error) {
	defer mon.Task()(nil)(&err)

	// if an access exists for that name, try to load it.
	if data, ok := a.Accesses[name]; ok {
		return uplink.ParseAccess(data)
	}
	return nil, nil
}
