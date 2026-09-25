// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package miniogw

import (
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/base58"
	"storj.io/gateway/miniogw/internal/streamidpb"
	"storj.io/picobuf"
)

func TestParseUploadID(t *testing.T) {
	encode := func(msg picobuf.Message, extra ...byte) string {
		data, err := picobuf.Marshal(msg)
		require.NoError(t, err)
		return base58.CheckEncode(append(data, extra...), 1)
	}
	want := &streamidpb.StreamID{
		Bucket:             []byte("bucket"),
		EncryptedObjectKey: []byte("key"),
		StreamId:           []byte("stream-id"),
	}

	// A listed upload ID lacks the object lock settings of the one returned
	// when the upload began; field 17 (legal hold) = true.
	full, listed := encode(want, 0x88, 0x01, 1), encode(want)
	require.NotEqual(t, full, listed)
	for _, uploadID := range []string{full, listed} {
		id, ok := parseUploadID(uploadID)
		require.True(t, ok)
		require.Equal(t, want.Bucket, id.Bucket)
		require.Equal(t, want.EncryptedObjectKey, id.EncryptedObjectKey)
		require.Equal(t, want.StreamId, id.StreamId)
	}

	for _, uploadID := range []string{
		"", "garbage",
		base58.CheckEncode([]byte("x"), 1),
		base58.CheckEncode(nil, 2),
		encode(&streamidpb.StreamID{Bucket: []byte("bucket")}),
	} {
		_, ok := parseUploadID(uploadID)
		require.False(t, ok, uploadID)
	}

	// A repeated field decodes to its last value, as on the satellite.
	victim := encode(&streamidpb.StreamID{EncryptedObjectKey: []byte("victim")})
	data, _, err := base58.CheckDecode(victim)
	require.NoError(t, err)
	id, ok := parseUploadID(encode(want, data...))
	require.True(t, ok)
	require.Equal(t, []byte("victim"), id.EncryptedObjectKey)
}
