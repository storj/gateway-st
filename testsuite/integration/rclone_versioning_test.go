// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package integration_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
)

// tests based on rclone doc https://rclone.org/s3/#versioning

func TestRcloneVersioning(t *testing.T) {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" || os.Getenv("AWS_ENDPOINT") == "" {
		t.Skip("AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY or AWS_ENDPOINT are not set")
	}

	ctx := testcontext.New(t)

	cmd := exec.Command("go", "install", "github.com/rclone/rclone@latest")
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "GOBIN="+ctx.Dir("binary"))
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Log(string(output))
	}
	require.NoError(t, err)

	runRclone := func(args ...string) string {
		output, err := exec.Command(ctx.File("binary", "rclone"), args...).CombinedOutput()
		if err != nil {
			t.Log(string(output))
		}
		require.NoError(t, err)
		return string(output)
	}

	runRclone("config", "create", "TestS3", "s3", "env_auth", "true", "provider", "Minio",
		"endpoint", os.Getenv("AWS_ENDPOINT"), "chunk_size", "64M", "upload_cutoff", "64M")

	bucketName := testrand.BucketName()

	input := createFile(ctx, t, "versioned", testrand.Bytes(10*memory.KiB))
	runRclone("copy", input, "TestS3:"+bucketName+"/")

	// enable versioning
	cmdOutput := runRclone("backend", "versioning", "TestS3:"+bucketName, "Enabled")
	require.Equal(t, "Enabled\n", cmdOutput)

	// create two more versions
	input = createFile(ctx, t, "versioned", testrand.Bytes(9*memory.KiB))
	runRclone("copy", input, "TestS3:"+bucketName+"/")

	input = createFile(ctx, t, "versioned", testrand.Bytes(8*memory.KiB))
	runRclone("copy", input, "TestS3:"+bucketName+"/")

	// delete latest version to create delete marker
	runRclone("delete", "TestS3:"+bucketName+"/versioned")

	cmdOutput = runRclone("-q", "--s3-versions", "ls", "TestS3:"+bucketName)

	require.Regexp(t, trim(`
		8192 versioned-v....-..-..-......-...
		9216 versioned-v....-..-..-......-...
		10240 versioned-v....-..-..-......-...
	`), trim(cmdOutput))

	runRclone("purge", "TestS3:"+bucketName)
}

func trim(content string) string {
	lines := strings.Split(content, "\n")
	result := []string{}
	for i := range lines {
		line := strings.TrimSpace(lines[i])
		if line != "" {
			result = append(result, line)
		}
	}
	return strings.Join(result, "\n")
}

func createFile(ctx *testcontext.Context, t *testing.T, name string, content []byte) string {
	path := ctx.File(name)

	err := os.WriteFile(path, content, 0644)
	require.NoError(t, err)

	return path
}
