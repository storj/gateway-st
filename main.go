// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"context"
	"net"
	"os"

	"github.com/minio/cli"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs"
	"go.uber.org/zap"

	"storj.io/common/cfgstruct"
	"storj.io/common/fpath"
	"storj.io/common/process"
	"storj.io/common/process/eventkitbq"
	"storj.io/gateway/miniogw"
	minio "storj.io/minio/cmd"
)

// GatewayFlags configuration flags.
type GatewayFlags struct {
	Server miniogw.ServerConfig
	Minio  miniogw.MinioConfig
}

var (
	// Error is the default gateway setup errs class.
	Error = errs.Class("gateway setup")

	// ConfigError is a class of errors relating to config validation.
	ConfigError = errs.Class("gateway configuration")

	// rootCmd represents the base gateway command when called without any subcommands.
	rootCmd = &cobra.Command{
		Use:   "gateway",
		Short: "Simple, file-based, S3-compatible gateway",
		Args:  cobra.OnlyValidArgs,
	}
	runCmd = &cobra.Command{
		Use:   "run",
		Short: "Run the S3 gateway",
		RunE:  cmdRun,
	}

	runCfg GatewayFlags

	confDir string
)

func init() {
	defaultConfDir := fpath.ApplicationDir("storj", "gateway")
	cfgstruct.SetupFlag(zap.L(), rootCmd, &confDir, "config-dir", defaultConfDir, "main directory for gateway configuration")
	defaults := cfgstruct.DefaultsFlag(rootCmd)

	rootCmd.AddCommand(runCmd)
	process.Bind(runCmd, &runCfg, defaults, cfgstruct.ConfDir(confDir))
}

func cmdRun(cmd *cobra.Command, args []string) (err error) {
	address := runCfg.Server.Address
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	if host == "" {
		address = net.JoinHostPort("127.0.0.1", port)
	}

	ctx, _ := process.Ctx(cmd)

	if err := process.InitMetrics(ctx, zap.L(), nil, "", eventkitbq.BQDestination); err != nil {
		zap.S().Warn("Failed to initialize telemetry batcher: ", err)
	}

	zap.S().Infof("Starting Simple S3 Gateway\n\n")
	zap.S().Infof("Endpoint: %s\n", address)
	zap.S().Infof("Access key: %s\n", runCfg.Minio.AccessKey)
	zap.S().Infof("Secret key: %s\n", runCfg.Minio.SecretKey)

	return runCfg.Run(ctx)
}

// Run starts a Minio Gateway given proper config.
func (flags GatewayFlags) Run(ctx context.Context) (err error) {
	err = minio.RegisterGatewayCommand(cli.Command{
		Name:  "storj",
		Usage: "Storj",
		Action: func(cliCtx *cli.Context) error {
			return flags.action(cliCtx)
		},
		HideHelpCommand: true,
	})
	if err != nil {
		return err
	}

	// TODO(jt): Surely there is a better way. This is so upsetting
	err = os.Setenv("MINIO_ACCESS_KEY", flags.Minio.AccessKey)
	if err != nil {
		return err
	}
	err = os.Setenv("MINIO_SECRET_KEY", flags.Minio.SecretKey)
	if err != nil {
		return err
	}

	minio.Main([]string{"storj", "gateway", "storj",
		"--address", flags.Server.Address, "--config-dir", flags.Minio.Dir, "--quiet",
		"--compat"})
	return errs.New("unexpected minio exit")
}

func (flags GatewayFlags) action(cliCtx *cli.Context) (err error) {

	minio.StartGateway(cliCtx, miniogw.NewStorjGateway(runCfg.Minio.Dir))

	return errs.New("unexpected minio exit")
}

func main() {
	process.Exec(rootCmd)
}
