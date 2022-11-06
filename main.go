// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/zeebo/errs"
	"go.uber.org/zap"

	"storj.io/common/base58"
	"storj.io/common/fpath"
	"storj.io/gateway/internal/wizard"
	"storj.io/gateway/miniogw"
	minio "storj.io/minio/cmd"
	"storj.io/private/cfgstruct"
	"storj.io/private/process"
	"storj.io/private/version"
	"storj.io/uplink"
)

// GatewayFlags configuration flags.
type GatewayFlags struct {
	NonInteractive   bool   `help:"disable interactive mode" default:"false" setup:"true"`
	SatelliteAddress string `help:"satellite address (<nodeid>@<address>:<port>)" default:"" setup:"true"`
	APIKey           string `help:"API key" default:"" setup:"true"`
	Passphrase       string `help:"encryption passphrase" default:"" setup:"true"`

	Server miniogw.ServerConfig
	Minio  miniogw.MinioConfig
	S3     miniogw.S3CompatibilityConfig

	Config

	Website bool `help:"serve content as a static website" default:"false" basic-help:"true"`
}

var (
	gatewayUserAgent = "Gateway-ST/" + version.Build.Version.String()

	// Error is the default gateway setup errs class.
	Error = errs.Class("gateway setup")

	// ConfigError is a class of errors relating to config validation.
	ConfigError = errs.Class("gateway configuration")

	// rootCmd represents the base gateway command when called without any subcommands.
	rootCmd = &cobra.Command{
		Use:   "gateway",
		Short: "Single-tenant, S3-compatible gateway to Storj DCS",
		Args:  cobra.OnlyValidArgs,
	}
	setupCmd = &cobra.Command{
		Use:         "setup",
		Short:       "Create a gateway config file",
		RunE:        cmdSetup,
		Annotations: map[string]string{"type": "setup"},
	}
	runCmd = &cobra.Command{
		Use:   "run",
		Short: "Run the S3 gateway",
		RunE:  cmdRun,
	}

	setupCfg GatewayFlags
	runCfg   GatewayFlags

	confDir string
)

func init() {
	defaultConfDir := fpath.ApplicationDir("storj", "gateway")
	cfgstruct.SetupFlag(zap.L(), rootCmd, &confDir, "config-dir", defaultConfDir, "main directory for gateway configuration")
	defaults := cfgstruct.DefaultsFlag(rootCmd)

	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(setupCmd)
	process.Bind(runCmd, &runCfg, defaults, cfgstruct.ConfDir(confDir))
	process.Bind(setupCmd, &setupCfg, defaults, cfgstruct.ConfDir(confDir), cfgstruct.SetupMode())

	rootCmd.PersistentFlags().BoolVar(new(bool), "advanced", false, "if used in with -h, print advanced flags help")
	cfgstruct.SetBoolAnnotation(rootCmd.PersistentFlags(), "advanced", cfgstruct.BasicHelpAnnotationName, true)
	cfgstruct.SetBoolAnnotation(rootCmd.PersistentFlags(), "config-dir", cfgstruct.BasicHelpAnnotationName, true)
	setUsageFunc(rootCmd)
}

func cmdSetup(cmd *cobra.Command, args []string) (err error) {
	setupDir, err := filepath.Abs(confDir)
	if err != nil {
		return Error.Wrap(err)
	}

	valid, _ := fpath.IsValidSetupDir(setupDir)
	if !valid {
		return Error.New("gateway configuration already exists (%v)", setupDir)
	}

	err = os.MkdirAll(setupDir, 0700)
	if err != nil {
		return Error.Wrap(err)
	}

	overrides := map[string]interface{}{}

	accessKeyFlag := cmd.Flag("minio.access-key")
	if !accessKeyFlag.Changed {
		accessKey, err := generateKey()
		if err != nil {
			return err
		}
		overrides[accessKeyFlag.Name] = accessKey
	}

	secretKeyFlag := cmd.Flag("minio.secret-key")
	if !secretKeyFlag.Changed {
		secretKey, err := generateKey()
		if err != nil {
			return err
		}
		overrides[secretKeyFlag.Name] = secretKey
	}

	if setupCfg.NonInteractive {
		return setupCfg.nonInteractive(cmd, setupDir, overrides)
	}
	return setupCfg.interactive(cmd, setupDir, overrides)
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

	if err := process.InitMetrics(ctx, zap.L(), nil, ""); err != nil {
		zap.S().Warn("Failed to initialize telemetry batcher: ", err)
	}

	zap.S().Infof("Starting Storj DCS S3 Gateway\n\n")
	zap.S().Infof("Endpoint: %s\n", address)
	zap.S().Infof("Access key: %s\n", runCfg.Minio.AccessKey)
	zap.S().Infof("Secret key: %s\n", runCfg.Minio.SecretKey)

	err = checkCfg(ctx)
	if err != nil {
		return err
	}

	return runCfg.Run(ctx)
}

func generateKey() (key string, err error) {
	var buf [20]byte
	_, err = rand.Read(buf[:])
	if err != nil {
		return "", Error.Wrap(err)
	}
	return base58.Encode(buf[:]), nil
}

func checkCfg(ctx context.Context) (err error) {
	access, err := runCfg.GetAccess()
	if err != nil {
		return ConfigError.New("failed parsing access config: %w", err)
	}

	config := runCfg.newUplinkConfig(ctx)

	project, err := config.OpenProject(ctx, access)
	if err != nil {
		return ConfigError.New("failed to open project: %w", err)
	}
	defer func() { err = errs.Combine(err, project.Close()) }()

	buckets := project.ListBuckets(ctx, nil)
	_ = buckets.Next()
	if buckets.Err() != nil {
		return ConfigError.New("failed to contact Satellite: %w", buckets.Err())
	}
	return nil
}

// Run starts a Minio Gateway given proper config.
func (flags GatewayFlags) Run(ctx context.Context) (err error) {
	err = minio.RegisterGatewayCommand(cli.Command{
		Name:  "storj",
		Usage: "Storj",
		Action: func(cliCtx *cli.Context) error {
			return flags.action(ctx, cliCtx)
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

func (flags GatewayFlags) action(ctx context.Context, cliCtx *cli.Context) (err error) {
	access, err := flags.GetAccess()
	if err != nil {
		return Error.Wrap(err)
	}

	config := flags.newUplinkConfig(ctx)

	gw, err := flags.NewGateway(ctx)
	if err != nil {
		return err
	}

	minio.StartGateway(cliCtx, miniogw.NewSingleTenantGateway(zap.L(), access, config, gw, flags.Website))

	return errs.New("unexpected minio exit")
}

// NewGateway creates a new minio Gateway.
func (flags GatewayFlags) NewGateway(ctx context.Context) (gw minio.Gateway, err error) {
	return miniogw.NewStorjGateway(flags.S3), nil
}

func (flags *GatewayFlags) newUplinkConfig(ctx context.Context) uplink.Config {
	// Transform the gateway config flags to the uplink config object
	config := uplink.Config{}
	config.DialTimeout = flags.Client.DialTimeout
	config.UserAgent = gatewayUserAgent
	if flags.Client.AdditionalUserAgent != "" {
		config.UserAgent = flags.Client.AdditionalUserAgent + " " + config.UserAgent
	}
	if flags.Client.UserAgent != "" {
		config.UserAgent = flags.Client.UserAgent + " " + config.UserAgent
	}
	return config
}

// interactive creates the configuration of the gateway interactively.
func (flags GatewayFlags) interactive(cmd *cobra.Command, setupDir string, overrides map[string]interface{}) error {
	ctx, _ := process.Ctx(cmd)

	satelliteAddress, err := wizard.PromptForSatellite(cmd)
	if err != nil {
		return Error.Wrap(err)
	}

	apiKey, err := wizard.PromptForAPIKey()
	if err != nil {
		return Error.Wrap(err)
	}

	passphrase, err := wizard.PromptForEncryptionPassphrase()
	if err != nil {
		return Error.Wrap(err)
	}

	access, err := uplink.RequestAccessWithPassphrase(ctx, satelliteAddress, apiKey, passphrase)
	if err != nil {
		return Error.Wrap(err)
	}

	accessData, err := access.Serialize()
	if err != nil {
		return Error.Wrap(err)
	}
	overrides["access"] = accessData

	tracingEnabled, err := wizard.PromptForTracing()
	if err != nil {
		return Error.Wrap(err)
	}
	if tracingEnabled {
		overrides["tracing.enabled"] = true
		overrides["tracing.sample"] = 0.1
		overrides["tracing.interval"] = 30 * time.Second
	}

	err = process.SaveConfig(cmd, filepath.Join(setupDir, "config.yaml"),
		process.SaveConfigWithOverrides(overrides),
		process.SaveConfigRemovingDeprecated())
	if err != nil {
		return Error.Wrap(err)
	}

	fmt.Println(`
Your S3 Gateway is configured and ready to use!

Some things to try next:

* See https://docs.storj.io/api-reference/s3-gateway for some example commands`)

	return nil
}

// nonInteractive creates the configuration of the gateway non-interactively.
func (flags GatewayFlags) nonInteractive(cmd *cobra.Command, setupDir string, overrides map[string]interface{}) (err error) {
	ctx, _ := process.Ctx(cmd)

	var access *uplink.Access
	accessString := setupCfg.Access

	if accessString != "" {
		access, err = uplink.ParseAccess(accessString)
	} else if setupCfg.SatelliteAddress != "" && setupCfg.APIKey != "" && setupCfg.Passphrase != "" {
		satellite := setupCfg.SatelliteAddress
		if fullAddress, ok := wizard.SatellitesURL[satellite]; ok {
			satellite = fullAddress
		}
		access, err = uplink.RequestAccessWithPassphrase(ctx, satellite, setupCfg.APIKey, setupCfg.Passphrase)
	} else {
		err = errs.New("non-interactive setup requires '--access' flag or all '--satellite-address', '--api-key', '--passphrase' flags")
	}
	if err != nil {
		return err
	}

	accessData, err := access.Serialize()
	if err != nil {
		return err
	}
	overrides["access"] = accessData

	return Error.Wrap(process.SaveConfig(cmd, filepath.Join(setupDir, "config.yaml"),
		process.SaveConfigWithOverrides(overrides),
		process.SaveConfigRemovingDeprecated()))
}

/*
`setUsageFunc` is a bit unconventional but cobra didn't leave much room for
extensibility here. `cmd.SetUsageTemplate` is fairly useless for our case without
the ability to add to the template's function map (see: https://golang.org/pkg/text/template/#hdr-Functions).

Because we can't alter what `cmd.Usage` generates, we have to edit it afterwards.
In order to hook this function *and* get the usage string, we have to juggle the
`cmd.usageFunc` between our hook and `nil`, so that we can get the usage string
from the default usage func.
*/
func setUsageFunc(cmd *cobra.Command) {
	if findBoolFlagEarly("advanced") {
		return
	}

	reset := func() (set func()) {
		original := cmd.UsageFunc()
		cmd.SetUsageFunc(nil)

		return func() {
			cmd.SetUsageFunc(original)
		}
	}

	cmd.SetUsageFunc(func(cmd *cobra.Command) error {
		set := reset()
		usageStr := cmd.UsageString()
		defer set()

		usageScanner := bufio.NewScanner(bytes.NewBufferString(usageStr))

		var basicFlags []string
		cmd.Flags().VisitAll(func(flag *pflag.Flag) {
			basic, ok := flag.Annotations[cfgstruct.BasicHelpAnnotationName]
			if ok && len(basic) == 1 && basic[0] == "true" {
				basicFlags = append(basicFlags, flag.Name)
			}
		})

		for usageScanner.Scan() {
			line := usageScanner.Text()
			trimmedLine := strings.TrimSpace(line)

			var flagName string
			if _, err := fmt.Sscanf(trimmedLine, "--%s", &flagName); err != nil {
				fmt.Println(line)
				continue
			}

			// TODO: properly filter flags with short names
			if !strings.HasPrefix(trimmedLine, "--") {
				fmt.Println(line)
			}

			for _, basicFlag := range basicFlags {
				if basicFlag == flagName {
					fmt.Println(line)
				}
			}
		}
		return nil
	})
}

func findBoolFlagEarly(flagName string) bool {
	for i, arg := range os.Args {
		arg := arg
		argHasPrefix := func(format string, args ...interface{}) bool {
			return strings.HasPrefix(arg, fmt.Sprintf(format, args...))
		}

		if !argHasPrefix("--%s", flagName) {
			continue
		}

		// NB: covers `--<flagName> false` usage
		if i+1 != len(os.Args) {
			next := os.Args[i+1]
			if next == "false" {
				return false
			}
		}

		if !argHasPrefix("--%s=false", flagName) {
			return true
		}
	}
	return false
}

func main() {
	process.Exec(rootCmd)
}
