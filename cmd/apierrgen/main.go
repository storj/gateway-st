// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	_ "embed"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/zeebo/errs"

	"storj.io/gateway/cmd/apierrgen/gen"
)

var (
	yamlPath    string
	outFilePath string
	packageName string
)

var rootCmd = &cobra.Command{
	RunE: func(cmd *cobra.Command, args []string) error {
		src, err := os.ReadFile(yamlPath)
		if err != nil {
			return fmt.Errorf("error reading input file: %w", err)
		}

		generated, err := gen.Generate(src, packageName)
		if err != nil {
			if len(generated) != 0 {
				_, writeErr := os.Stderr.Write(generated)
				err = errs.Combine(err, writeErr)
			}
			return err
		}

		if err := os.WriteFile(outFilePath, generated, 0o644); err != nil {
			return fmt.Errorf("error writing generated source: %w", err)
		}

		fmt.Printf("generated source written to %s\n", outFilePath)

		return nil
	},
}

func main() {
	rootCmd.Flags().StringVarP(&packageName, "package", "p", "", "go package name to use in the generated file (required)")
	rootCmd.Flags().StringVarP(&yamlPath, "in", "i", "", "path to the YAML file containing error definitions (required)")
	rootCmd.Flags().StringVarP(&outFilePath, "out", "o", "", "path that the generated file should be written to (required)")

	must(rootCmd.MarkFlagRequired("package"))
	must(rootCmd.MarkFlagRequired("in"))
	must(rootCmd.MarkFlagRequired("out"))

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
