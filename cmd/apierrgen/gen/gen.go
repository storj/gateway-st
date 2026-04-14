// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package gen

import (
	"bytes"
	_ "embed"
	"go/format"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"text/template"

	"github.com/zeebo/errs"
	"gopkg.in/yaml.v3"
)

var (
	//go:embed template.tmpl
	tmplStr string
	tmpl    = template.Must(template.New("").Funcs(template.FuncMap{
		"httpStatusToConst": httpStatusToConst,
	}).Parse(tmplStr))
)

// ErrorDefinition is the definition of an error.
type ErrorDefinition struct {
	Name        string `yaml:"name"`
	Code        string `yaml:"code"`
	Description string `yaml:"description"`
	HTTPStatus  int    `yaml:"http_status"`
}

// Generate generates Go code containing definitions for error codes and responses
// based on the provided input. The input is expected to be YAML containing a sequence
// of mappings, each with the following properties:
//
//   - name: The internal name of the error.
//   - code: The error code to be exposed to clients.
//   - description: The error message to be exposed to clients.
//   - http_status: The HTTP status code of the error response.
func Generate(in []byte, packageName string) (out []byte, err error) {
	var errDefs []ErrorDefinition
	if err := yaml.Unmarshal(in, &errDefs); err != nil {
		return nil, errs.New("error parsing input file: %w", err)
	}
	slices.SortFunc(errDefs, func(defA, defB ErrorDefinition) int {
		return strings.Compare(defA.Name, defB.Name)
	})

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, struct {
		PackageName string
		Errors      []ErrorDefinition
	}{
		PackageName: packageName,
		Errors:      errDefs,
	})
	if err != nil {
		return nil, errs.New("error executing template: %w", err)
	}

	bufBytes := buf.Bytes()
	formatted, err := format.Source(bufBytes)
	if err != nil {
		return bufBytes, errs.New("error formatting generated source: %w", err)
	}

	return formatted, nil
}

var httpStatusToConstMap = map[int]string{
	http.StatusBadRequest:          "http.StatusBadRequest",
	http.StatusForbidden:           "http.StatusForbidden",
	http.StatusNotFound:            "http.StatusNotFound",
	http.StatusLengthRequired:      "http.StatusLengthRequired",
	http.StatusInternalServerError: "http.StatusInternalServerError",
	http.StatusNotImplemented:      "http.StatusNotImplemented",
}

// httpStatusToConst maps a status code to its corresponding net/http constant name.
// If the code is unknown, it is returned as an integer literal.
func httpStatusToConst(code int) string {
	if constName, ok := httpStatusToConstMap[code]; ok {
		return constName
	}
	return strconv.Itoa(code)
}
