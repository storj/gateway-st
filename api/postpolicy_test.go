// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/gateway/api"
	"storj.io/gateway/api/apierr"
)

func TestPostPolicyUnmarshal(t *testing.T) {
	expiration := time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.UTC)
	expirationStr := expiration.Format(time.RFC3339Nano)

	unmarshalPolicy := func(policyStr string) (api.PostPolicy, error) {
		var form api.PostPolicy
		err := json.Unmarshal([]byte(policyStr), &form)
		return form, err
	}

	t.Run("Valid policy", func(t *testing.T) {
		form, err := unmarshalPolicy(`{
			"expiration": "` + expirationStr + `",
			"conditions": [
				{"bucket": "my-bucket"},
				["eq", "$key", "uploads/file.txt"],
				["starts-with", "$content-type", "image/"],
				["content-length-range", 1024, 10485760]
			]
		}`)
		require.NoError(t, err)

		require.Equal(t, api.PostPolicy{
			Expiration: api.PostPolicyExpiration{Time: expiration},
			Conditions: api.PostPolicyConditions{
				Items: []api.PostPolicyCondition{
					{
						Operator: api.PostPolicyOperatorEqual,
						Key:      "$bucket",
						Value:    "my-bucket",
					},
					{
						Operator: api.PostPolicyOperatorEqual,
						Key:      "$key",
						Value:    "uploads/file.txt",
					},
					{
						Operator: api.PostPolicyOperatorStartsWith,
						Key:      "$content-type",
						Value:    "image/",
					},
				},
				ContentLengthRange: api.ContentLengthRange{
					Min:   1024,
					Max:   10485760,
					Valid: true,
				},
			},
		}, form)
	})

	t.Run("Missing expiration", func(t *testing.T) {
		_, err := unmarshalPolicy(`{
			"conditions": []
		}`)
		require.ErrorIs(t, err, apierr.CodePostPolicyMissingExpiration)
	})

	t.Run("Missing conditions", func(t *testing.T) {
		_, err := unmarshalPolicy(`{
			"expiration": "` + expirationStr + `"
		}`)
		require.ErrorIs(t, err, apierr.CodePostPolicyMissingConditions)
	})

	t.Run("Unknown top-level field", func(t *testing.T) {
		_, err := unmarshalPolicy(`{
			"expiration": "` + expirationStr + `",
			"conditions": [],
			"unexpected": "value"
		}`)
		require.ErrorIs(t, err, apierr.PostPolicyUnexpectedElementError{
			ElementName: "unexpected",
		})
	})

	t.Run("Invalid expiration type", func(t *testing.T) {
		_, err := unmarshalPolicy(`{
			"expiration": 12345,
			"conditions": []
		}`)
		require.ErrorIs(t, err, apierr.CodePostPolicyInvalidExpirationType)
	})

	t.Run("Invalid expiration format", func(t *testing.T) {
		_, err := unmarshalPolicy(`{
			"expiration": "foo",
			"conditions": []
		}`)
		require.ErrorIs(t, err, apierr.PostPolicyInvalidExpirationError{
			Value: "foo",
		})
	})

	t.Run("Invalid conditions type", func(t *testing.T) {
		_, err := unmarshalPolicy(`{
			"expiration": "` + expirationStr + `",
			"conditions": "invalid"
		}`)
		require.ErrorIs(t, err, apierr.CodePostPolicyInvalidConditionsType)
	})

	for _, tt := range []struct {
		name        string
		conditions  string
		expectedErr error
	}{
		{
			name:       "Unknown operator",
			conditions: `[["unknown", "$key", "value"]]`,
			expectedErr: apierr.PostPolicyConditionUnknownOperationError{
				OperationName: "unknown",
			},
		},
		{
			name:       "eq with wrong arg count",
			conditions: `[["eq", "$key"]]`,
			expectedErr: apierr.PostPolicyConditionInvalidArgumentCountError{
				OperationName: string(api.PostPolicyOperatorEqual),
			},
		},
		{
			name:       "starts-with with wrong arg count",
			conditions: `[["starts-with", "$key"]]`,
			expectedErr: apierr.PostPolicyConditionInvalidArgumentCountError{
				OperationName: string(api.PostPolicyOperatorStartsWith),
			},
		},
		{
			name:       "content-length-range with wrong arg count",
			conditions: `[["content-length-range", 0]]`,
			expectedErr: apierr.PostPolicyConditionInvalidArgumentCountError{
				OperationName: string(api.PostPolicyOperatorContentLengthRange),
			},
		},
		{
			name:        "content-length-range with invalid args",
			conditions:  `[["content-length-range", "abc", "def"]]`,
			expectedErr: apierr.CodePostPolicyContentLengthConditionInvalidString,
		},
		{
			name:        "content-length-range with float arg",
			conditions:  `[["content-length-range", 1.5, 100]]`,
			expectedErr: apierr.CodePostPolicyInvalidJSON,
		},
		{
			name:        "Map condition with too many properties",
			conditions:  `[{"a": "1", "b": "2"}]`,
			expectedErr: apierr.CodePostPolicySimpleConditionTooManyProperties,
		},
		{
			name:        "Array condition key missing prefix",
			conditions:  `[["eq", "key", "value"]]`,
			expectedErr: apierr.CodePostPolicyMatchConditionKeyMissingPrefix,
		},
		{
			name:        "Invalid condition type",
			conditions:  `[42]`,
			expectedErr: apierr.CodePostPolicyInvalidConditionType,
		},
	} {
		t.Run("Invalid conditions/"+tt.name, func(t *testing.T) {
			_, err := unmarshalPolicy(`{
				"expiration": "` + expirationStr + `",
				"conditions": ` + tt.conditions + `
			}`)
			require.ErrorIs(t, err, tt.expectedErr)
		})
	}

	t.Run("content-length-range string-encoded integer args", func(t *testing.T) {
		form, err := unmarshalPolicy(`{
			"expiration": "` + expirationStr + `",
			"conditions": [["content-length-range", "100", "5000"]]
		}`)
		require.NoError(t, err)
		require.Equal(t, api.ContentLengthRange{
			Min:   100,
			Max:   5000,
			Valid: true,
		}, form.Conditions.ContentLengthRange)
	})
}
