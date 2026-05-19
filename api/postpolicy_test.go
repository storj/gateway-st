// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/amwolff/awsig"
	"github.com/stretchr/testify/assert"
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

func TestCheckPostForm(t *testing.T) {
	newPostPolicy := func() api.PostPolicy {
		return api.PostPolicy{
			Expiration: api.PostPolicyExpiration{Time: time.Now().Add(time.Hour)},
			Conditions: api.PostPolicyConditions{
				Items: []api.PostPolicyCondition{
					{api.PostPolicyOperatorEqual, "$bucket", "my-bucket"},
					{api.PostPolicyOperatorEqual, "$key", "photo.jpg"},
				},
			},
		}
	}

	newSigV4PostPolicy := func() api.PostPolicy {
		policy := newPostPolicy()
		policy.Conditions.Items = append(policy.Conditions.Items, []api.PostPolicyCondition{
			{api.PostPolicyOperatorEqual, "$x-amz-algorithm", "algorithm"},
			{api.PostPolicyOperatorEqual, "$x-amz-credential", "credential"},
			{api.PostPolicyOperatorEqual, "$x-amz-date", "date"},
		}...)
		return policy
	}

	newSigV2PostPolicy := func() api.PostPolicy {
		policy := newPostPolicy()
		policy.Conditions.Items = append(policy.Conditions.Items, []api.PostPolicyCondition{
			{api.PostPolicyOperatorEqual, "$awsaccesskeyid", "access key ID"},
		}...)
		return policy
	}

	newPostForm := func() awsig.PostForm {
		return awsig.PostForm{
			"Bucket": {{Value: "my-bucket"}},
			"Key":    {{Value: "photo.jpg"}},
			"File":   {{Value: "vacation_photo.jpg"}},
		}
	}

	newSigV4PostForm := func() awsig.PostForm {
		form := newPostForm()
		form.Set("x-amz-algorithm", awsig.PostFormElement{Value: "algorithm"})
		form.Set("x-amz-credential", awsig.PostFormElement{Value: "credential"})
		form.Set("x-amz-date", awsig.PostFormElement{Value: "date"})
		form.Set("x-amz-signature", awsig.PostFormElement{Value: "signature"})
		return form
	}

	newSigV2PostForm := func() awsig.PostForm {
		form := newPostForm()
		form.Set("awsaccesskeyid", awsig.PostFormElement{Value: "access key ID"})
		form.Set("signature", awsig.PostFormElement{Value: "signature"})
		return form
	}

	t.Run("Expired", func(t *testing.T) {
		policy := newSigV4PostPolicy()
		policy.Expiration.Time = time.Now().Add(-time.Hour)
		err := api.CheckPostForm(policy, newSigV4PostForm())
		require.ErrorIs(t, err, apierr.CodePostPolicyExpired)
	})

	t.Run("eq", func(t *testing.T) {
		policy := newSigV4PostPolicy()

		t.Run("Match", func(t *testing.T) {
			err := api.CheckPostForm(policy, newSigV4PostForm())
			require.NoError(t, err)
		})

		t.Run("Mismatch", func(t *testing.T) {
			form := newSigV4PostForm()
			form.Set("bucket", awsig.PostFormElement{Value: "other-bucket"})

			err := api.CheckPostForm(policy, form)
			require.ErrorIs(t, err, apierr.PostFormConditionFailedError{
				Condition: `["eq","$bucket","my-bucket"]`,
			})
		})
	})

	t.Run("starts-with", func(t *testing.T) {
		policy := newSigV4PostPolicy()
		policy.Conditions.Items = append(policy.Conditions.Items, api.PostPolicyCondition{
			Operator: api.PostPolicyOperatorStartsWith,
			Key:      "$content-type",
			Value:    "image/",
		})

		t.Run("Match", func(t *testing.T) {
			form := newSigV4PostForm()
			form.Set("Content-Type", awsig.PostFormElement{Value: "image/jpeg"})

			err := api.CheckPostForm(policy, form)
			require.NoError(t, err)
		})

		t.Run("Mismatch", func(t *testing.T) {
			form := newSigV4PostForm()
			form.Set("Content-Type", awsig.PostFormElement{Value: "text/plain"})

			err := api.CheckPostForm(policy, form)
			require.ErrorIs(t, err, apierr.PostFormConditionFailedError{
				Condition: `["starts-with","$content-type","image/"]`,
			})
		})

		t.Run("Empty prefix matches anything", func(t *testing.T) {
			policy := newSigV4PostPolicy()
			policy.Conditions.Items = append(policy.Conditions.Items, api.PostPolicyCondition{
				Operator: api.PostPolicyOperatorStartsWith,
				Key:      "$content-type",
				Value:    "",
			})

			form := newSigV4PostForm()
			form.Set("Content-Type", awsig.PostFormElement{Value: "text/plain"})

			err := api.CheckPostForm(policy, form)
			require.NoError(t, err)
		})
	})

	t.Run("Extra form field", func(t *testing.T) {
		policy := newSigV4PostPolicy()
		form := newSigV4PostForm()
		form.Set("a", awsig.PostFormElement{Value: "1"})

		err := api.CheckPostForm(policy, form)
		require.ErrorIs(t, err, apierr.PostFormExtraFieldsError{
			FieldName: "a",
		})
	})

	t.Run("Multiple key fields", func(t *testing.T) {
		policy := newSigV4PostPolicy()
		form := newSigV4PostForm()
		form.Add("key", form.Get("key"))

		err := api.CheckPostForm(policy, form)
		require.ErrorIs(t, err, apierr.CodePostFormMultipleKeyFields)
	})

	t.Run("Missing form field", func(t *testing.T) {
		t.Run("Basic required fields", func(t *testing.T) {
			for _, tt := range []struct {
				field         string
				expectedErrIs error
			}{
				{"bucket", apierr.PostFormMissingFieldError{FieldName: "bucket"}},
				{"key", apierr.PostFormMissingFieldError{FieldName: "key"}},
				{"file", apierr.CodePostFormInvalidFileCount},
			} {
				policy := newSigV4PostPolicy()
				form := newSigV4PostForm()
				form.Del(tt.field)

				err := api.CheckPostForm(policy, form)
				assert.ErrorIs(t, err, tt.expectedErrIs)
			}
		})

		t.Run("SigV4 required fields", func(t *testing.T) {
			for _, field := range []string{
				"x-amz-credential",
				"x-amz-date",
				"x-amz-signature",
			} {
				policy := newSigV4PostPolicy()
				form := newSigV4PostForm()
				delete(form, http.CanonicalHeaderKey(field))

				err := api.CheckPostForm(policy, form)
				assert.ErrorIs(t, err, apierr.PostFormMissingFieldError{FieldName: field})
			}
		})

		t.Run("SigV2 required fields", func(t *testing.T) {
			policy := newSigV2PostPolicy()
			form := newSigV2PostForm()
			form.Del("signature")

			err := api.CheckPostForm(policy, form)
			require.ErrorIs(t, err, apierr.PostFormMissingFieldError{FieldName: "signature"})
		})

		t.Run("Missing signature version fields", func(t *testing.T) {
			err := api.CheckPostForm(newPostPolicy(), newPostForm())
			require.ErrorIs(t, err, apierr.CodeAccessDenied)
		})
	})

	for _, tt := range []struct {
		condition     api.PostPolicyCondition
		expectedErrIs error
	}{
		{
			condition: api.PostPolicyCondition{
				Operator: api.PostPolicyOperatorEqual,
				Key:      "$file",
				Value:    "vacation_photo.jpg",
			},
			expectedErrIs: apierr.PostFormConditionFailedError{
				Condition: `["eq","$file","vacation_photo.jpg"]`,
			},
		},
		{
			condition: api.PostPolicyCondition{
				Operator: api.PostPolicyOperatorStartsWith,
				Key:      "$file",
				Value:    "vacation",
			},
			expectedErrIs: apierr.PostFormConditionFailedError{
				Condition: `["starts-with","$file","vacation"]`,
			},
		},
	} {
		t.Run(fmt.Sprintf("File condition always fails/%s", tt.condition.Operator), func(t *testing.T) {
			policy := newSigV4PostPolicy()
			policy.Conditions.Items = append(policy.Conditions.Items, tt.condition)
			form := newSigV4PostForm()

			err := api.CheckPostForm(policy, form)
			require.ErrorIs(t, err, tt.expectedErrIs)
		})
	}
}

func TestExtractMetadataFromPostForm(t *testing.T) {
	postForm := awsig.PostForm{
		"Content-Type":        {{Value: "image/png"}},
		"Cache-Control":       {{Value: "max-age=60"}},
		"Content-Language":    {{Value: "en-US"}, {Value: "en-CA"}},
		"Content-Encoding":    {{Value: "gzip"}},
		"Content-Disposition": {{Value: "inline"}},
		"Expires":             {{Value: "+2h"}},
		"X-Amz-Storage-Class": {{Value: "STANDARD"}},
		"X-Amz-Meta-Key":      {{Value: "amzMeta1"}, {Value: "amzMeta2"}},
		"X-Minio-Meta-Key":    {{Value: "minioMeta1"}, {Value: "minioMeta2"}},
	}

	expectedMeta := make(map[string]string)
	for k, elems := range postForm {
		values := make([]string, 0, len(elems))
		for _, elem := range elems {
			values = append(values, elem.Value)
		}
		expectedMeta[k] = strings.Join(values, ",")
	}

	// Insert fields that should not be extracted.
	maps.Copy(postForm, awsig.PostForm{
		"Authorization":  {{Value: "AWS 1234567890ABCDEF:g0h1i2j3k4l5m6n7o8p9q0r1s2="}},
		"Content-Length": {{Value: "123"}},
	})

	extracted, err := api.ExtractMetadataFromPostForm(postForm)
	require.NoError(t, err)
	require.Equal(t, expectedMeta, extracted)
}
