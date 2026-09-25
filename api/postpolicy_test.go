// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
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

	t.Run("content-length-range intersects multiple conditions", func(t *testing.T) {
		form, err := unmarshalPolicy(`{
			"expiration": "` + expirationStr + `",
			"conditions": [["content-length-range", 10, 20], ["content-length-range", 0, 100]]
		}`)
		require.NoError(t, err)
		require.Equal(t, api.ContentLengthRange{
			Min:   10,
			Max:   20,
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
			// Fields exempt from policy conditions.
			"Policy":          {{Value: "cG9saWN5"}},
			"X-Ignore-Submit": {{Value: "Upload"}},
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
		err := api.CheckPostForm(policy, newSigV4PostForm(), "my-bucket")
		require.ErrorIs(t, err, apierr.CodePostPolicyExpired)
	})

	t.Run("eq", func(t *testing.T) {
		policy := newSigV4PostPolicy()

		t.Run("Match", func(t *testing.T) {
			err := api.CheckPostForm(policy, newSigV4PostForm(), "my-bucket")
			require.NoError(t, err)
		})

		t.Run("Mismatch", func(t *testing.T) {
			form := newSigV4PostForm()
			form.Set("bucket", awsig.PostFormElement{Value: "other-bucket"})

			err := api.CheckPostForm(policy, form, "my-bucket")
			require.ErrorIs(t, err, apierr.PostFormConditionFailedError{
				Condition: `["eq","$bucket","my-bucket"]`,
			})
		})
	})

	t.Run("URL bucket", func(t *testing.T) {
		policy := newSigV4PostPolicy()

		// Like boto3's generate_presigned_post, omit the bucket field.
		form := newSigV4PostForm()
		form.Del("bucket")
		require.NoError(t, api.CheckPostForm(policy, form, "my-bucket"))

		// The policy is checked against the URL bucket, not the form field.
		err := api.CheckPostForm(policy, form, "other-bucket")
		require.ErrorIs(t, err, apierr.PostFormConditionFailedError{
			Condition: `["eq","$bucket","my-bucket"]`,
		})

		// Field names are case-insensitive.
		policy.Conditions.Items = append(policy.Conditions.Items, api.PostPolicyCondition{
			Operator: api.PostPolicyOperatorEqual, Key: "$Bucket", Value: "my-bucket",
		})
		require.NoError(t, api.CheckPostForm(policy, form, "my-bucket"))

		// A form bucket must match the URL bucket.
		err = api.CheckPostForm(policy, newSigV4PostForm(), "other-bucket")
		require.ErrorIs(t, err, apierr.PostFormConditionFailedError{
			Condition: `["eq","$bucket","other-bucket"]`,
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

			err := api.CheckPostForm(policy, form, "my-bucket")
			require.NoError(t, err)
		})

		t.Run("Mismatch", func(t *testing.T) {
			form := newSigV4PostForm()
			form.Set("Content-Type", awsig.PostFormElement{Value: "text/plain"})

			err := api.CheckPostForm(policy, form, "my-bucket")
			require.ErrorIs(t, err, apierr.PostFormConditionFailedError{
				Condition: `["starts-with","$content-type","image/"]`,
			})
		})

		t.Run("Every list entry must match", func(t *testing.T) {
			form := newSigV4PostForm()
			form.Set("Content-Type", awsig.PostFormElement{Value: "image/png"})
			form.Add("Content-Type", awsig.PostFormElement{Value: "text/html"})

			err := api.CheckPostForm(policy, form, "my-bucket")
			require.ErrorIs(t, err, apierr.PostFormConditionFailedError{
				Condition: `["starts-with","$content-type","image/"]`,
			})

			form.Set("Content-Type", awsig.PostFormElement{Value: "image/png, image/jpeg"})
			require.NoError(t, api.CheckPostForm(policy, form, "my-bucket"))
		})

		t.Run("Repeated fields are joined", func(t *testing.T) {
			form := newSigV4PostForm()
			form.Set("Content-Type", awsig.PostFormElement{Value: "image/png"})
			form.Set("x-amz-meta-foo", awsig.PostFormElement{Value: "a1"})
			form.Add("x-amz-meta-foo", awsig.PostFormElement{Value: "zzz"})

			for _, tt := range []struct {
				cond api.PostPolicyCondition
				ok   bool
			}{
				{api.PostPolicyCondition{api.PostPolicyOperatorStartsWith, "$x-amz-meta-foo", "a"}, true},
				{api.PostPolicyCondition{api.PostPolicyOperatorEqual, "$x-amz-meta-foo", "a1,zzz"}, true},
				{api.PostPolicyCondition{api.PostPolicyOperatorEqual, "$x-amz-meta-foo", "a1"}, false},
			} {
				policy := newSigV4PostPolicy()
				policy.Conditions.Items = append(policy.Conditions.Items,
					api.PostPolicyCondition{api.PostPolicyOperatorStartsWith, "$content-type", "image/"},
					tt.cond)

				err := api.CheckPostForm(policy, form, "my-bucket")
				if tt.ok {
					assert.NoError(t, err, tt.cond)
				} else {
					assert.ErrorAs(t, err, &apierr.PostFormConditionFailedError{}, tt.cond)
				}
			}
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

			err := api.CheckPostForm(policy, form, "my-bucket")
			require.NoError(t, err)
		})
	})

	t.Run("SigV2 exempt fields", func(t *testing.T) {
		err := api.CheckPostForm(newPostPolicy(), newSigV2PostForm(), "my-bucket")
		require.NoError(t, err)
	})

	t.Run("Extra form field", func(t *testing.T) {
		policy := newSigV4PostPolicy()
		form := newSigV4PostForm()
		form.Set("a", awsig.PostFormElement{Value: "1"})

		err := api.CheckPostForm(policy, form, "my-bucket")
		require.ErrorIs(t, err, apierr.PostFormExtraFieldsError{
			FieldName: "a",
		})
	})

	t.Run("Missing bucket condition", func(t *testing.T) {
		policy := newSigV4PostPolicy()
		policy.Conditions.Items = policy.Conditions.Items[1:]

		form := newSigV4PostForm()
		form.Del("bucket")

		err := api.CheckPostForm(policy, form, "my-bucket")
		require.ErrorIs(t, err, apierr.PostFormExtraFieldsError{
			FieldName: "bucket",
		})
	})

	t.Run("Multiple key fields", func(t *testing.T) {
		policy := newSigV4PostPolicy()
		form := newSigV4PostForm()
		form.Add("key", form.Get("key"))

		err := api.CheckPostForm(policy, form, "my-bucket")
		require.ErrorIs(t, err, apierr.CodePostFormMultipleKeyFields)
	})

	t.Run("Missing form field", func(t *testing.T) {
		t.Run("Basic required fields", func(t *testing.T) {
			for _, tt := range []struct {
				field         string
				expectedErrIs error
			}{
				{"key", apierr.PostFormMissingFieldError{FieldName: "key"}},
				{"file", apierr.CodePostFormInvalidFileCount},
			} {
				policy := newSigV4PostPolicy()
				form := newSigV4PostForm()
				form.Del(tt.field)

				err := api.CheckPostForm(policy, form, "my-bucket")
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

				err := api.CheckPostForm(policy, form, "my-bucket")
				assert.ErrorIs(t, err, apierr.PostFormMissingFieldError{FieldName: field})
			}
		})

		t.Run("SigV2 required fields", func(t *testing.T) {
			policy := newSigV2PostPolicy()
			form := newSigV2PostForm()
			form.Del("signature")

			err := api.CheckPostForm(policy, form, "my-bucket")
			require.ErrorIs(t, err, apierr.PostFormMissingFieldError{FieldName: "signature"})
		})

		t.Run("Policy field", func(t *testing.T) {
			policy := newSigV4PostPolicy()
			policy.Conditions.Items = append(policy.Conditions.Items, api.PostPolicyCondition{
				Operator: api.PostPolicyOperatorEqual,
				Key:      "$x-amz-meta-foo",
				Value:    "bar",
			})

			err := api.CheckPostForm(policy, newSigV4PostForm(), "my-bucket")
			require.ErrorIs(t, err, apierr.PostFormConditionFailedError{
				Condition: `["eq","$x-amz-meta-foo","bar"]`,
			})
		})

		t.Run("Missing signature version fields", func(t *testing.T) {
			err := api.CheckPostForm(newPostPolicy(), newPostForm(), "my-bucket")
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

			err := api.CheckPostForm(policy, form, "my-bucket")
			require.ErrorIs(t, err, tt.expectedErrIs)
		})
	}
}
