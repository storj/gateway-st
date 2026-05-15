// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package api

import (
	"encoding/json"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"storj.io/gateway/api/apierr"
)

// PostPolicyOperator is the operator of a POST policy condition.
type PostPolicyOperator string

const (
	// PostPolicyOperatorEqual is the POST policy operator for strict matching.
	PostPolicyOperatorEqual PostPolicyOperator = "eq"
	// PostPolicyOperatorStartsWith is the POST policy operator for prefix matching.
	PostPolicyOperatorStartsWith PostPolicyOperator = "starts-with"
	// PostPolicyOperatorContentLengthRange is the POST policy operator for enforcing an allowable
	// range on the Content-Length of a request.
	PostPolicyOperatorContentLengthRange PostPolicyOperator = "content-length-range"
)

// PostPolicyExpiration wraps time.Time to implement json.Unmarshaler with the time.RFC3339Nano format.
// The default implementation of json.Unmarshaler for time.Time uses the time.RFC3339 format,
// which is only precise up to a whole second.
type PostPolicyExpiration struct {
	time.Time
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (exp *PostPolicyExpiration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return apierr.PostPolicyInvalidExpirationError{
			Value: s,
		}
	}
	*exp = PostPolicyExpiration{Time: t}
	return nil
}

// PostPolicyCondition represents a POST policy condition.
type PostPolicyCondition struct {
	Operator PostPolicyOperator
	Key      string
	Value    string
}

// ContentLengthRange is a constraint on the size of a request body.
type ContentLengthRange struct {
	Min int64
	Max int64
	// Valid indicates whether the "content-length-range" key was found in the policy.
	Valid bool
}

// PostPolicyConditions contains conditions parsed from a POST policy document.
type PostPolicyConditions struct {
	Items              []PostPolicyCondition
	ContentLengthRange ContentLengthRange
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (conds *PostPolicyConditions) UnmarshalJSON(data []byte) error {
	var raw []any
	if err := json.Unmarshal(data, &raw); err != nil {
		return apierr.CodePostPolicyInvalidConditionsType
	}
	for _, item := range raw {
		if err := conds.addCondition(item); err != nil {
			return err
		}
	}
	return nil
}

func (conds *PostPolicyConditions) addCondition(val any) error {
	switch cond := val.(type) {
	case map[string]any:
		return conds.addMapCondition(cond)
	case []any:
		return conds.addArrayCondition(cond)
	default:
		return apierr.CodePostPolicyInvalidConditionType
	}
}

// applyMapCondition handles a condition expressed as a JSON object of the
// form {"key": "value"} (referred to as a simple condition by S3), which
// is equivalent to ["eq", "$key", "value"].
func (conds *PostPolicyConditions) addMapCondition(m map[string]any) error {
	if len(m) != 1 {
		return apierr.CodePostPolicySimpleConditionTooManyProperties
	}
	for k, v := range m {
		val, ok := v.(string)
		if !ok {
			return apierr.CodePostPolicySimpleConditionInvalidValueType
		}
		conds.Items = append(conds.Items, PostPolicyCondition{
			Operator: PostPolicyOperatorEqual,
			Key:      "$" + strings.ToLower(k),
			Value:    val,
		})
	}
	return nil
}

// applyArrayCondition handles a condition expressed as a three-element JSON array,
// which has multiple interpretations:
//   - ["eq" | "starts-with", "$key", "value"]
//   - ["content-length-range", min, max]
func (conds *PostPolicyConditions) addArrayCondition(arr []any) error {
	if len(arr) == 0 {
		return apierr.CodePostPolicyConditionMissingOperationID
	}
	op, ok := arr[0].(string)
	if !ok {
		return apierr.CodePostPolicyConditionMissingOperationID
	}

	opNormalized := PostPolicyOperator(strings.ToLower(op))
	switch opNormalized {
	case PostPolicyOperatorEqual, PostPolicyOperatorStartsWith:
		if len(arr) != 3 {
			return apierr.PostPolicyConditionInvalidArgumentCountError{
				OperationName: string(opNormalized),
			}
		}
		return conds.addMatchCondition(opNormalized, arr[1], arr[2])
	case PostPolicyOperatorContentLengthRange:
		if len(arr) != 3 {
			return apierr.PostPolicyConditionInvalidArgumentCountError{
				OperationName: string(opNormalized),
			}
		}
		return conds.addLengthRange(arr[1], arr[2])
	default:
		return apierr.PostPolicyConditionUnknownOperationError{
			OperationName: op,
		}
	}
}

func (conds *PostPolicyConditions) addMatchCondition(op PostPolicyOperator, rawKey, rawVal any) error {
	val, ok := rawVal.(string)
	if !ok {
		return apierr.CodePostPolicyMatchConditionInvalidValueType
	}

	// S3 supports keys that aren't strings. Particularly, a key may be a condition array,
	// and its value evaluates to "true" or "false" and may be compared to the condition value
	// (e.g. this is supported: ["eq", ["eq", "$bucket", bucket], "true"]). However, this
	// functionality is undocumented, and it's unlikely that any clients take advantage of it,
	// so we don't support it.
	key, ok := rawKey.(string)
	if !ok {
		return apierr.CodePostPolicyMatchConditionInvalidKeyType
	}

	// S3 also supports keys that aren't prefixed with "$". They are compared plainly against
	// the value (e.g. this is supported: ["eq", "foo", "foo"]). However, for the same reason
	// as above, we don't support them.
	key = strings.ToLower(key)
	if !strings.HasPrefix(key, "$") {
		return apierr.CodePostPolicyMatchConditionKeyMissingPrefix
	}

	conds.Items = append(conds.Items, PostPolicyCondition{
		Operator: op,
		Key:      key,
		Value:    val,
	})

	return nil
}

func (conds *PostPolicyConditions) addLengthRange(rawMin, rawMax any) error {
	min, err := conditionArgToInt64(rawMin)
	if err != nil {
		return err
	}

	max, err := conditionArgToInt64(rawMax)
	if err != nil {
		return err
	}

	conds.ContentLengthRange = ContentLengthRange{
		Min:   min,
		Max:   max,
		Valid: true,
	}
	return nil
}

func conditionArgToInt64(v any) (int64, error) {
	switch n := v.(type) {
	case float64:
		if math.Trunc(n) != n {
			return 0, apierr.CodePostPolicyInvalidJSON
		}
		return int64(n), nil
	case string:
		i, err := strconv.ParseInt(n, 10, 64)
		if err != nil {
			// We differ from S3 here by returning an error if the string
			// can't be parsed as a number.
			return 0, apierr.CodePostPolicyContentLengthConditionInvalidString
		}
		return i, nil
	default:
		return 0, apierr.CodePostPolicyContentLengthConditionInvalidValueType
	}
}

// PostPolicy represents a POST policy document.
type PostPolicy struct {
	Expiration PostPolicyExpiration
	Conditions PostPolicyConditions
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (f *PostPolicy) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	expRaw, ok := raw["expiration"]
	if !ok {
		return apierr.CodePostPolicyMissingExpiration
	}

	condsRaw, ok := raw["conditions"]
	if !ok {
		return apierr.CodePostPolicyMissingConditions
	}

	if len(raw) > 2 {
		// S3 returns the first unknown field encountered rather than the first unknown field,
		// but it isn't worth it to reduce the simplicity of this method for the sake of accuracy.
		unknownKeys := slices.DeleteFunc(slices.Sorted(maps.Keys(raw)), func(key string) bool {
			return key == "expiration" || key == "conditions"
		})
		return apierr.PostPolicyUnexpectedElementError{
			ElementName: unknownKeys[0],
		}
	}

	// Confirm that the expiration value is a string.
	var expStr string
	if err := json.Unmarshal(expRaw, &expStr); err != nil {
		return apierr.CodePostPolicyInvalidExpirationType
	}

	if err := json.Unmarshal(expRaw, &f.Expiration); err != nil {
		return err
	}

	// Confirm that the conditions value is a list.
	var condsCheck any
	if err := json.Unmarshal(condsRaw, &condsCheck); err != nil {
		return err
	}
	if _, ok := condsCheck.([]any); !ok {
		return apierr.CodePostPolicyInvalidConditionsType
	}

	if err := json.Unmarshal(condsRaw, &f.Conditions); err != nil {
		return err
	}

	return nil
}
