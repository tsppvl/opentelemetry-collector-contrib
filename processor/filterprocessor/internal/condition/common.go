// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package condition // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/filterprocessor/internal/condition"

import (
	"go.opentelemetry.io/collector/component"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
)

// resourceConditionBuilder creates signal-specific conditions (parsedLogConditions, parsedMetricConditions, parsedTraceConditions, parsedProfileConditions) from resource conditions
type resourceConditionBuilder[R any] func([]*ottl.Condition[*ottlresource.TransformContext], component.TelemetrySettings, ottl.ErrorMode) R

// scopeConditionBuilder creates signal-specific conditions (parsedLogConditions, parsedMetricConditions, parsedTraceConditions, parsedProfileConditions) from scope conditions
type scopeConditionBuilder[R any] func([]*ottl.Condition[*ottlscope.TransformContext], component.TelemetrySettings, ottl.ErrorMode) R

func resourceConditionsConverter[R any](builder resourceConditionBuilder[R]) ottl.ParsedConditionsConverter[*ottlresource.TransformContext, R] {
	return func(pc *ottl.ParserCollection[R], conditions ottl.ConditionsGetter, parsedConditions []*ottl.Condition[*ottlresource.TransformContext]) (R, error) {
		contextConditions, err := toContextConditions(conditions)
		if err != nil {
			return *new(R), err
		}
		errorMode := getErrorMode(pc, contextConditions)
		return builder(parsedConditions, pc.Settings, errorMode), nil
	}
}

func scopeConditionsConverter[R any](builder scopeConditionBuilder[R]) ottl.ParsedConditionsConverter[*ottlscope.TransformContext, R] {
	return func(pc *ottl.ParserCollection[R], conditions ottl.ConditionsGetter, parsedConditions []*ottl.Condition[*ottlscope.TransformContext]) (R, error) {
		contextConditions, err := toContextConditions(conditions)
		if err != nil {
			return *new(R), err
		}
		errorMode := getErrorMode(pc, contextConditions)
		return builder(parsedConditions, pc.Settings, errorMode), nil
	}
}
