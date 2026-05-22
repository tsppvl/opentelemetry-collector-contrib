// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package condition

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
)

// These blank-identifier assignments take function values of ottl.WithConditionConverter
// instantiated with each signal-specific parsed-conditions type. Taking a function value
// forces the Go compiler to generate the GCshape stencil in THIS package's object file.
//
// Without this, Go 1.25's internal linker (used when CGO_ENABLED=0) fails with:
//   relocation target pkg/ottl.WithConditionConverter[...] not defined
// because the stencil is expected in pkg/ottl but pkg/ottl has no direct call with
// filterprocessor-local types, so the stencil is never emitted there.
var (
	_ = ottl.WithConditionConverter[*ottlresource.TransformContext, parsedTraceConditions]
	_ = ottl.WithConditionConverter[*ottlscope.TransformContext, parsedTraceConditions]
	_ = ottl.WithConditionConverter[*ottlresource.TransformContext, parsedMetricConditions]
	_ = ottl.WithConditionConverter[*ottlscope.TransformContext, parsedMetricConditions]
	_ = ottl.WithConditionConverter[*ottlresource.TransformContext, parsedLogConditions]
	_ = ottl.WithConditionConverter[*ottlscope.TransformContext, parsedLogConditions]
	_ = ottl.WithConditionConverter[*ottlresource.TransformContext, parsedProfileConditions]
	_ = ottl.WithConditionConverter[*ottlscope.TransformContext, parsedProfileConditions]
)
