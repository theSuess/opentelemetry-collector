// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package processorhelper // import "go.opentelemetry.io/collector/processor/processorhelper"

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/internal"
	"go.opentelemetry.io/collector/processor/processorhelper/internal/metadata"
)

const signalKey = "otel.signal"

type obsReport struct {
	baseAttrs        metric.MeasurementOption   // processor + signal (for duration)
	destinationAttrs []metric.MeasurementOption // one per destination
	telemetryBuilder *metadata.TelemetryBuilder
}

func newObsReport(set processor.Settings, signal pipeline.Signal) (*obsReport, error) {
	telemetryBuilder, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	base := attribute.NewSet(
		attribute.String(internal.ProcessorKey, set.ID.String()),
		attribute.String(signalKey, signal.String()),
	)

	destAttrs := make([]metric.MeasurementOption, 0, len(set.DestinationIDs))
	for _, dest := range set.DestinationIDs {
		destAttrs = append(destAttrs, metric.WithAttributeSet(attribute.NewSet(
			attribute.String("destination", dest.String()),
		)))
	}
	if len(destAttrs) == 0 {
		// Fallback: record without destination if IDs are not available.
		destAttrs = []metric.MeasurementOption{metric.WithAttributeSet(attribute.NewSet())}
	}

	return &obsReport{
		baseAttrs:        metric.WithAttributeSet(base),
		destinationAttrs: destAttrs,
		telemetryBuilder: telemetryBuilder,
	}, nil
}

func (or *obsReport) recordInOut(ctx context.Context, incoming, outgoing int) {
	or.telemetryBuilder.ProcessorIncomingItems.Add(ctx, int64(incoming), or.baseAttrs)
	for _, destAttrs := range or.destinationAttrs {
		or.telemetryBuilder.ProcessorOutgoingItems.Add(ctx, int64(outgoing), or.baseAttrs, destAttrs)
	}
}

func (or *obsReport) recordInternalDuration(ctx context.Context, startTime time.Time) {
	or.telemetryBuilder.ProcessorInternalDuration.Record(
		ctx, time.Since(startTime).Seconds(), or.baseAttrs)
}
