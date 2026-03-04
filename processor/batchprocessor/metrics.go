// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor // import "go.opentelemetry.io/collector/processor/batchprocessor"

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/batchprocessor/internal/metadata"
	"go.opentelemetry.io/collector/processor/internal"
)

type trigger int

const (
	triggerTimeout trigger = iota
	triggerBatchSize
)

type batchProcessorTelemetry struct {
	exportCtx context.Context

	processorAttr    metric.MeasurementOption
	destinationAttrs []metric.MeasurementOption // one per destination
	telemetryBuilder *metadata.TelemetryBuilder
}

const signalKey = "otel.signal"

func newBatchProcessorTelemetry(set processor.Settings, signal pipeline.Signal, currentMetadataCardinality func() int) (*batchProcessorTelemetry, error) {
	attrs := metric.WithAttributeSet(attribute.NewSet(
		attribute.String(internal.ProcessorKey, set.ID.String()),
		attribute.String(signalKey, signal.String()),
	))

	telemetryBuilder, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	err = telemetryBuilder.RegisterProcessorBatchMetadataCardinalityCallback(func(_ context.Context, observer metric.Int64Observer) error {
		observer.Observe(int64(currentMetadataCardinality()), attrs)
		return nil
	})
	if err != nil {
		return nil, err
	}
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

	return &batchProcessorTelemetry{
		exportCtx:        context.Background(),
		telemetryBuilder: telemetryBuilder,
		processorAttr:    attrs,
		destinationAttrs: destAttrs,
	}, nil
}

func (bpt *batchProcessorTelemetry) record(trigger trigger, sent, bytes int64) {
	switch trigger {
	case triggerBatchSize:
		bpt.telemetryBuilder.ProcessorBatchBatchSizeTriggerSend.Add(bpt.exportCtx, 1, bpt.processorAttr)
	case triggerTimeout:
		bpt.telemetryBuilder.ProcessorBatchTimeoutTriggerSend.Add(bpt.exportCtx, 1, bpt.processorAttr)
	}

	bpt.telemetryBuilder.ProcessorBatchBatchSendSize.Record(bpt.exportCtx, sent, bpt.processorAttr)
	bpt.telemetryBuilder.ProcessorBatchBatchSendSizeBytes.Record(bpt.exportCtx, bytes, bpt.processorAttr)
	for _, destAttrs := range bpt.destinationAttrs {
		bpt.telemetryBuilder.ProcessorOutgoingItems.Add(bpt.exportCtx, sent, bpt.processorAttr, destAttrs)
	}
}
