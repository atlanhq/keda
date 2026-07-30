package metricscollector

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

// hasSeries reports whether the collector currently exposes a series carrying the given label value.
func hasSeries(t *testing.T, c prometheus.Collector, label, value string) bool {
	t.Helper()

	ch := make(chan prometheus.Metric)
	go func() {
		c.Collect(ch)
		close(ch)
	}()

	found := false
	for m := range ch {
		var pb dto.Metric
		if !assert.NoError(t, m.Write(&pb)) {
			continue
		}
		for _, l := range pb.GetLabel() {
			if l.GetName() == label && l.GetValue() == value {
				found = true
			}
		}
	}
	return found
}

func TestDeleteScaledObjectMetrics(t *testing.T) {
	p := &PromMetrics{}

	const namespace = "delete-metrics-test"
	deleted := "worker-twd-main-aaaaaaa-scale"
	retained := "worker-twd-main-bbbbbbb-scale"

	for _, scaledObject := range []string{deleted, retained} {
		p.RecordScaledObjectError(namespace, scaledObject, errors.New("scaler with id 0 not found"))
		p.RecordScaledObjectPaused(namespace, scaledObject, false)
		p.RecordScalableObjectLatency(namespace, scaledObject, true, time.Second)
	}

	p.DeleteScaledObjectMetrics(namespace, deleted)

	assert.False(t, hasSeries(t, scaledObjectErrors, "scaledObject", deleted), "errors_total should be dropped for the deleted ScaledObject")
	assert.False(t, hasSeries(t, scaledObjectPaused, "scaledObject", deleted), "paused should be dropped for the deleted ScaledObject")
	assert.False(t, hasSeries(t, internalLoopLatency, "resource", deleted), "loop latency should be dropped for the deleted ScaledObject")

	assert.True(t, hasSeries(t, scaledObjectErrors, "scaledObject", retained), "errors_total should survive for other ScaledObjects")
	assert.True(t, hasSeries(t, scaledObjectPaused, "scaledObject", retained), "paused should survive for other ScaledObjects")
	assert.True(t, hasSeries(t, internalLoopLatency, "resource", retained), "loop latency should survive for other ScaledObjects")
}
