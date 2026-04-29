package scalers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

var (
	temporalEndpoint  = "localhost:7233"
	temporalNamespace = "v2"
	temporalQueueName = "default"

	logger = logr.Discard()
)

type parseTemporalMetadataTestData struct {
	metadata map[string]string
	isError  bool
}

type temporalMetricIdentifier struct {
	metadataTestData *parseTemporalMetadataTestData
	triggerIndex     int
	name             string
}

var testTemporalMetadata = []parseTemporalMetadataTestData{
	// nothing passed
	{map[string]string{}, true},
	// Missing taskQueue, should fail
	{map[string]string{"endpoint": temporalEndpoint, "namespace": temporalNamespace}, true},
	// Missing namespace, should success
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": temporalQueueName}, false},
	// Missing endpoint, should fail
	{map[string]string{"taskQueue": temporalQueueName, "namespace": temporalNamespace}, true},
	// invalid minConnectTimeout
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": temporalQueueName, "namespace": temporalNamespace, "minConnectTimeout": "-1"}, true},
	// All good.
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": temporalQueueName, "namespace": temporalNamespace}, false},
	// All good + activationLagThreshold
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": temporalQueueName, "namespace": temporalNamespace, "activationTargetQueueSize": "10"}, false},
}

var temporalMetricIdentifiers = []temporalMetricIdentifier{
	{&testTemporalMetadata[5], 0, "s0-temporal-v2-default"},
	{&testTemporalMetadata[5], 1, "s1-temporal-v2-default"},
}

func TestTemporalParseMetadata(t *testing.T) {
	for _, testData := range testTemporalMetadata {
		metadata := &scalersconfig.ScalerConfig{TriggerMetadata: testData.metadata}
		_, err := parseTemporalMetadata(metadata, logger)

		if err != nil && !testData.isError {
			t.Error("Expected success but got err", err)
		}
		if err == nil && testData.isError {
			t.Error("Expected error but got success")
		}
	}
}

func TestTemporalGetMetricSpecForScaling(t *testing.T) {
	for _, testData := range temporalMetricIdentifiers {
		metadata, err := parseTemporalMetadata(&scalersconfig.ScalerConfig{
			TriggerMetadata: testData.metadataTestData.metadata,
			TriggerIndex:    testData.triggerIndex,
		}, logger)

		if err != nil {
			t.Fatal("Could not parse metadata:", err)
		}
		mockScaler := temporalScaler{
			metadata: metadata,
		}
		metricSpec := mockScaler.GetMetricSpecForScaling(context.Background())
		metricName := metricSpec[0].External.Metric.Name

		if metricName != testData.name {
			t.Error("Wrong External metric source name:", metricName)
		}
	}
}

func TestParseTemporalMetadata(t *testing.T) {
	cases := []struct {
		name        string
		metadata    map[string]string
		wantMeta    *temporalMetadata
		authParams  map[string]string
		resolvedEnv map[string]string
		wantErr     bool
	}{
		{
			name: "empty queue name",
			metadata: map[string]string{
				"endpoint":  "test:7233",
				"namespace": "default",
			},
			wantMeta: &temporalMetadata{
				Endpoint:                    "test:7233",
				Namespace:                   "default",
				TaskQueue:                   "",
				TargetQueueSize:             5,
				ActivationTargetQueueSize:   0,
				AllActive:                   false,
				Unversioned:                 false,
				IncludeRunningWorkflowCount: true,
				WorkerMetricsPort:           9464,
				MinConnectTimeout:           5,
			},
			wantErr: true,
		},
		{
			name: "empty namespace",
			metadata: map[string]string{
				"endpoint":  "test:7233",
				"taskQueue": "testxx",
			},
			wantMeta: &temporalMetadata{
				Endpoint:                    "test:7233",
				Namespace:                   "default",
				TaskQueue:                   "testxx",
				TargetQueueSize:             5,
				ActivationTargetQueueSize:   0,
				AllActive:                   false,
				Unversioned:                 false,
				IncludeRunningWorkflowCount: true,
				WorkerMetricsPort:           9464,
				MinConnectTimeout:           5,
			},
			wantErr: false,
		},
		{
			name: "activationTargetQueueSize should not be 0",
			metadata: map[string]string{
				"endpoint":                  "test:7233",
				"namespace":                 "default",
				"taskQueue":                 "testxx",
				"activationTargetQueueSize": "12",
			},
			wantMeta: &temporalMetadata{
				Endpoint:                    "test:7233",
				Namespace:                   "default",
				TaskQueue:                   "testxx",
				TargetQueueSize:             5,
				ActivationTargetQueueSize:   12,
				AllActive:                   false,
				Unversioned:                 false,
				IncludeRunningWorkflowCount: true,
				WorkerMetricsPort:           9464,
				MinConnectTimeout:           5,
			},
			wantErr: false,
		},
		{
			name: "apiKey should not be empty",
			metadata: map[string]string{
				"endpoint":  "test:7233",
				"namespace": "default",
				"taskQueue": "testxx",
			},
			wantMeta: &temporalMetadata{
				Endpoint:                    "test:7233",
				Namespace:                   "default",
				TaskQueue:                   "testxx",
				TargetQueueSize:             5,
				ActivationTargetQueueSize:   0,
				AllActive:                   false,
				Unversioned:                 false,
				IncludeRunningWorkflowCount: true,
				WorkerMetricsPort:           9464,
				APIKey:                      "test01",
				MinConnectTimeout:           5,
			},
			authParams: map[string]string{
				"apiKey": "test01",
			},
			wantErr: false,
		},
		{
			name: "queue type should not be empty",
			metadata: map[string]string{
				"endpoint":   "test:7233",
				"namespace":  "default",
				"taskQueue":  "testxx",
				"queueTypes": "workflow,activity",
			},
			wantMeta: &temporalMetadata{
				Endpoint:                    "test:7233",
				Namespace:                   "default",
				TaskQueue:                   "testxx",
				TargetQueueSize:             5,
				ActivationTargetQueueSize:   0,
				AllActive:                   false,
				Unversioned:                 false,
				IncludeRunningWorkflowCount: true,
				QueueTypes:                  []string{"workflow", "activity"},
				WorkerMetricsPort:           9464,
				MinConnectTimeout:           5,
			},
			wantErr: false,
		},
		{
			name: "read config from env",
			resolvedEnv: map[string]string{
				"endpoint":  "test:7233",
				"namespace": "default",
				"taskQueue": "testxx",
			},
			metadata: map[string]string{
				"endpointFromEnv":  "endpoint",
				"namespaceFromEnv": "namespace",
				"taskQueueFromEnv": "taskQueue",
			},
			wantMeta: &temporalMetadata{
				Endpoint:                    "test:7233",
				Namespace:                   "default",
				TaskQueue:                   "testxx",
				TargetQueueSize:             5,
				ActivationTargetQueueSize:   0,
				AllActive:                   false,
				Unversioned:                 false,
				IncludeRunningWorkflowCount: true,
				WorkerMetricsPort:           9464,
				APIKey:                      "test01",
				MinConnectTimeout:           5,
			},
			authParams: map[string]string{
				"apiKey": "test01",
			},
			wantErr: false,
		},
		{
			name: "apiKey provided",
			metadata: map[string]string{
				"endpoint":  "test:7233",
				"namespace": "default",
				"taskQueue": "testxx",
				"apiKey":    "test-api-key",
			},
			wantMeta: &temporalMetadata{
				Endpoint:                    "test:7233",
				Namespace:                   "default",
				TaskQueue:                   "testxx",
				TargetQueueSize:             5,
				ActivationTargetQueueSize:   0,
				AllActive:                   false,
				Unversioned:                 false,
				IncludeRunningWorkflowCount: true,
				WorkerMetricsPort:           9464,
				APIKey:                      "test-api-key",
				MinConnectTimeout:           5,
			},
			authParams: map[string]string{
				"apiKey": "test-api-key",
			},
			wantErr: false,
		},
		{
			name: "with tlsServerName",
			metadata: map[string]string{
				"endpoint":      "test:7233",
				"namespace":     "default",
				"taskQueue":     "testxx",
				"tlsServerName": "my-namespace.tmpr.cloud",
			},
			wantMeta: &temporalMetadata{
				Endpoint:                    "test:7233",
				Namespace:                   "default",
				TaskQueue:                   "testxx",
				TargetQueueSize:             5,
				ActivationTargetQueueSize:   0,
				AllActive:                   false,
				Unversioned:                 false,
				IncludeRunningWorkflowCount: true,
				WorkerMetricsPort:           9464,
				MinConnectTimeout:           5,
				TLSServerName:               "my-namespace.tmpr.cloud",
			},
			wantErr: false,
		},
		{
			name: "with tlsServerName and apiKey",
			metadata: map[string]string{
				"endpoint":      "test:7233",
				"namespace":     "default",
				"taskQueue":     "testxx",
				"tlsServerName": "my-namespace.tmpr.cloud",
			},
			authParams: map[string]string{
				"apiKey": "test01",
			},
			wantMeta: &temporalMetadata{
				Endpoint:                    "test:7233",
				Namespace:                   "default",
				TaskQueue:                   "testxx",
				TargetQueueSize:             5,
				ActivationTargetQueueSize:   0,
				AllActive:                   false,
				Unversioned:                 false,
				IncludeRunningWorkflowCount: true,
				WorkerMetricsPort:           9464,
				APIKey:                      "test01",
				MinConnectTimeout:           5,
				TLSServerName:               "my-namespace.tmpr.cloud",
			},
			wantErr: false,
		},
		{
			name: "with tlsServerName and certificate",
			metadata: map[string]string{
				"endpoint":      "test:7233",
				"namespace":     "default",
				"taskQueue":     "testxx",
				"tlsServerName": "my-namespace.tmpr.cloud",
			},
			authParams: map[string]string{
				"cert":        "cert-data",
				"key":         "key-data",
				"keyPassword": "password",
				"ca":          "ca-data",
			},
			wantMeta: &temporalMetadata{
				Endpoint:                    "test:7233",
				Namespace:                   "default",
				TaskQueue:                   "testxx",
				TargetQueueSize:             5,
				ActivationTargetQueueSize:   0,
				AllActive:                   false,
				Unversioned:                 false,
				IncludeRunningWorkflowCount: true,
				WorkerMetricsPort:           9464,
				Cert:                        "cert-data",
				Key:                         "key-data",
				KeyPassword:                 "password",
				CA:                          "ca-data",
				MinConnectTimeout:           5,
				TLSServerName:               "my-namespace.tmpr.cloud",
			},
			wantErr: false,
		},
		{
			name: "includeRunningWorkflowCount disabled",
			metadata: map[string]string{
				"endpoint":                    "test:7233",
				"namespace":                   "default",
				"taskQueue":                   "testxx",
				"includeRunningWorkflowCount": "false",
			},
			wantMeta: &temporalMetadata{
				Endpoint:                    "test:7233",
				Namespace:                   "default",
				TaskQueue:                   "testxx",
				TargetQueueSize:             5,
				ActivationTargetQueueSize:   0,
				AllActive:                   false,
				Unversioned:                 false,
				IncludeRunningWorkflowCount: false,
				WorkerMetricsPort:           9464,
				MinConnectTimeout:           5,
			},
			wantErr: false,
		},
		{
			name: "workflowTaskQueueForCount set",
			metadata: map[string]string{
				"endpoint":                  "test:7233",
				"namespace":                 "default",
				"taskQueue":                 "activity-queue",
				"workflowTaskQueueForCount": "workflow-queue",
			},
			wantMeta: &temporalMetadata{
				Endpoint:                    "test:7233",
				Namespace:                   "default",
				TaskQueue:                   "activity-queue",
				TargetQueueSize:             5,
				ActivationTargetQueueSize:   0,
				AllActive:                   false,
				Unversioned:                 false,
				IncludeRunningWorkflowCount: true,
				WorkflowTaskQueueForCount:   "workflow-queue",
				WorkerMetricsPort:           9464,
				MinConnectTimeout:           5,
			},
			wantErr: false,
		},
	}

	for _, testCase := range cases {
		c := testCase
		t.Run(c.name, func(t *testing.T) {
			config := &scalersconfig.ScalerConfig{
				TriggerMetadata: c.metadata,
				AuthParams:      c.authParams,
				ResolvedEnv:     c.resolvedEnv,
			}
			meta, err := parseTemporalMetadata(config, logger)
			if c.wantErr == true && err != nil {
				t.Log("Expected error, got err")
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, c.wantMeta, meta)
		})
	}
}

func TestTemporalDefaultQueueTypes(t *testing.T) {
	metadata, err := parseTemporalMetadata(&scalersconfig.ScalerConfig{
		TriggerMetadata: map[string]string{
			"endpoint": "localhost:7233", "taskQueue": "testcc",
		},
	}, logger)

	assert.NoError(t, err, "error should be nil")
	assert.Empty(t, metadata.QueueTypes, "queueTypes should be empty")

	assert.Len(t, getQueueTypes(metadata.QueueTypes), 3, "all queue types should be there")

	metadata.QueueTypes = []string{"workflow"}
	assert.Len(t, getQueueTypes(metadata.QueueTypes), 1, "only one type should be there")
}

func TestParseTemporalMetadataWorkerMetricsPort(t *testing.T) {
	cases := []struct {
		name     string
		metadata map[string]string
		wantPort int
	}{
		{
			name: "default port",
			metadata: map[string]string{
				"endpoint":  "test:7233",
				"taskQueue": "q",
			},
			wantPort: 9464,
		},
		{
			name: "custom port",
			metadata: map[string]string{
				"endpoint":          "test:7233",
				"taskQueue":         "q",
				"workerMetricsPort": "8080",
			},
			wantPort: 8080,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := &scalersconfig.ScalerConfig{TriggerMetadata: tc.metadata}
			meta, err := parseTemporalMetadata(config, logger)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantPort, meta.WorkerMetricsPort)
		})
	}
}

func TestParseUsedSlots(t *testing.T) {
	// Slot scraping always filters to ActivityWorker only. WorkflowWorker slots
	// reflect the Temporal SDK sticky cache (always >= 1 while worker is alive)
	// and are not meaningful for scaling decisions.
	activityOnly := map[string]bool{"ActivityWorker": true}

	cases := []struct {
		name      string
		input     string
		taskQueue string
		want      int64
		wantErr   bool
	}{
		{
			name:      "counts only activity slots, ignores workflow and local",
			taskQueue: "atlan-cosmos-production",
			input: `# HELP temporal_worker_task_slots_used Current number of used slots per task type
# TYPE temporal_worker_task_slots_used gauge
temporal_worker_task_slots_used{namespace="default",service_name="temporal-core-sdk",task_queue="atlan-cosmos-production",worker_type="ActivityWorker"} 5
temporal_worker_task_slots_used{namespace="default",service_name="temporal-core-sdk",task_queue="atlan-cosmos-production",worker_type="WorkflowWorker"} 1
temporal_worker_task_slots_used{namespace="default",service_name="temporal-core-sdk",task_queue="atlan-cosmos-production",worker_type="LocalActivityWorker"} 0
`,
			want: 5,
		},
		{
			name:      "no matching queue",
			taskQueue: "atlan-redshift-production",
			input: `# HELP temporal_worker_task_slots_used Current number of used slots per task type
# TYPE temporal_worker_task_slots_used gauge
temporal_worker_task_slots_used{namespace="default",service_name="temporal-core-sdk",task_queue="atlan-cosmos-production",worker_type="ActivityWorker"} 5
`,
			want: 0,
		},
		{
			name:      "empty task queue matches all",
			taskQueue: "",
			input: `# HELP temporal_worker_task_slots_used Current number of used slots per task type
# TYPE temporal_worker_task_slots_used gauge
temporal_worker_task_slots_used{namespace="default",service_name="temporal-core-sdk",task_queue="atlan-cosmos-production",worker_type="ActivityWorker"} 3
temporal_worker_task_slots_used{namespace="default",service_name="temporal-core-sdk",task_queue="atlan-other-production",worker_type="ActivityWorker"} 7
`,
			want: 10,
		},
		{
			name:      "metric not present",
			taskQueue: "q",
			input: `# HELP some_other_metric A different metric
# TYPE some_other_metric gauge
some_other_metric{foo="bar"} 42
`,
			want: 0,
		},
		{
			name:      "empty input",
			taskQueue: "q",
			input:     "",
			want:      0,
		},
		{
			name:      "multiple activity queues on same worker",
			taskQueue: "atlan-cosmos-production",
			input: `# HELP temporal_worker_task_slots_used Current number of used slots per task type
# TYPE temporal_worker_task_slots_used gauge
temporal_worker_task_slots_used{namespace="default",task_queue="atlan-cosmos-production",worker_type="ActivityWorker"} 4
temporal_worker_task_slots_used{namespace="default",task_queue="atlan-cosmos-production",worker_type="WorkflowWorker"} 2
temporal_worker_task_slots_used{namespace="default",task_queue="other-queue",worker_type="ActivityWorker"} 8
`,
			want: 4,
		},
		{
			name:      "workflow sticky cache slot excluded from metric",
			taskQueue: "atlan-redshift-production",
			input: `# HELP temporal_worker_task_slots_used Current number of used slots per task type
# TYPE temporal_worker_task_slots_used gauge
temporal_worker_task_slots_used{namespace="default",task_queue="atlan-redshift-production",worker_type="ActivityWorker"} 0
temporal_worker_task_slots_used{namespace="default",task_queue="atlan-redshift-production",worker_type="WorkflowWorker"} 1
temporal_worker_task_slots_used{namespace="default",task_queue="atlan-redshift-production",worker_type="LocalActivityWorker"} 0
`,
			want: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := strings.NewReader(tc.input)
			got, err := parseUsedSlots(r, tc.taskQueue, activityOnly)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// testServerPort extracts the host and port from an httptest.Server URL.
func testServerPort(t *testing.T, srv *httptest.Server) (string, int) {
	t.Helper()
	host := strings.TrimPrefix(srv.URL, "http://")
	parts := strings.Split(host, ":")
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		t.Fatalf("failed to parse test server port: %v", err)
	}
	return parts[0], port
}

// newFakeWorkerPod creates a running pod with the worker label and given IP.
func newFakeWorkerPod(name, namespace, ip string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"app.kubernetes.io/component": "worker"},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: ip,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
}

func TestGetUsedWorkerSlotsCacheFallback(t *testing.T) {
	metricsBody := `# HELP temporal_worker_task_slots_used Current number of used slots per task type
# TYPE temporal_worker_task_slots_used gauge
temporal_worker_task_slots_used{namespace="default",task_queue="q",worker_type="ActivityWorker"} 3
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, metricsBody)
	}))

	ip, port := testServerPort(t, srv)
	ns := "test-ns"

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	pod := newFakeWorkerPod("worker-0", ns, ip)
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(pod).Build()

	s := &temporalScaler{
		metadata:     &temporalMetadata{TaskQueue: "q", WorkerMetricsPort: port},
		httpClient:   srv.Client(),
		kubeClient:   kubeClient,
		logger:       logr.Discard(),
		podNamespace: ns,
	}

	// First call: server is up, should scrape successfully and cache.
	slots, err := s.getUsedWorkerSlots(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(3), slots)
	assert.Equal(t, int64(3), s.lastSlots.value)

	// Shut down server to simulate transient failure.
	srv.Close()

	// Second call: scrape fails, should fall back to cached value.
	slots, err = s.getUsedWorkerSlots(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(3), slots)

	// Expire the cache manually, should return 0.
	s.slotsMu.Lock()
	s.lastSlots.timestamp = time.Now().Add(-slotsCacheTTL - time.Second)
	s.slotsMu.Unlock()

	slots, err = s.getUsedWorkerSlots(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(0), slots)
}

func TestGetUsedWorkerSlotsTimeout(t *testing.T) {
	// Server that takes too long to respond.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
	}))
	defer srv.Close()

	ip, port := testServerPort(t, srv)
	ns := "test-ns"

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	pod := newFakeWorkerPod("worker-0", ns, ip)
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(pod).Build()

	s := &temporalScaler{
		metadata:     &temporalMetadata{TaskQueue: "q", WorkerMetricsPort: port},
		httpClient:   srv.Client(),
		kubeClient:   kubeClient,
		logger:       logr.Discard(),
		podNamespace: ns,
	}

	// Use a very short parent context timeout to trigger the scrape budget expiry.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Should return 0 (scrape timed out, no cache).
	slots, err := s.getUsedWorkerSlots(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), slots)
}

func TestComposeMetric(t *testing.T) {
	tests := []struct {
		name           string
		backlog        int64
		runningCount   int64
		usedSlots      int64
		slotsAvailable bool
		want           int64
	}{
		{name: "idle", backlog: 0, runningCount: 0, usedSlots: 0, slotsAvailable: true, want: 0},
		{name: "ghost flicker — slots without workflow ignored", backlog: 0, runningCount: 0, usedSlots: 1, slotsAvailable: true, want: 0},
		{name: "ghost flicker (multi)", backlog: 0, runningCount: 0, usedSlots: 5, slotsAvailable: true, want: 0},
		{name: "active workflow with many activities", backlog: 0, runningCount: 1, usedSlots: 50, slotsAvailable: true, want: 51},
		{name: "active workflow + backlog + slots", backlog: 100, runningCount: 2, usedSlots: 8, slotsAvailable: true, want: 110},
		{name: "backlog only", backlog: 25, runningCount: 0, usedSlots: 0, slotsAvailable: true, want: 25},
		{name: "backlog with ghost flicker", backlog: 7, runningCount: 0, usedSlots: 1, slotsAvailable: true, want: 7},
		{name: "slots scrape failed, workflow running", backlog: 0, runningCount: 3, usedSlots: 99, slotsAvailable: false, want: 3},
		{name: "slots scrape failed, no workflow", backlog: 4, runningCount: 0, usedSlots: 99, slotsAvailable: false, want: 4},
		{name: "slots scrape failed with backlog and workflow", backlog: 10, runningCount: 2, usedSlots: 99, slotsAvailable: false, want: 12},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := composeMetric(tc.backlog, tc.runningCount, tc.usedSlots, tc.slotsAvailable)
			assert.Equal(t, tc.want, got)
		})
	}
}

