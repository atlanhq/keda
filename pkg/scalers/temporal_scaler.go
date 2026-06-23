package scalers

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	v2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/metrics/pkg/apis/external_metrics"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
)

const (
	// scrapeLoopTimeout is the total time budget for scraping all worker pods.
	scrapeLoopTimeout = 12 * time.Second
	// slotsCacheTTL is how long a cached slots value remains valid after a
	// successful scrape before falling back to 0 on persistent failure.
	slotsCacheTTL = 180 * time.Second
	// maxMetricsResponseBytes limits the size of a single pod's /metrics response
	// to prevent OOM from misconfigured or malicious pods.
	maxMetricsResponseBytes = 10 * 1024 * 1024 // 10 MB
)

var (
	temporalDefauleQueueTypes = []sdk.TaskQueueType{
		sdk.TaskQueueTypeActivity,
		sdk.TaskQueueTypeWorkflow,
		sdk.TaskQueueTypeNexus,
	}

	// temporalSlotsScrapeErrors counts worker slot scrape failures by reason:
	//   pod_scrape_error              – a single pod's /metrics request failed
	//   scrape_loop_timeout           – 12s budget exceeded, used partial results
	//   all_pods_failed_cache_hit     – all pods failed; returned last cached value
	//   all_pods_failed_cache_expired – all pods failed and cache expired; returned 0
	temporalSlotsScrapeErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "keda",
			Subsystem: "temporal_scaler",
			Name:      "worker_slots_scrape_errors_total",
			Help: "Total number of temporal worker slot scrape failures. " +
				"Use reason label to distinguish pod-level errors from full-scrape failures.",
		},
		[]string{"namespace", "task_queue", "reason"},
	)
)

func init() {
	ctrlmetrics.Registry.MustRegister(temporalSlotsScrapeErrors)
}

type slotsCache struct {
	value     int64
	timestamp time.Time
}

type temporalScaler struct {
	metricType   v2.MetricTargetType
	metadata     *temporalMetadata
	tcl          sdk.Client
	kubeClient   client.Client
	httpClient   *http.Client
	logger       logr.Logger
	podNamespace string
	slotsMu      sync.Mutex
	lastSlots    slotsCache
}

type temporalMetadata struct {
	Endpoint                    string   `keda:"name=endpoint,                      order=triggerMetadata;resolvedEnv"`
	Namespace                   string   `keda:"name=namespace,                 order=triggerMetadata;resolvedEnv, default=default"`
	ActivationTargetQueueSize   int64    `keda:"name=activationTargetQueueSize, order=triggerMetadata, default=0"`
	TargetQueueSize             int64    `keda:"name=targetQueueSize,           order=triggerMetadata, default=5"`
	TaskQueue                   string   `keda:"name=taskQueue,                 order=triggerMetadata;resolvedEnv"`
	QueueTypes                  []string `keda:"name=queueTypes,                order=triggerMetadata, optional"`
	BuildID                     string   `keda:"name=buildId,                   order=triggerMetadata;resolvedEnv, optional"`
	WorkerDeploymentName        string   `keda:"name=workerDeploymentName,      order=triggerMetadata;resolvedEnv, optional"`
	WorkerDeploymentBuildID     string   `keda:"name=workerDeploymentBuildId,   order=triggerMetadata;resolvedEnv, optional"`
	AllActive                   bool     `keda:"name=selectAllActive,           order=triggerMetadata, default=false"`
	Unversioned                 bool     `keda:"name=selectUnversioned,         order=triggerMetadata, default=false"`
	IncludeRunningWorkflowCount bool     `keda:"name=includeRunningWorkflowCount, order=triggerMetadata, default=true"`
	WorkflowTaskQueueForCount   string   `keda:"name=workflowTaskQueueForCount,   order=triggerMetadata;resolvedEnv, optional"`
	WorkerMetricsPort           int      `keda:"name=workerMetricsPort,           order=triggerMetadata, default=9464"`
	GateSlotsOnRunningWorkflow  bool     `keda:"name=gateSlotsOnRunningWorkflow,  order=triggerMetadata, default=true"`
	APIKey                      string   `keda:"name=apiKey,                    order=authParams;resolvedEnv, optional"`
	MinConnectTimeout           int      `keda:"name=minConnectTimeout,         order=triggerMetadata, default=5"`

	UnsafeSsl     bool   `keda:"name=unsafeSsl,                 order=triggerMetadata, optional"`
	Cert          string `keda:"name=cert,                      order=authParams, optional"`
	Key           string `keda:"name=key,                       order=authParams, optional"`
	KeyPassword   string `keda:"name=keyPassword,               order=authParams, optional"`
	CA            string `keda:"name=ca,                        order=authParams, optional"`
	TLSServerName string `keda:"name=tlsServerName,             order=triggerMetadata, optional"`

	triggerIndex int
}

func (a *temporalMetadata) Validate() error {
	if a.TargetQueueSize < 0 {
		return fmt.Errorf("targetQueueSize must be a positive number")
	}
	if a.ActivationTargetQueueSize < 0 {
		return fmt.Errorf("activationTargetQueueSize must be a positive number")
	}

	if (a.Cert == "") != (a.Key == "") {
		return fmt.Errorf("both cert and key must be provided when using TLS")
	}

	if a.MinConnectTimeout < 0 {
		return fmt.Errorf("minConnectTimeout must be a positive number")
	}

	if a.WorkerMetricsPort < 1 || a.WorkerMetricsPort > 65535 {
		return fmt.Errorf("workerMetricsPort must be between 1 and 65535")
	}

	return nil
}

func NewTemporalScaler(ctx context.Context, kubeClient client.Client, config *scalersconfig.ScalerConfig) (Scaler, error) {
	logger := InitializeLogger(config, "temporal_scaler")

	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, fmt.Errorf("failed to get scaler metric type: %w", err)
	}

	meta, err := parseTemporalMetadata(config, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Temporal metadata: %w", err)
	}

	c, err := getTemporalClient(ctx, meta, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Temporal client connection: %w", err)
	}

	return &temporalScaler{
		metricType:   metricType,
		metadata:     meta,
		tcl:          c,
		kubeClient:   kubeClient,
		httpClient:   kedautil.CreateHTTPClient(config.GlobalHTTPTimeout, false),
		logger:       logger,
		podNamespace: config.ScalableObjectNamespace,
	}, nil
}

func (s *temporalScaler) Close(_ context.Context) error {
	if s.tcl != nil {
		s.tcl.Close()
	}
	return nil
}

func (s *temporalScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {
	metricName := kedautil.NormalizeString(fmt.Sprintf("temporal-%s-%s", s.metadata.Namespace, s.metadata.TaskQueue))
	externalMetric := &v2.ExternalMetricSource{
		Metric: v2.MetricIdentifier{
			Name: GenerateMetricNameWithIndex(s.metadata.triggerIndex, metricName),
		},
		Target: GetMetricTarget(s.metricType, s.metadata.TargetQueueSize),
	}

	metricSpec := v2.MetricSpec{
		External: externalMetric,
		Type:     externalMetricType,
	}

	return []v2.MetricSpec{metricSpec}
}

func (s *temporalScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	queueSize, err := s.getQueueSize(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get Temporal queue size: %w", err)
	}

	metric := GenerateMetricInMili(metricName, float64(queueSize))

	return []external_metrics.ExternalMetricValue{metric}, queueSize > s.metadata.ActivationTargetQueueSize, nil
}

func (s *temporalScaler) getQueueSize(ctx context.Context) (int64, error) {
	var selection *sdk.TaskQueueVersionSelection
	if s.metadata.AllActive || s.metadata.Unversioned || s.metadata.BuildID != "" {
		selection = &sdk.TaskQueueVersionSelection{
			AllActive:   s.metadata.AllActive,
			Unversioned: s.metadata.Unversioned,
			BuildIDs:    []string{s.metadata.BuildID},
		}
	}

	queueType := getQueueTypes(s.metadata.QueueTypes)

	resp, err := s.tcl.DescribeTaskQueueEnhanced(ctx, sdk.DescribeTaskQueueEnhancedOptions{
		TaskQueue:      s.metadata.TaskQueue,
		ReportStats:    true,
		Versions:       selection,
		TaskQueueTypes: queueType,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get Temporal queue size: %w", err)
	}

	backlog := getCombinedBacklogCount(resp)

	var runningCount int64
	if s.metadata.IncludeRunningWorkflowCount {
		var err error
		runningCount, err = s.getRunningWorkflowCount(ctx)
		if err != nil {
			s.logger.V(1).Info("failed to get running workflow count, using backlog only", "error", err)
			runningCount = 0
		}
	}

	usedSlots, slotsErr := s.getUsedWorkerSlots(ctx)
	if slotsErr != nil {
		s.logger.Info("failed to get worker slots metric, excluding from metric", "error", slotsErr)
	}

	return composeMetric(backlog, runningCount, usedSlots, slotsErr == nil, s.metadata.GateSlotsOnRunningWorkflow), nil
}

// composeMetric returns the composite scaling metric: backlog + runningWorkflow
// count + used worker slots, with the slots term conditionally gated.
//
// gateSlotsOnRunningWorkflow (default true) is the combined-pool behaviour: a
// worker shares a task queue with the workflows that schedule its activities,
// so a running workflow on the queue is a reliable signal that any reported
// used-slots are real. Gating on runningCount > 0 suppresses the SDK
// ghost-flicker false positive — when no workflow is running, non-empty poll
// responses lacking activity_type still mark a slot used (see sdk-core
// pollers/mod.rs:162-170), which would keep cooldown reset forever and block
// scale-to-zero.
//
// gateSlotsOnRunningWorkflow=false is for a DEDICATED activity pool: it polls
// an activity-only task queue, so no workflow ever runs on it and runningCount
// is structurally 0 — the gate above would discard the used-slots signal
// entirely and let KEDA scale the pool to zero while a long activity is still
// executing on it (deleting the busy pod). With the gate off, used-slots
// (already scoped to this pool's ActivityWorker + task queue) drives the
// metric directly: non-zero exactly while a heavy activity runs, and 0 the
// moment it finishes — so the pool stays warm under load and still scales to
// zero when idle, even while the parent workflow keeps running on another
// queue. Trade-off: a genuinely idle pool exhibiting ghost-flicker can stay up
// one extra cooldown window; that over-provisions rather than killing a busy
// pod, and is the conservative direction.
func composeMetric(backlog, runningCount, usedSlots int64, slotsAvailable, gateSlotsOnRunningWorkflow bool) int64 {
	metric := backlog + runningCount
	if slotsAvailable && (!gateSlotsOnRunningWorkflow || runningCount > 0) {
		metric += usedSlots
	}
	return metric
}

// buildRunningCountQuery composes the visibility query used to count
// running workflows. Pulled out as a pure function so the query-string logic
// is independently unit-testable.
//
// Three modes, in priority order:
//
//   1. workerDeploymentName + workerDeploymentBuildID set (RECOMMENDED, for
//      Temporal's worker-deployment-versioning model): use the canonical,
//      single-valued
//      `TemporalWorkerDeploymentVersion = '<deployment-name>:<buildId>'`
//      search attribute. Set automatically by Temporal as the current routing
//      assignment regardless of versioning behavior (Pinned/AutoUpgrade).
//      Field names match upstream KEDA v2.20.1's metadata schema so this
//      branch can switch to upstream without callers re-emitting metadata.
//
//   2. buildID set, workerDeploymentName empty (LEGACY, for older
//      worker-versioning-rules model): falls back to
//      `BuildIds = 'versioned:<buildId>'`. Doesn't match workflows pinned
//      via worker-deployment versioning (the assignment marker for those is
//      `pinned:<dep>:<buildId>`, not `versioned:<buildId>`). Kept for
//      backward compatibility with deployments still on the older versioning
//      model.
//
//   3. Neither set: task-queue-wide count, no version scoping.
func buildRunningCountQuery(taskQueue, workerDeploymentName, workerDeploymentBuildID, buildID string) string {
	escapedTQ := strings.ReplaceAll(taskQueue, "'", "''")
	query := fmt.Sprintf("ExecutionStatus = 'Running' AND TaskQueue = '%s'", escapedTQ)
	switch {
	case workerDeploymentName != "" && workerDeploymentBuildID != "":
		// Canonical worker-deployment-versioning query. Uses the single-valued
		// TemporalWorkerDeploymentVersion attribute set automatically by
		// Temporal — uniform across Pinned + AutoUpgrade behaviors.
		escapedDep := strings.ReplaceAll(workerDeploymentName, "'", "''")
		escapedBID := strings.ReplaceAll(workerDeploymentBuildID, "'", "''")
		query = fmt.Sprintf("%s AND TemporalWorkerDeploymentVersion = '%s:%s'",
			query, escapedDep, escapedBID)
	case buildID != "":
		// Legacy worker-versioning-rules path.
		escapedBID := strings.ReplaceAll(buildID, "'", "''")
		query = fmt.Sprintf("%s AND BuildIds = 'versioned:%s'", query, escapedBID)
	}
	return query
}

// getRunningWorkflowCount returns the approximate number of running workflow executions
// for the task queue (or workflowTaskQueueForCount if set). Used to avoid premature
// scale-down when workers are fast and backlog is often zero.
func (s *temporalScaler) getRunningWorkflowCount(ctx context.Context) (int64, error) {
	taskQueue := s.metadata.WorkflowTaskQueueForCount
	if taskQueue == "" {
		taskQueue = s.metadata.TaskQueue
	}
	query := buildRunningCountQuery(taskQueue, s.metadata.WorkerDeploymentName, s.metadata.WorkerDeploymentBuildID, s.metadata.BuildID)

	req := &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: s.metadata.Namespace,
		Query:     query,
	}
	resp, err := s.tcl.CountWorkflow(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("count workflow: %w", err)
	}
	return resp.GetCount(), nil
}

// getUsedWorkerSlots discovers worker pods in the ScaledObject's namespace and
// scrapes their Prometheus metrics endpoint to sum temporal_worker_task_slots_used
// for worker types matching the configured queueTypes. This prevents premature
// scale-down when workers are actively executing tasks but the task queue backlog
// is empty.
//
// When the scaler is configured for a specific worker version (workerDeploymentBuildId
// or the legacy buildId), the pod listing is filtered by the upstream
// `temporal.io/build-id` label so that a per-version ScaledObject only sums slots
// from its own pods. Without this filter, multiple per-version SOs sharing a
// namespace would each see every version's pods and double-count the slots term,
// keeping all versions warm whenever any one of them is busy. When neither buildID
// field is set (unversioned worker), no build-id filter is applied.
//
// On transient failures (all pod scrapes fail), it returns the last known good
// value if within the cache TTL. A total timeout budget bounds the scrape loop
// so that slow/unreachable pods don't block the KEDA polling cycle.
func (s *temporalScaler) getUsedWorkerSlots(ctx context.Context) (int64, error) {
	if s.kubeClient == nil || s.httpClient == nil {
		return 0, fmt.Errorf("kubernetes client or http client not configured")
	}

	podList := &corev1.PodList{}
	labelSelector := client.MatchingLabels{"app.kubernetes.io/component": "worker"}
	if buildID := s.workerBuildID(); buildID != "" {
		labelSelector["temporal.io/build-id"] = buildID
	}
	if err := s.kubeClient.List(ctx, podList, client.InNamespace(s.podNamespace), labelSelector); err != nil {
		return 0, fmt.Errorf("failed to list worker pods in namespace %s: %w", s.podNamespace, err)
	}

	if len(podList.Items) == 0 {
		return 0, nil
	}

	// Apply a timeout budget for the entire scrape loop.
	scrapeCtx, cancel := context.WithTimeout(ctx, scrapeLoopTimeout)
	defer cancel()

	var totalUsedSlots int64
	var scrapedCount, attemptedCount int
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" || !isPodReady(pod) {
			continue
		}

		// Stop scraping if we've exceeded the timeout budget.
		if scrapeCtx.Err() != nil {
			s.logger.Info("scrape loop timeout reached, using partial results",
				"scraped", scrapedCount, "remaining", len(podList.Items)-i)
			temporalSlotsScrapeErrors.WithLabelValues(s.podNamespace, s.metadata.TaskQueue, "scrape_loop_timeout").Inc()
			break
		}

		attemptedCount++
		slots, err := s.scrapeWorkerSlots(scrapeCtx, pod.Status.PodIP)
		if err != nil {
			s.logger.Info("failed to scrape worker pod metrics, skipping",
				"pod", pod.Name, "ip", pod.Status.PodIP, "error", err)
			temporalSlotsScrapeErrors.WithLabelValues(s.podNamespace, s.metadata.TaskQueue, "pod_scrape_error").Inc()
			continue
		}
		totalUsedSlots += slots
		scrapedCount++
	}

	s.logger.V(1).Info("worker slots metric",
		"namespace", s.podNamespace, "totalUsedSlots", totalUsedSlots,
		"podCount", len(podList.Items), "scrapedCount", scrapedCount)

	// No ready pods to scrape (e.g. all pods still starting up) — return 0.
	if attemptedCount == 0 {
		return 0, nil
	}

	// All attempted scrapes failed — fall back to cached value within TTL.
	if scrapedCount == 0 {
		s.slotsMu.Lock()
		cached := s.lastSlots
		s.slotsMu.Unlock()
		if time.Since(cached.timestamp) <= slotsCacheTTL {
			s.logger.Info("all scrapes failed, using cached slots value",
				"cachedValue", cached.value, "cacheAge", time.Since(cached.timestamp).String())
			temporalSlotsScrapeErrors.WithLabelValues(s.podNamespace, s.metadata.TaskQueue, "all_pods_failed_cache_hit").Inc()
			return cached.value, nil
		}
		s.logger.Info("all scrapes failed and cache expired, returning 0")
		temporalSlotsScrapeErrors.WithLabelValues(s.podNamespace, s.metadata.TaskQueue, "all_pods_failed_cache_expired").Inc()
		return 0, nil
	}

	// Update cache with the fresh value.
	s.slotsMu.Lock()
	s.lastSlots = slotsCache{value: totalUsedSlots, timestamp: time.Now()}
	s.slotsMu.Unlock()

	return totalUsedSlots, nil
}

// workerBuildID returns the build ID used to scope the pod listing for slot
// scraping. Prefers the new workerDeploymentBuildId field (worker-deployment
// versioning model), falling back to the legacy buildId (worker-versioning-rules
// model). Returns "" for unversioned workers, in which case no build-id filter
// is applied.
func (s *temporalScaler) workerBuildID() string {
	if s.metadata.WorkerDeploymentBuildID != "" {
		return s.metadata.WorkerDeploymentBuildID
	}
	return s.metadata.BuildID
}

// scrapeWorkerSlots fetches Prometheus metrics from a single worker pod and returns
// the sum of temporal_worker_task_slots_used for worker types matching the
// configured queueTypes and task queue.
func (s *temporalScaler) scrapeWorkerSlots(ctx context.Context, podIP string) (int64, error) {
	hostPort := net.JoinHostPort(podIP, strconv.Itoa(s.metadata.WorkerMetricsPort))
	url := fmt.Sprintf("http://%s/metrics", hostPort)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("scrape %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("scrape %s returned status %d", url, resp.StatusCode)
	}

	limitedBody := io.LimitReader(resp.Body, maxMetricsResponseBytes)
	activityOnly := map[string]bool{"ActivityWorker": true}
	return parseUsedSlots(limitedBody, s.metadata.TaskQueue, activityOnly)
}

// parseUsedSlots parses Prometheus text format and extracts the sum of
// temporal_worker_task_slots_used for the given worker types matching the task queue.
func parseUsedSlots(r io.Reader, taskQueue string, workerTypes map[string]bool) (int64, error) {
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return 0, fmt.Errorf("parse prometheus metrics: %w", err)
	}

	family, ok := families["temporal_worker_task_slots_used"]
	if !ok {
		return 0, nil
	}

	var total int64
	for _, m := range family.GetMetric() {
		if matchesWorkerSlot(m, taskQueue, workerTypes) {
			total += int64(m.GetGauge().GetValue())
		}
	}
	return total, nil
}

// matchesWorkerSlot returns true if the metric's worker_type is in the allowed set
// and (if taskQueue is non-empty) task_queue matches the configured queue.
func matchesWorkerSlot(m *dto.Metric, taskQueue string, workerTypes map[string]bool) bool {
	var typeMatches bool
	queueMatches := taskQueue == ""
	for _, lp := range m.GetLabel() {
		switch lp.GetName() {
		case "worker_type":
			typeMatches = workerTypes[lp.GetValue()]
		case "task_queue":
			if !queueMatches {
				queueMatches = lp.GetValue() == taskQueue
			}
		}
	}
	return typeMatches && queueMatches
}

func isPodReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func getQueueTypes(queueTypes []string) []sdk.TaskQueueType {
	var taskQueueTypes []sdk.TaskQueueType
	for _, t := range queueTypes {
		var taskQueueType sdk.TaskQueueType
		switch t {
		case "workflow":
			taskQueueType = sdk.TaskQueueTypeWorkflow
		case "activity":
			taskQueueType = sdk.TaskQueueTypeActivity
		case "nexus":
			taskQueueType = sdk.TaskQueueTypeNexus
		}
		taskQueueTypes = append(taskQueueTypes, taskQueueType)
	}

	if len(taskQueueTypes) == 0 {
		return temporalDefauleQueueTypes
	}
	return taskQueueTypes
}

func getCombinedBacklogCount(description sdk.TaskQueueDescription) int64 {
	var count int64

	for _, versionInfo := range description.VersionsInfo {
		for _, typeInfo := range versionInfo.TypesInfo {
			if typeInfo.Stats != nil {
				count += typeInfo.Stats.ApproximateBacklogCount
			}
		}
	}
	return count
}

func getTemporalClient(ctx context.Context, meta *temporalMetadata, log logr.Logger) (sdk.Client, error) {
	logHandler := logr.ToSlogHandler(log)
	options := sdk.Options{
		HostPort:  meta.Endpoint,
		Namespace: meta.Namespace,
		Logger:    sdklog.NewStructuredLogger(slog.New(logHandler)),
	}

	dialOptions := []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{
			MinConnectTimeout: time.Duration(meta.MinConnectTimeout) * time.Second,
		}),
	}

	dialOptions = append(dialOptions, grpc.WithUnaryInterceptor(
		func(ctx context.Context, method string, req any, reply any,
			cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
		) error {
			return invoker(
				metadata.AppendToOutgoingContext(ctx, "temporal-namespace", meta.Namespace),
				method,
				req,
				reply,
				cc,
				opts...,
			)
		},
	))

	var tlsConfig *tls.Config

	if meta.APIKey != "" {
		options.Credentials = sdk.NewAPIKeyStaticCredentials(meta.APIKey)
		tlsConfig = kedautil.CreateTLSClientConfig(meta.UnsafeSsl)
	}

	if meta.Cert != "" && meta.Key != "" {
		var err error
		tlsConfig, err = kedautil.NewTLSConfigWithPassword(meta.Cert, meta.Key, meta.KeyPassword, meta.CA, meta.UnsafeSsl)
		if err != nil {
			return nil, err
		}
	}

	if tlsConfig != nil && meta.TLSServerName != "" {
		tlsConfig.ServerName = meta.TLSServerName
	}

	options.ConnectionOptions = sdk.ConnectionOptions{
		DialOptions: dialOptions,
		TLS:         tlsConfig,
	}

	return sdk.DialContext(ctx, options)
}

func parseTemporalMetadata(config *scalersconfig.ScalerConfig, _ logr.Logger) (*temporalMetadata, error) {
	meta := &temporalMetadata{triggerIndex: config.TriggerIndex}
	if err := config.TypedConfig(meta); err != nil {
		return meta, fmt.Errorf("error parsing temporal metadata: %w", err)
	}

	return meta, nil
}
