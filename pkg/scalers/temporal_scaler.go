package scalers

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/metrics/pkg/apis/external_metrics"

	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
)

var (
	temporalDefaultQueueTypes = []sdk.TaskQueueType{
		sdk.TaskQueueTypeActivity,
		sdk.TaskQueueTypeWorkflow,
		sdk.TaskQueueTypeNexus,
	}

	// validVisibilityQueryLiteral matches values that are safe to interpolate into a Temporal
	// visibility query. Temporal task queue and deployment identifiers permit alphanumerics,
	// hyphens, underscores, dots, forward slashes, and colons. Rejecting anything outside this
	// set prevents query injection since the SDK offers no parameterized visibility queries.
	validVisibilityQueryLiteral = regexp.MustCompile(`^[a-zA-Z0-9\-_./:]+$`)
)

type temporalScaler struct {
	metricType v2.MetricTargetType
	metadata   *temporalMetadata
	tcl        sdk.Client
	logger     logr.Logger
}

type temporalMetadata struct {
	Endpoint                  string   `keda:"name=endpoint,                  order=triggerMetadata;resolvedEnv"`
	Namespace                 string   `keda:"name=namespace,                 order=triggerMetadata;resolvedEnv, default=default"`
	ActivationTargetQueueSize int64    `keda:"name=activationTargetQueueSize, order=triggerMetadata, default=0"`
	TargetQueueSize           int64    `keda:"name=targetQueueSize,           order=triggerMetadata, default=5"`
	TaskQueue                 string   `keda:"name=taskQueue,                 order=triggerMetadata;resolvedEnv"`
	QueueTypes                []string `keda:"name=queueTypes,                order=triggerMetadata, optional"`
	// buildId, selectAllActive and selectUnversioned are rejected upstream since v2.21. The fork
	// keeps accepting them: the temporal-worker-controller sets selectUnversioned on Current and
	// Ramping versions, and getQueueSize forwards all three to DescribeTaskQueueEnhanced.
	BuildID                 string `keda:"name=buildId,                   order=triggerMetadata;resolvedEnv, optional"`
	WorkerDeploymentName    string `keda:"name=workerDeploymentName,      order=triggerMetadata;resolvedEnv, optional"`
	WorkerDeploymentBuildID string `keda:"name=workerDeploymentBuildId,   order=triggerMetadata;resolvedEnv, optional"`
	AllActive               bool   `keda:"name=selectAllActive,           order=triggerMetadata, default=false"`
	Unversioned             bool   `keda:"name=selectUnversioned,         order=triggerMetadata, default=false"`
	// IncludeRunningWorkflowCount adds the running workflow count on the task queue (or on
	// WorkflowTaskQueueForCount) to the reported metric, so workers are not scaled away while
	// workflows are still executing and the backlog is momentarily empty.
	IncludeRunningWorkflowCount bool   `keda:"name=includeRunningWorkflowCount, order=triggerMetadata, default=true"`
	WorkflowTaskQueueForCount   string `keda:"name=workflowTaskQueueForCount,   order=triggerMetadata;resolvedEnv, optional"`
	APIKey                      string `keda:"name=apiKey,                    order=authParams;resolvedEnv, optional"`
	MinConnectTimeout           int    `keda:"name=minConnectTimeout,         order=triggerMetadata, default=5"`
	EnableTLS                   bool   `keda:"name=enableTLS,                 order=triggerMetadata, default=true"`

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

	if (a.WorkerDeploymentName == "") != (a.WorkerDeploymentBuildID == "") {
		return fmt.Errorf("workerDeploymentName and workerDeploymentBuildId must both be set")
	}

	if a.IncludeRunningWorkflowCount {
		for _, name := range []string{a.TaskQueue, a.WorkflowTaskQueueForCount, a.WorkerDeploymentName, a.WorkerDeploymentBuildID} {
			if name != "" && !validVisibilityQueryLiteral.MatchString(name) {
				return fmt.Errorf("value %q contains characters not allowed in Temporal visibility queries when includeRunningWorkflowCount is enabled", name)
			}
		}
	}

	return nil
}

func NewTemporalScaler(ctx context.Context, config *scalersconfig.ScalerConfig) (Scaler, error) {
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

	kv := []any{
		"mode", scalerMode(meta),
		"endpoint", meta.Endpoint,
		"namespace", meta.Namespace,
		"taskQueue", meta.TaskQueue,
		"targetQueueSize", meta.TargetQueueSize,
		"activationTargetQueueSize", meta.ActivationTargetQueueSize,
		"includeRunningWorkflowCount", meta.IncludeRunningWorkflowCount,
		"authType", authType(meta),
		"unsafeSsl", meta.UnsafeSsl,
	}
	if meta.TLSServerName != "" {
		kv = append(kv, "tlsServerName", meta.TLSServerName)
	}
	if meta.WorkerDeploymentName != "" {
		kv = append(kv, "workerDeploymentName", meta.WorkerDeploymentName, "workerDeploymentBuildId", meta.WorkerDeploymentBuildID)
	}
	if meta.BuildID != "" || meta.AllActive || meta.Unversioned {
		kv = append(kv, "buildId", meta.BuildID, "selectAllActive", meta.AllActive, "selectUnversioned", meta.Unversioned)
	}
	if len(meta.QueueTypes) > 0 {
		kv = append(kv, "queueTypes", meta.QueueTypes)
	}
	logger.Info("Temporal scaler initialized", kv...)

	return &temporalScaler{
		metricType: metricType,
		metadata:   meta,
		tcl:        c,
		logger:     logger,
	}, nil
}

func (s *temporalScaler) Close(_ context.Context) error {
	s.logger.V(1).Info("closing Temporal scaler")
	if s.tcl != nil {
		s.tcl.Close()
	}
	return nil
}

func (s *temporalScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {
	metricName := fmt.Sprintf("temporal-%s-%s", s.metadata.Namespace, s.metadata.TaskQueue)
	if s.metadata.WorkerDeploymentName != "" {
		metricName = fmt.Sprintf("%s-%s-%s", metricName, s.metadata.WorkerDeploymentName, s.metadata.WorkerDeploymentBuildID)
	}
	metricName = kedautil.NormalizeString(metricName)

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
		s.logger.Error(err, "failed to get Temporal queue size",
			"mode", scalerMode(s.metadata),
			"namespace", s.metadata.Namespace,
			"taskQueue", s.metadata.TaskQueue,
			"grpcCode", status.Code(err).String())
		return nil, false, fmt.Errorf("failed to get Temporal queue size: %w", err)
	}

	metric := GenerateMetricInMili(metricName, float64(queueSize))
	isActive := queueSize > s.metadata.ActivationTargetQueueSize

	s.logger.V(1).Info("polled Temporal queue size",
		"mode", scalerMode(s.metadata),
		"namespace", s.metadata.Namespace,
		"taskQueue", s.metadata.TaskQueue,
		"workerDeploymentName", s.metadata.WorkerDeploymentName,
		"workerDeploymentBuildId", s.metadata.WorkerDeploymentBuildID,
		"queueSize", queueSize,
		"activationThreshold", s.metadata.ActivationTargetQueueSize,
		"isActive", isActive)

	return []external_metrics.ExternalMetricValue{metric}, isActive, nil
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

	if !s.metadata.IncludeRunningWorkflowCount {
		return backlog, nil
	}

	runningCount, err := s.getRunningWorkflowCount(ctx)
	if err != nil {
		s.logger.V(1).Info("failed to get running workflow count, using backlog only", "error", err)
		return backlog, nil
	}

	return backlog + runningCount, nil
}

// getRunningWorkflowCount returns the approximate number of running workflow executions
// for the task queue (or workflowTaskQueueForCount if set). Used to avoid premature
// scale-down when workers are fast and backlog is often zero.
func (s *temporalScaler) getRunningWorkflowCount(ctx context.Context) (int64, error) {
	taskQueue := s.metadata.WorkflowTaskQueueForCount
	if taskQueue == "" {
		taskQueue = s.metadata.TaskQueue
	}
	// Escape single quotes in task queue name for visibility query
	escaped := strings.ReplaceAll(taskQueue, "'", "''")
	query := fmt.Sprintf("ExecutionStatus = 'Running' AND TaskQueue = '%s'", escaped)

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

func scalerMode(m *temporalMetadata) string {
	switch {
	case m.WorkerDeploymentName != "":
		return "deployment-version"
	case m.BuildID != "" || m.AllActive || m.Unversioned:
		return "build-id"
	default:
		return "unversioned"
	}
}

func authType(m *temporalMetadata) string {
	switch {
	case m.APIKey != "":
		return "apiKey"
	case m.Cert != "":
		return "mtls"
	default:
		return "none"
	}
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
		return temporalDefaultQueueTypes
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

	if meta.APIKey != "" {
		options.Credentials = sdk.NewAPIKeyStaticCredentials(meta.APIKey)
	}

	tlsConfig, err := buildTemporalTLSConfig(meta, log)
	if err != nil {
		return nil, err
	}

	options.ConnectionOptions = sdk.ConnectionOptions{
		DialOptions: dialOptions,
		TLS:         tlsConfig,
	}

	return sdk.DialContext(ctx, options)
}

func buildTemporalTLSConfig(meta *temporalMetadata, log logr.Logger) (*tls.Config, error) {
	// cert+key always uses TLS (mTLS); enableTLS only controls apiKey-only connections.
	// When both apiKey and cert+key are set (e.g. Temporal Cloud), the client certificate
	// is included - restoring the layered behavior from before this function was extracted.
	if meta.Cert != "" && meta.Key != "" {
		if !meta.EnableTLS {
			log.V(0).Info("enableTLS is ignored when cert/key are provided; mTLS requires TLS")
		}
		tlsConfig, err := kedautil.NewTLSConfigWithPassword(meta.Cert, meta.Key, meta.KeyPassword, meta.CA, meta.UnsafeSsl)
		if err != nil {
			return nil, err
		}
		if meta.TLSServerName != "" {
			tlsConfig.ServerName = meta.TLSServerName
		}
		return tlsConfig, nil
	}

	// apiKey without cert+key: TLS gated by enableTLS
	if meta.APIKey != "" {
		if !meta.EnableTLS {
			log.V(0).Info("WARNING: enableTLS is false while apiKey is set; API key will be transmitted in cleartext - only use this in trusted network environments")
			return nil, nil
		}
		tlsConfig := kedautil.CreateTLSClientConfig(meta.UnsafeSsl)
		if meta.TLSServerName != "" {
			tlsConfig.ServerName = meta.TLSServerName
		}
		return tlsConfig, nil
	}

	return nil, nil
}

func parseTemporalMetadata(config *scalersconfig.ScalerConfig, _ logr.Logger) (*temporalMetadata, error) {
	meta := &temporalMetadata{triggerIndex: config.TriggerIndex}
	if err := config.TypedConfig(meta); err != nil {
		return meta, fmt.Errorf("error parsing temporal metadata: %w", err)
	}

	return meta, nil
}
