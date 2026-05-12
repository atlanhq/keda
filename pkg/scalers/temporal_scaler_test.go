package scalers

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	// workerDeploymentName without workerDeploymentBuildId
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": temporalQueueName, "namespace": temporalNamespace, "workerDeploymentName": "my-deploy"}, true},
	// workerDeploymentBuildId without workerDeploymentName
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": temporalQueueName, "namespace": temporalNamespace, "workerDeploymentBuildId": "v1"}, true},
	// the fork still accepts buildId alongside a deployment version
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": temporalQueueName, "namespace": temporalNamespace, "workerDeploymentName": "my-deploy", "workerDeploymentBuildId": "v1", "buildId": "v1"}, false},
	// the fork still accepts selectAllActive alongside a deployment version
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": temporalQueueName, "namespace": temporalNamespace, "workerDeploymentName": "my-deploy", "workerDeploymentBuildId": "v1", "selectAllActive": "true"}, false},
	// valid deployment config
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": temporalQueueName, "namespace": temporalNamespace, "workerDeploymentName": "my-deploy", "workerDeploymentBuildId": "v1"}, false},
	// legacy buildId is still accepted by the fork
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": temporalQueueName, "namespace": temporalNamespace, "buildId": "v1"}, false},
	// workflowTaskQueueForCount alone is valid: includeRunningWorkflowCount defaults to true
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": temporalQueueName, "namespace": temporalNamespace, "workflowTaskQueueForCount": "wf-queue"}, false},
	// includeRunningWorkflowCount alone is valid
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": temporalQueueName, "namespace": temporalNamespace, "includeRunningWorkflowCount": "true"}, false},
	// includeRunningWorkflowCount + workflowTaskQueueForCount is valid
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": temporalQueueName, "namespace": temporalNamespace, "includeRunningWorkflowCount": "true", "workflowTaskQueueForCount": "wf-queue"}, false},
	// includeRunningWorkflowCount rejects unsafe characters in taskQueue
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": "bad queue' OR '1'='1", "namespace": temporalNamespace, "includeRunningWorkflowCount": "true"}, true},
	// the fork still accepts selectAllActive
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": temporalQueueName, "namespace": temporalNamespace, "selectAllActive": "true"}, false},
	// the fork still accepts selectUnversioned
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": temporalQueueName, "namespace": temporalNamespace, "selectUnversioned": "true"}, false},
	// temporal-worker-controller Current/Ramping version shape
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": temporalQueueName, "namespace": temporalNamespace, "workerDeploymentName": "app-ns/app-worker-twd", "workerDeploymentBuildId": "0.4.1", "selectUnversioned": "true"}, false},
	// unsafe characters are rejected by default, since includeRunningWorkflowCount defaults to true
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": "bad queue' OR '1'='1", "namespace": temporalNamespace}, true},
	// unsafe characters never reach a visibility query when includeRunningWorkflowCount is off
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": "bad queue' OR '1'='1", "namespace": temporalNamespace, "includeRunningWorkflowCount": "false"}, false},
	// includeRunningWorkflowCount off with workflowTaskQueueForCount set is accepted (ignored)
	{map[string]string{"endpoint": temporalEndpoint, "taskQueue": temporalQueueName, "namespace": temporalNamespace, "includeRunningWorkflowCount": "false", "workflowTaskQueueForCount": "wf-queue"}, false},
}

var temporalMetricIdentifiers = []temporalMetricIdentifier{
	{&testTemporalMetadata[5], 0, "s0-temporal-v2-default"},
	{&testTemporalMetadata[5], 1, "s1-temporal-v2-default"},
	// deployment version: metric name includes workerDeploymentName + workerDeploymentBuildId
	{&testTemporalMetadata[11], 0, "s0-temporal-v2-default-my-deploy-v1"},
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
				IncludeRunningWorkflowCount: true,
				MinConnectTimeout:           5,
				EnableTLS:                   true,
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
				IncludeRunningWorkflowCount: true,
				MinConnectTimeout:           5,
				EnableTLS:                   true,
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
				IncludeRunningWorkflowCount: true,
				MinConnectTimeout:           5,
				EnableTLS:                   true,
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
				APIKey:                      "test01",
				IncludeRunningWorkflowCount: true,
				MinConnectTimeout:           5,
				EnableTLS:                   true,
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
				QueueTypes:                  []string{"workflow", "activity"},
				IncludeRunningWorkflowCount: true,
				MinConnectTimeout:           5,
				EnableTLS:                   true,
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
				APIKey:                      "test01",
				IncludeRunningWorkflowCount: true,
				MinConnectTimeout:           5,
				EnableTLS:                   true,
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
				APIKey:                      "test-api-key",
				IncludeRunningWorkflowCount: true,
				MinConnectTimeout:           5,
				EnableTLS:                   true,
			},
			authParams: map[string]string{
				"apiKey": "test-api-key",
			},
			wantErr: false,
		},
		{
			name: "apiKey with enableTLS false (plaintext gRPC)",
			metadata: map[string]string{
				"endpoint":  "test:7233",
				"namespace": "default",
				"taskQueue": "testxx",
				"enableTLS": "false",
			},
			authParams: map[string]string{
				"apiKey": "test-api-key",
			},
			wantMeta: &temporalMetadata{
				Endpoint:                    "test:7233",
				Namespace:                   "default",
				TaskQueue:                   "testxx",
				TargetQueueSize:             5,
				ActivationTargetQueueSize:   0,
				APIKey:                      "test-api-key",
				IncludeRunningWorkflowCount: true,
				MinConnectTimeout:           5,
				EnableTLS:                   false,
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
				IncludeRunningWorkflowCount: true,
				MinConnectTimeout:           5,
				EnableTLS:                   true,
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
				APIKey:                      "test01",
				IncludeRunningWorkflowCount: true,
				MinConnectTimeout:           5,
				EnableTLS:                   true,
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
				Cert:                        "cert-data",
				Key:                         "key-data",
				KeyPassword:                 "password",
				CA:                          "ca-data",
				IncludeRunningWorkflowCount: true,
				MinConnectTimeout:           5,
				EnableTLS:                   true,
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
				IncludeRunningWorkflowCount: false,
				MinConnectTimeout:           5,
				EnableTLS:                   true,
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
				IncludeRunningWorkflowCount: true,
				WorkflowTaskQueueForCount:   "workflow-queue",
				MinConnectTimeout:           5,
				EnableTLS:                   true,
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

func TestScalerMode(t *testing.T) {
	cases := []struct {
		name string
		meta temporalMetadata
		want string
	}{
		{"no versioning fields", temporalMetadata{}, "unversioned"},
		{"workerDeploymentName set", temporalMetadata{WorkerDeploymentName: "d"}, "deployment-version"},
		{"workerDeploymentName with selectUnversioned", temporalMetadata{WorkerDeploymentName: "d", Unversioned: true}, "deployment-version"},
		{"buildId set", temporalMetadata{BuildID: "v1"}, "build-id"},
		{"selectAllActive set", temporalMetadata{AllActive: true}, "build-id"},
		{"selectUnversioned set", temporalMetadata{Unversioned: true}, "build-id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, scalerMode(&tc.meta))
		})
	}
}

func generateTemporalTestCertAndKey(t *testing.T) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return string(certPEM), string(keyPEM)
}

func TestBuildTemporalTLSConfig(t *testing.T) {
	t.Run("apiKey + enableTLS=true produces non-nil TLS config", func(t *testing.T) {
		meta := &temporalMetadata{APIKey: "secret", EnableTLS: true}
		tlsCfg, err := buildTemporalTLSConfig(meta, logger)
		assert.NoError(t, err)
		assert.NotNil(t, tlsCfg)
	})
	t.Run("apiKey + enableTLS=false produces nil TLS config (plaintext)", func(t *testing.T) {
		meta := &temporalMetadata{APIKey: "secret", EnableTLS: false}
		tlsCfg, err := buildTemporalTLSConfig(meta, logger)
		assert.NoError(t, err)
		assert.Nil(t, tlsCfg)
	})
	t.Run("cert+key without apiKey always produces non-nil TLS config", func(t *testing.T) {
		certPEM, keyPEM := generateTemporalTestCertAndKey(t)
		meta := &temporalMetadata{Cert: certPEM, Key: keyPEM}
		tlsCfg, err := buildTemporalTLSConfig(meta, logger)
		assert.NoError(t, err)
		assert.NotNil(t, tlsCfg)
	})
	t.Run("apiKey + cert+key produces non-nil TLS config with client cert (mTLS + apiKey)", func(t *testing.T) {
		certPEM, keyPEM := generateTemporalTestCertAndKey(t)
		meta := &temporalMetadata{APIKey: "secret", EnableTLS: true, Cert: certPEM, Key: keyPEM}
		tlsCfg, err := buildTemporalTLSConfig(meta, logger)
		assert.NoError(t, err)
		assert.NotNil(t, tlsCfg)
		assert.NotEmpty(t, tlsCfg.Certificates, "client certificate must be present in TLS config")
	})
}

func TestAuthType(t *testing.T) {
	cases := []struct {
		name string
		meta temporalMetadata
		want string
	}{
		{"nothing set", temporalMetadata{}, "none"},
		{"apiKey set", temporalMetadata{APIKey: "secret"}, "apiKey"},
		{"cert set", temporalMetadata{Cert: "cert-data"}, "mtls"},
		{"apiKey takes precedence over cert", temporalMetadata{APIKey: "secret", Cert: "cert-data"}, "apiKey"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, authType(&tc.meta))
		})
	}
}
