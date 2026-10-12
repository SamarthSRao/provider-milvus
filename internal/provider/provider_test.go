package provider

import (
	"context"
	"encoding/json"
	"testing"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/definition/components"
	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

func configParams(t *testing.T, cfg string) *runtime.RawExtension {
	t.Helper()
	raw, err := json.Marshal(components.MilvusParameters{Configuration: cfg})
	require.NoError(t, err)
	return &runtime.RawExtension{Raw: raw}
}

func newTestContext(t *testing.T, spec corev1alpha1.InstanceSpec) *controller.Context {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, milvusapi.AddToScheme(scheme))
	require.NoError(t, monitoringv1.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Spec:       spec,
	}
	provider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: common.ProviderName},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, provider).Build()
	return controller.NewContext(context.Background(), fakeClient, instance, common.ProviderName)
}

func TestMilvusEngineConfig(t *testing.T) {
	tests := []struct {
		name       string
		components map[string]corev1alpha1.ComponentSpec
		want       milvusapi.Values
		wantErr    string
	}{
		{
			name:       "no components",
			components: map[string]corev1alpha1.ComponentSpec{},
			want:       nil,
		},
		{
			name: "component without configuration",
			components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentStandalone: {},
			},
			want: nil,
		},
		{
			name: "single component configuration",
			components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentStandalone: {
					Parameters: configParams(t, "log:\n  level: debug\n"),
				},
			},
			want: milvusapi.Values{
				"log": map[string]any{"level": "debug"},
			},
		},
		{
			name: "deep merge across components",
			components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentProxy: {
					Parameters: configParams(t, "proxy:\n  maxNameLength: 255\n"),
				},
				common.ComponentQueryNode: {
					Parameters: configParams(t, "queryNode:\n  gracefulTime: 5000\n"),
				},
			},
			want: milvusapi.Values{
				"proxy":     map[string]any{"maxNameLength": float64(255)},
				"queryNode": map[string]any{"gracefulTime": float64(5000)},
			},
		},
		{
			name: "nested keys under same section are merged",
			components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentMixCoord: {
					Parameters: configParams(t, "dataCoord:\n  segment:\n    maxSize: 1024\n"),
				},
				common.ComponentDataNode: {
					Parameters: configParams(t, "dataCoord:\n  enableCompaction: true\n"),
				},
			},
			want: milvusapi.Values{
				"dataCoord": map[string]any{
					"segment":          map[string]any{"maxSize": float64(1024)},
					"enableCompaction": true,
				},
			},
		},
		{
			name: "invalid yaml returns error",
			components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentStandalone: {
					Parameters: configParams(t, "log: [unclosed"),
				},
			},
			wantErr: "invalid configuration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestContext(t, corev1alpha1.InstanceSpec{Components: tt.components})
			got, err := milvusEngineConfig(c)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, map[string]any(tt.want), map[string]any(got))
		})
	}
}

func TestBuildMilvusSpecConfiguration(t *testing.T) {
	t.Run("standalone config lands in spec.config", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentStandalone: {
					Parameters: configParams(t, "log:\n  level: info\n"),
				},
			},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"level": "info"}, spec.Conf["log"])
	})

	t.Run("cluster merges config from multiple components", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentProxy: {
					Parameters: configParams(t, "proxy:\n  maxNameLength: 255\n"),
				},
				common.ComponentQueryNode: {
					Parameters: configParams(t, "queryNode:\n  gracefulTime: 5000\n"),
				},
			},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"maxNameLength": float64(255)}, spec.Conf["proxy"])
		assert.Equal(t, map[string]any{"gracefulTime": float64(5000)}, spec.Conf["queryNode"])
	})

	t.Run("no configuration leaves spec.config empty", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentStandalone: {},
			},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		assert.Nil(t, spec.Conf)
	})
}

func TestBuildMilvusSpecClusterComponents(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
		Components: map[string]corev1alpha1.ComponentSpec{
			common.ComponentProxy:    {Replicas: ptr.To(int32(3))},
			common.ComponentDataNode: {Replicas: ptr.To(int32(5))},
		},
	})
	spec, err := BuildMilvusSpec(c)
	require.NoError(t, err)

	// Milvus 2.6 cluster uses a single MixCoord plus a StreamingNode; the
	// pre-2.6 coordinators and IndexNode are no longer generated.
	require.NotNil(t, spec.Com.Proxy)
	require.NotNil(t, spec.Com.MixCoord)
	require.NotNil(t, spec.Com.DataNode)
	require.NotNil(t, spec.Com.QueryNode)
	require.NotNil(t, spec.Com.StreamingNode)

	assert.Equal(t, milvusapi.MilvusModeCluster, spec.Mode)
	assert.Equal(t, ptr.To(int32(3)), spec.Com.Proxy.Replicas)
	assert.Equal(t, ptr.To(int32(5)), spec.Com.DataNode.Replicas)
}

func gpuProviderSpec() corev1alpha1.ProviderSpec {
	components := map[string]corev1alpha1.Component{}
	bundle := map[string]string{}
	for _, name := range []string{common.ComponentStandalone, common.ComponentProxy, common.ComponentMixCoord, common.ComponentDataNode, common.ComponentQueryNode, common.ComponentStreaming} {
		components[name] = corev1alpha1.Component{Type: "milvus"}
		bundle[name] = "2.6.15-gpu"
	}
	return corev1alpha1.ProviderSpec{
		Components: components,
		ComponentTypes: map[string]corev1alpha1.ComponentType{"milvus": {Versions: []corev1alpha1.ComponentVersion{
			{Version: "2.6.15", Image: "milvusdb/milvus:v2.6.15"},
			{Version: "2.6.15-gpu", Image: "milvusdb/milvus:v2.6.15-gpu"},
		}}},
		Versions: []corev1alpha1.VersionBundle{{Name: "2.6.15-gpu", Components: bundle}},
	}
}

func newTestContextWithProviderSpec(t *testing.T, providerSpec corev1alpha1.ProviderSpec, spec corev1alpha1.InstanceSpec) *controller.Context {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, milvusapi.AddToScheme(scheme))
	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Spec:       spec,
	}
	provider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: common.ProviderName},
		Spec:       providerSpec,
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, provider).Build()
	return controller.NewContext(context.Background(), fakeClient, instance, common.ProviderName)
}

func TestBuildMilvusSpecComponentImages(t *testing.T) {
	t.Run("bundle keeps one image and the ordered rolling upgrade", func(t *testing.T) {
		c := newTestContextWithProviderSpec(t, gpuProviderSpec(), corev1alpha1.InstanceSpec{
			Version:  "2.6.15-gpu",
			Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)

		assert.Equal(t, "milvusdb/milvus:v2.6.15-gpu", spec.Com.Image)
		assert.Equal(t, "milvusdb/milvus:v2.6.15-gpu", spec.Com.QueryNode.Image)
		assert.Equal(t, "milvusdb/milvus:v2.6.15-gpu", spec.Com.Proxy.Image)
		// The operator parses the version as semver, so the build suffix is dropped.
		assert.Equal(t, "2.6.15", spec.Com.QueryNode.Version)
		assert.Equal(t, "2.6.15", spec.Com.Version)
		assert.Equal(t, milvusapi.ImageUpdateModeRollingUpgrade, spec.Com.ImageUpdateMode)
	})

	t.Run("components pinned to one release roll in order on its image", func(t *testing.T) {
		pinned := corev1alpha1.ComponentSpec{Version: "2.6.15"}
		c := newTestContextWithProviderSpec(t, gpuProviderSpec(), corev1alpha1.InstanceSpec{
			Version:  "2.6.15-gpu",
			Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentProxy:     pinned,
				common.ComponentMixCoord:  pinned,
				common.ComponentDataNode:  pinned,
				common.ComponentQueryNode: pinned,
				common.ComponentStreaming: pinned,
			},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)

		assert.Equal(t, "milvusdb/milvus:v2.6.15", spec.Com.Image)
		assert.Equal(t, milvusapi.ImageUpdateModeRollingUpgrade, spec.Com.ImageUpdateMode)
	})

	// Live clusters showed the ordered upgrade deadlocking on mixed images.
	t.Run("image override updates all images at once", func(t *testing.T) {
		c := newTestContextWithProviderSpec(t, gpuProviderSpec(), corev1alpha1.InstanceSpec{
			Version:  "2.6.15-gpu",
			Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentStreaming: {Image: "registry.local/milvus:v2.6.15-gpu"},
			},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)

		assert.Equal(t, "registry.local/milvus:v2.6.15-gpu", spec.Com.StreamingNode.Image)
		assert.Equal(t, milvusapi.ImageUpdateModeAll, spec.Com.ImageUpdateMode)
	})
}

func TestBuildMilvusSpecActiveStandby(t *testing.T) {
	t.Run("cluster enables active-standby on every coordinator", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentMixCoord: {
					Replicas:   ptr.To(int32(2)),
					Parameters: configParams(t, "rootCoord:\n  dmlChannelNum: 32\n"),
				},
			},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		for _, section := range coordinatorConfigSections {
			assert.Equal(t, true, spec.Conf[section].(map[string]any)["enableActiveStandby"], section)
		}
		assert.Equal(t, float64(32), spec.Conf["rootCoord"].(map[string]any)["dmlChannelNum"])
	})

	t.Run("standalone is left to the operator", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{Topology: &corev1alpha1.TopologySpec{Type: "standalone"}})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		assert.Nil(t, spec.Conf)
	})
}

func TestValidateMixCoordStandby(t *testing.T) {
	disabled := configParams(t, "queryCoord:\n  enableActiveStandby: false\n")
	tests := map[string]struct {
		replicas int32
		wantErr  string
	}{
		"single replica may disable active-standby": {replicas: 1},
		"several replicas need active-standby":      {replicas: 2, wantErr: "queryCoord.enableActiveStandby"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			c := newTestContext(t, corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
				Components: map[string]corev1alpha1.ComponentSpec{
					common.ComponentMixCoord: {Replicas: ptr.To(tt.replicas), Parameters: disabled},
				},
			})
			err := validateInstance(c)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestNotReadyMessage(t *testing.T) {
	cr := &milvusapi.Milvus{Status: milvusapi.MilvusStatus{Conditions: []milvusapi.MilvusCondition{
		{Type: "MilvusReady", Status: corev1.ConditionFalse, Message: "[standalone] not ready"},
		{Type: "StorageReady", Status: corev1.ConditionFalse, Reason: "SecretNotExist", Message: "Secret not exist"},
		{Type: "EtcdReady", Status: corev1.ConditionTrue},
	}}}
	assert.Equal(t, "waiting: StorageReady: Secret not exist", notReadyMessage(cr, "waiting"),
		"a failing dependency is reported before the components waiting on it")

	cr.Status.Conditions[1].Message = ""
	assert.Equal(t, "waiting: StorageReady: SecretNotExist", notReadyMessage(cr, "waiting"))

	assert.Equal(t, "waiting", notReadyMessage(&milvusapi.Milvus{}, "waiting"))
}

func TestBuildMilvusSpecLabelsComponentPods(t *testing.T) {
	podLabels := func(component string) map[string]string {
		return map[string]string{
			controller.ProviderLabel:  common.ProviderName,
			controller.InstanceLabel:  "test-milvus",
			controller.ComponentLabel: component,
		}
	}

	t.Run("standalone", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)

		assert.Equal(t, podLabels(common.ComponentStandalone), spec.Com.Standalone.PodLabels)
		assert.Equal(t, []string{common.ComponentStandalone}, c.LabelledComponents())
	})

	t.Run("cluster labels every component, including ones absent from the Instance", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentProxy: {},
			},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)

		assert.Equal(t, podLabels(common.ComponentProxy), spec.Com.Proxy.PodLabels)
		assert.Equal(t, podLabels(common.ComponentMixCoord), spec.Com.MixCoord.PodLabels)
		assert.Equal(t, podLabels(common.ComponentDataNode), spec.Com.DataNode.PodLabels)
		assert.Equal(t, podLabels(common.ComponentQueryNode), spec.Com.QueryNode.PodLabels)
		assert.Equal(t, podLabels(common.ComponentStreaming), spec.Com.StreamingNode.PodLabels)
		assert.Nil(t, spec.Com.PodLabels, "global podLabels would be merged into every component")
		assert.ElementsMatch(t, []string{
			common.ComponentProxy, common.ComponentMixCoord, common.ComponentDataNode,
			common.ComponentQueryNode, common.ComponentStreaming,
		}, c.LabelledComponents())
	})
}

func TestBuildMilvusSpecComponentResources(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
		Components: map[string]corev1alpha1.ComponentSpec{
			common.ComponentDataNode: {
				Resources: resources(t,
					map[corev1.ResourceName]string{"cpu": "2", "memory": "4Gi"},
					map[corev1.ResourceName]string{"cpu": "1", "memory": "2Gi"}),
			},
		},
	})
	spec, err := BuildMilvusSpec(c)
	require.NoError(t, err)
	require.NotNil(t, spec.Com.DataNode)
	res := spec.Com.DataNode.Resources
	require.NotNil(t, res)
	// Both limits and requests flow through, not just limits.
	assert.Equal(t, "2", res.Limits.Cpu().String())
	assert.Equal(t, "4Gi", res.Limits.Memory().String())
	assert.Equal(t, "1", res.Requests.Cpu().String())
	assert.Equal(t, "2Gi", res.Requests.Memory().String())
}

func TestSyncSeedsAuthAndStatusSurfacesCredentials(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
		Components: map[string]corev1alpha1.ComponentSpec{
			common.ComponentStandalone: {},
		},
	})
	p := New()

	require.NoError(t, p.Sync(c))

	cr := &milvusapi.Milvus{}
	require.NoError(t, c.Get(cr, c.Name()))
	security := cr.Spec.Conf["common"].(map[string]any)["security"].(map[string]any)
	assert.Equal(t, true, security["authorizationEnabled"])
	assert.NotContains(t, security, "defaultRootPassword", "the password must stay out of the CR")
	require.Len(t, cr.Spec.Com.Env, 1)
	assert.Equal(t, credentialsSecretName(c.Name()), cr.Spec.Com.Env[0].ValueFrom.SecretKeyRef.Name)

	secret := &corev1.Secret{}
	require.NoError(t, c.Get(secret, credentialsSecretName(c.Name())))
	password := string(secret.Data[secretKeyPassword])
	assert.NotEmpty(t, password)

	cr.Status.Status = milvusapi.StatusHealthy
	// Stands in for the operator reporting health.
	require.NoError(t, c.Client().Update(c.Context(), cr))

	status, err := p.Status(c)
	require.NoError(t, err)

	cd := status.ConnectionDetails
	assert.Equal(t, rootUsername, cd.Username)
	assert.Equal(t, password, cd.Password, "connection password must match the seeded root password")
	assert.Equal(t, "test-milvus-milvus.db.svc.cluster.local", cd.Host)
	assert.Equal(t, "19530", cd.Port)
	assert.Equal(t, rootUsername+":"+password, cd.AdditionalProperties["token"])
}

func TestBuildMilvusSpecTopology(t *testing.T) {
	t.Run("standalone maps mode, version and replicas", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
			Version:  "2.6.0",
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentStandalone: {Replicas: ptr.To(int32(2))},
			},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)

		assert.Equal(t, milvusapi.MilvusModeStandalone, spec.Mode)
		assert.Equal(t, "2.6.0", spec.Com.Version)
		require.NotNil(t, spec.Com.Standalone)
		assert.Equal(t, ptr.To(int32(2)), spec.Com.Standalone.Replicas)
	})

	t.Run("unset version falls back to the default", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		assert.Equal(t, "2.6.11", spec.Com.Version)
	})

	t.Run("unset version resolves the default bundle and image from the Provider", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
		})
		provider := &corev1alpha1.Provider{}
		require.NoError(t, c.Client().Get(context.Background(), client.ObjectKey{Name: common.ProviderName}, provider))
		provider.Spec = corev1alpha1.ProviderSpec{
			ComponentTypes: map[string]corev1alpha1.ComponentType{
				"milvus": {Versions: []corev1alpha1.ComponentVersion{{Version: "2.6.0", Image: "example.com/milvus:v2.6.0"}}},
			},
			Components: map[string]corev1alpha1.Component{
				common.ComponentStandalone: {Type: "milvus"},
			},
			DefaultVersion: "2.6.0",
			Versions:       []corev1alpha1.VersionBundle{{Name: "2.6.0"}},
		}
		require.NoError(t, c.Client().Update(context.Background(), provider))

		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		assert.Equal(t, "2.6.0", spec.Com.Version)
		assert.Equal(t, "example.com/milvus:v2.6.0", spec.Com.Image)
	})

	t.Run("missing Provider CR returns a not-found error", func(t *testing.T) {
		scheme := runtime.NewScheme()
		require.NoError(t, corev1alpha1.AddToScheme(scheme))
		require.NoError(t, milvusapi.AddToScheme(scheme))

		instance := &corev1alpha1.Instance{
			ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).Build()
		c := controller.NewContext(context.Background(), fakeClient, instance, common.ProviderName)

		_, err := BuildMilvusSpec(c)
		require.Error(t, err)
		assert.True(t, apierrors.IsNotFound(err), "got %v", err)
	})
}

func newMilvusCR(status milvusapi.MilvusHealthStatus, endpoint string) *milvusapi.Milvus {
	return &milvusapi.Milvus{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Status: milvusapi.MilvusStatus{
			Status:   status,
			Endpoint: endpoint,
		},
	}
}

func TestProviderStatus(t *testing.T) {
	t.Run("missing Milvus CR returns Provisioning", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{})

		status, err := New().Status(c)
		require.NoError(t, err)
		assert.Equal(t, corev1alpha1.InstancePhaseProvisioning, status.Phase)
		assert.Equal(t, "waiting for Milvus CR to be created", status.Message)
	})

	notReadyTests := []struct {
		name        string
		status      milvusapi.MilvusHealthStatus
		wantPhase   corev1alpha1.InstancePhase
		wantMessage string
	}{
		{name: "stopped", status: milvusapi.StatusStopped, wantPhase: corev1alpha1.InstancePhasePending, wantMessage: "Milvus is stopped"},
		{name: "unhealthy", status: milvusapi.StatusUnhealthy, wantPhase: corev1alpha1.InstancePhaseProvisioning, wantMessage: "Milvus is unhealthy"},
		{name: "pending", status: milvusapi.StatusPending, wantPhase: corev1alpha1.InstancePhaseProvisioning, wantMessage: "Milvus is being initialized or updated"},
		{name: "deleting", status: milvusapi.StatusDeleting, wantPhase: corev1alpha1.InstancePhaseProvisioning, wantMessage: "Milvus is being initialized or updated"},
		{name: "unknown", status: "Unknown", wantPhase: corev1alpha1.InstancePhaseProvisioning, wantMessage: "Milvus is initializing"},
	}
	for _, tt := range notReadyTests {
		t.Run(tt.name+" status is not ready", func(t *testing.T) {
			c := newTestContext(t, corev1alpha1.InstanceSpec{})
			require.NoError(t, c.Client().Create(context.Background(), newMilvusCR(tt.status, "")))

			status, err := New().Status(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantPhase, status.Phase)
			assert.Equal(t, tt.wantMessage, status.Message)
		})
	}

	t.Run("healthy status is Ready with a connection URI", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{})
		require.NoError(t, c.Client().Create(context.Background(), newMilvusCR(milvusapi.StatusHealthy, "my-endpoint.db.svc.cluster.local:19530")))

		status, err := New().Status(c)
		require.NoError(t, err)
		assert.Equal(t, corev1alpha1.InstancePhaseReady, status.Phase)
		assert.Equal(t, "http://my-endpoint.db.svc.cluster.local:19530", status.ConnectionDetails.URI)
	})

	rolloutTests := []struct {
		name        string
		prepare     func(cr *milvusapi.Milvus)
		wantMessage string
	}{
		{
			name: "operator has not seen the latest spec",
			prepare: func(cr *milvusapi.Milvus) {
				cr.Generation = 3
				cr.Status.ObservedGeneration = 2
			},
			wantMessage: "Milvus is applying the latest changes",
		},
		{
			name: "rolling upgrade in progress",
			prepare: func(cr *milvusapi.Milvus) {
				cr.Status.Conditions = []milvusapi.MilvusCondition{{
					Type:    milvusapi.ConditionMilvusUpdated,
					Status:  corev1.ConditionFalse,
					Reason:  "MilvusUpgradingImage",
					Message: "Milvus is performing rolling upgrade pending components[queryNode]",
				}}
			},
			wantMessage: "Milvus is performing rolling upgrade pending components[queryNode]",
		},
	}
	for _, tt := range rolloutTests {
		t.Run("healthy status is Updating while "+tt.name, func(t *testing.T) {
			c := newTestContext(t, corev1alpha1.InstanceSpec{})
			cr := newMilvusCR(milvusapi.StatusHealthy, "my-endpoint.db.svc.cluster.local:19530")
			require.NoError(t, c.Client().Create(context.Background(), cr))
			tt.prepare(cr)
			require.NoError(t, c.Client().Update(context.Background(), cr))

			status, err := New().Status(c)
			require.NoError(t, err)
			assert.Equal(t, corev1alpha1.InstancePhaseUpdating, status.Phase)
			assert.Equal(t, tt.wantMessage, status.Message)
			assert.Equal(t, "http://my-endpoint.db.svc.cluster.local:19530", status.ConnectionDetails.URI)
		})
	}

	t.Run("unhealthy status is Updating while a rollout replaces pods", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{})
		cr := newMilvusCR(milvusapi.StatusUnhealthy, "")
		cr.Status.Conditions = []milvusapi.MilvusCondition{
			{Type: "MilvusReady", Status: corev1.ConditionFalse, Message: "[querynode] not ready"},
			{Type: milvusapi.ConditionMilvusUpdated, Status: corev1.ConditionFalse, Message: "Milvus is performing rolling upgrade pending components[querynode]"},
		}
		require.NoError(t, c.Client().Create(context.Background(), cr))

		status, err := New().Status(c)
		require.NoError(t, err)
		assert.Equal(t, corev1alpha1.InstancePhaseUpdating, status.Phase)
		assert.Equal(t, "Milvus is performing rolling upgrade pending components[querynode]: MilvusReady: [querynode] not ready", status.Message)
	})

	t.Run("healthy status with an unresolved endpoint stays Provisioning", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{})
		cr := newMilvusCR(milvusapi.StatusHealthy, "")
		cr.Spec.Com.Standalone = &milvusapi.MilvusStandalone{
			ServiceComponent: milvusapi.ServiceComponent{ServiceType: corev1.ServiceTypeLoadBalancer},
		}
		require.NoError(t, c.Client().Create(context.Background(), cr))

		status, err := New().Status(c)
		require.NoError(t, err)
		assert.Equal(t, corev1alpha1.InstancePhaseProvisioning, status.Phase)
	})
}
