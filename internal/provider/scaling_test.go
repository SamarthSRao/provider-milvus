package provider

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"

	"github.com/openeverest/provider-milvus/definition/components"
	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

func autoscalingParams(t *testing.T, autoscaling components.Autoscaling, groups ...components.DeploymentGroup) *runtime.RawExtension {
	t.Helper()
	raw, err := json.Marshal(components.MilvusParameters{Autoscaling: &autoscaling, Groups: groups})
	require.NoError(t, err)
	return &runtime.RawExtension{Raw: raw}
}

func cpuLimit(t *testing.T) *corev1.ResourceRequirements {
	t.Helper()
	return resources(t, map[corev1.ResourceName]string{"cpu": "1", "memory": "2Gi"}, nil)
}

func TestBuildMilvusSpecAutoscaling(t *testing.T) {
	cluster := func(t *testing.T, autoscaling components.Autoscaling) milvusapi.MilvusSpec {
		t.Helper()
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentQueryNode: {
					Replicas:   ptr.To(int32(2)),
					Resources:  cpuLimit(t),
					Parameters: autoscalingParams(t, autoscaling),
				},
			},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		return spec
	}
	utilization := func(resource corev1.ResourceName, percent int32) autoscalingv2.MetricSpec {
		return autoscalingv2.MetricSpec{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name:   resource,
				Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: ptr.To(percent)},
			},
		}
	}

	t.Run("replicas are the minimum", func(t *testing.T) {
		spec := cluster(t, components.Autoscaling{
			MaxReplicas:             6,
			TargetCPUUtilization:    ptr.To(int32(70)),
			TargetMemoryUtilization: ptr.To(int32(85)),
		})
		assert.Equal(t, &milvusapi.HPASpec{
			MinReplicas: ptr.To(int32(2)),
			MaxReplicas: 6,
			Metrics:     []autoscalingv2.MetricSpec{utilization(corev1.ResourceCPU, 70), utilization(corev1.ResourceMemory, 85)},
		}, spec.Com.QueryNode.HPA)
		assert.Nil(t, spec.Com.Proxy.HPA)
	})

	t.Run("no target aims at 80% CPU", func(t *testing.T) {
		spec := cluster(t, components.Autoscaling{MaxReplicas: 4})
		assert.Equal(t, []autoscalingv2.MetricSpec{utilization(corev1.ResourceCPU, 80)}, spec.Com.QueryNode.HPA.Metrics)
	})
}

func TestValidateAutoscaling(t *testing.T) {
	tests := []struct {
		name      string
		component string
		spec      corev1alpha1.ComponentSpec
		wantErr   string
	}{
		{
			name:      "valid",
			component: common.ComponentProxy,
			spec: corev1alpha1.ComponentSpec{
				Replicas:   ptr.To(int32(2)),
				Resources:  cpuLimit(t),
				Parameters: autoscalingParams(t, components.Autoscaling{MaxReplicas: 2, TargetMemoryUtilization: ptr.To(int32(75))}),
			},
		},
		{
			name:      "coordinator",
			component: common.ComponentMixCoord,
			spec:      corev1alpha1.ComponentSpec{Resources: cpuLimit(t), Parameters: autoscalingParams(t, components.Autoscaling{MaxReplicas: 3})},
			wantErr:   `component "mixCoord" does not support autoscaling`,
		},
		{
			name:      "with groups",
			component: common.ComponentQueryNode,
			spec: corev1alpha1.ComponentSpec{
				Resources:  cpuLimit(t),
				Parameters: autoscalingParams(t, components.Autoscaling{MaxReplicas: 3}, components.DeploymentGroup{Name: "a", Replicas: 1}),
			},
			wantErr: "cannot combine autoscaling with groups",
		},
		{
			name:      "maximum below replicas",
			component: common.ComponentDataNode,
			spec: corev1alpha1.ComponentSpec{
				Replicas:   ptr.To(int32(3)),
				Resources:  cpuLimit(t),
				Parameters: autoscalingParams(t, components.Autoscaling{MaxReplicas: 2}),
			},
			wantErr: "autoscaling.maxReplicas (2) must be at least its replicas (3)",
		},
		{
			name:      "zero target",
			component: common.ComponentDataNode,
			spec: corev1alpha1.ComponentSpec{
				Resources:  cpuLimit(t),
				Parameters: autoscalingParams(t, components.Autoscaling{MaxReplicas: 2, TargetCPUUtilization: ptr.To(int32(0))}),
			},
			wantErr: "autoscaling.targetCPUUtilization must be >= 1",
		},
		{
			name:      "memory target without memory resources",
			component: common.ComponentDataNode,
			spec: corev1alpha1.ComponentSpec{
				Resources:  resources(t, map[corev1.ResourceName]string{"cpu": "1"}, nil),
				Parameters: autoscalingParams(t, components.Autoscaling{MaxReplicas: 2, TargetMemoryUtilization: ptr.To(int32(80))}),
			},
			wantErr: "autoscaling.targetMemoryUtilization needs resources.requests.memory or resources.limits.memory",
		},
		{
			name:      "default CPU target without resources",
			component: common.ComponentProxy,
			spec:      corev1alpha1.ComponentSpec{Parameters: autoscalingParams(t, components.Autoscaling{MaxReplicas: 2})},
			wantErr:   "targets CPU by default and needs resources.requests.cpu or resources.limits.cpu",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestContext(t, corev1alpha1.InstanceSpec{
				Topology:   &corev1alpha1.TopologySpec{Type: "cluster"},
				Components: map[string]corev1alpha1.ComponentSpec{tt.component: tt.spec},
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

func TestValidateStandaloneSingleReplica(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
		Components: map[string]corev1alpha1.ComponentSpec{
			common.ComponentStandalone: {Replicas: ptr.To(int32(2))},
		},
	})
	err := validateInstance(c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "standalone runs a single replica")
}

func TestClearDroppedAutoscaling(t *testing.T) {
	hpa := &milvusapi.HPASpec{MinReplicas: ptr.To(int32(1)), MaxReplicas: 3}
	existing := &milvusapi.Milvus{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Spec: milvusapi.MilvusSpec{Com: milvusapi.MilvusComponents{
			Proxy:     &milvusapi.MilvusProxy{ServiceComponent: milvusapi.ServiceComponent{Component: milvusapi.Component{HPA: hpa}}},
			QueryNode: &milvusapi.MilvusQueryNode{Component: milvusapi.Component{HPA: hpa}},
		}},
	}
	c := newTestContextWithObjects(t, corev1alpha1.InstanceSpec{Topology: &corev1alpha1.TopologySpec{Type: "cluster"}}, existing)
	desired := milvusapi.MilvusSpec{Com: milvusapi.MilvusComponents{
		Proxy:     &milvusapi.MilvusProxy{ServiceComponent: milvusapi.ServiceComponent{Component: milvusapi.Component{HPA: hpa}}},
		QueryNode: &milvusapi.MilvusQueryNode{},
	}}

	require.NoError(t, clearDroppedAutoscaling(c, desired))

	cr := &milvusapi.Milvus{}
	require.NoError(t, c.Get(cr, "test-milvus"))
	assert.Nil(t, cr.Spec.Com.QueryNode.HPA)
	assert.Equal(t, hpa, cr.Spec.Com.Proxy.HPA, "autoscaling that stays on is left to the apply")
}

type fakeLoadedReplicas struct {
	replicas map[string]int32
	err      error
	calls    int
}

func (f *fakeLoadedReplicas) LoadedReplicas(context.Context, string, string) (map[string]int32, error) {
	f.calls++
	return f.replicas, f.err
}

func TestValidateReplicaHostsScaleIn(t *testing.T) {
	deployed := func(replicas int32, status milvusapi.MilvusHealthStatus) *milvusapi.Milvus {
		return &milvusapi.Milvus{
			ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
			Spec: milvusapi.MilvusSpec{Com: milvusapi.MilvusComponents{
				QueryNode:     &milvusapi.MilvusQueryNode{Component: milvusapi.Component{Replicas: ptr.To(replicas)}},
				StreamingNode: &milvusapi.MilvusStreamingNode{Component: milvusapi.Component{Replicas: ptr.To(replicas)}},
			}},
			Status: milvusapi.MilvusStatus{Status: status},
		}
	}
	credentials := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: credentialsSecretName("test-milvus"), Namespace: "db"},
		Data:       map[string][]byte{secretKeyAppliedPassword: []byte("pw")},
	}
	nodes := func(component string, replicas int32) corev1alpha1.InstanceSpec {
		return corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
			Components: map[string]corev1alpha1.ComponentSpec{
				component: {Replicas: ptr.To(replicas)},
			},
		}
	}

	tests := []struct {
		name      string
		spec      corev1alpha1.InstanceSpec
		milvus    *milvusapi.Milvus
		api       *fakeLoadedReplicas
		wantCalls int
		wantErr   string
	}{
		{
			name:      "query nodes below a collection's replicas",
			spec:      nodes(common.ComponentQueryNode, 1),
			milvus:    deployed(3, milvusapi.StatusHealthy),
			api:       &fakeLoadedReplicas{replicas: map[string]int32{"docs": 2, "logs": 1}},
			wantCalls: 1,
			wantErr:   `queryNode replicas cannot go below 2: collection "docs" is loaded with 2 replicas`,
		},
		{
			name:      "streaming nodes below a collection's replicas",
			spec:      nodes(common.ComponentStreaming, 1),
			milvus:    deployed(2, milvusapi.StatusHealthy),
			api:       &fakeLoadedReplicas{replicas: map[string]int32{"docs": 2}},
			wantCalls: 1,
			wantErr:   `streamingNode replicas cannot go below 2`,
		},
		{
			name:      "enough for every collection",
			spec:      nodes(common.ComponentQueryNode, 2),
			milvus:    deployed(3, milvusapi.StatusHealthy),
			api:       &fakeLoadedReplicas{replicas: map[string]int32{"docs": 2}},
			wantCalls: 1,
		},
		{
			name:   "scaling out does not ask Milvus",
			spec:   nodes(common.ComponentQueryNode, 4),
			milvus: deployed(3, milvusapi.StatusHealthy),
			api:    &fakeLoadedReplicas{},
		},
		{
			name:   "unhealthy Milvus is not asked",
			spec:   nodes(common.ComponentQueryNode, 1),
			milvus: deployed(3, milvusapi.StatusUnhealthy),
			api:    &fakeLoadedReplicas{},
		},
		{
			name:      "Milvus not answering lets the change through",
			spec:      nodes(common.ComponentQueryNode, 1),
			milvus:    deployed(3, milvusapi.StatusHealthy),
			api:       &fakeLoadedReplicas{err: errors.New("connection refused")},
			wantCalls: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestContextWithObjects(t, tt.spec, tt.milvus, credentials)
			p := &Provider{loadedReplicas: tt.api}
			err := p.validateReplicaHostsScaleIn(c)
			assert.Equal(t, tt.wantCalls, tt.api.calls)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
