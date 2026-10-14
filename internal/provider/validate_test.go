package provider

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/definition/dependencies"
	"github.com/openeverest/provider-milvus/definition/topologies/standalone"
	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

func resources(t *testing.T, limits, requests map[corev1.ResourceName]string) *corev1.ResourceRequirements {
	t.Helper()
	toList := func(m map[corev1.ResourceName]string) corev1.ResourceList {
		if m == nil {
			return nil
		}
		list := corev1.ResourceList{}
		for name, value := range m {
			list[name] = resource.MustParse(value)
		}
		return list
	}
	return &corev1.ResourceRequirements{
		Limits:   toList(limits),
		Requests: toList(requests),
	}
}

func storage(t *testing.T, size string) *corev1alpha1.Storage {
	t.Helper()
	return &corev1alpha1.Storage{Size: resource.MustParse(size)}
}

func TestValidateInstance(t *testing.T) {
	tests := []struct {
		name    string
		spec    corev1alpha1.InstanceSpec
		wantErr string
	}{
		{
			name: "valid standalone",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
				Components: map[string]corev1alpha1.ComponentSpec{
					common.ComponentStandalone: {
						Replicas:  ptr.To(int32(1)),
						Resources: resources(t, map[corev1.ResourceName]string{"cpu": "1", "memory": "2Gi"}, nil),
					},
				},
			},
		},
		{
			name: "component storage",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
				Components: map[string]corev1alpha1.ComponentSpec{
					common.ComponentStandalone: {Storage: storage(t, "20Gi")},
				},
			},
			wantErr: `component "standalone" does not take storage`,
		},
		{
			name: "unsupported topology",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "sharded"},
			},
			wantErr: "unsupported topology",
		},
		{
			name: "component not valid for topology",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
				Components: map[string]corev1alpha1.ComponentSpec{
					common.ComponentProxy: {Replicas: ptr.To(int32(1))},
				},
			},
			wantErr: "not valid for standalone topology",
		},
		{
			name: "replicas below one",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
				Components: map[string]corev1alpha1.ComponentSpec{
					common.ComponentStandalone: {Replicas: ptr.To(int32(0))},
				},
			},
			wantErr: "replicas must be >= 1",
		},
		{
			name: "cpu below minimum",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
				Components: map[string]corev1alpha1.ComponentSpec{
					common.ComponentStandalone: {
						Resources: resources(t, map[corev1.ResourceName]string{"cpu": "50m", "memory": "2Gi"}, nil),
					},
				},
			},
			wantErr: "resources.limits.cpu must be >= 100m",
		},
		{
			name: "memory below minimum",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
				Components: map[string]corev1alpha1.ComponentSpec{
					common.ComponentStandalone: {
						Resources: resources(t, map[corev1.ResourceName]string{"cpu": "1", "memory": "128Mi"}, nil),
					},
				},
			},
			wantErr: "resources.limits.memory must be >= 512Mi",
		},
		{
			name: "request exceeds limit",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
				Components: map[string]corev1alpha1.ComponentSpec{
					common.ComponentStandalone: {
						Resources: resources(t,
							map[corev1.ResourceName]string{"cpu": "1", "memory": "2Gi"},
							map[corev1.ResourceName]string{"cpu": "2", "memory": "1Gi"}),
					},
				},
			},
			wantErr: "resources.requests.cpu",
		},
		{
			name: "cluster coordinator replicas below one",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
				Components: map[string]corev1alpha1.ComponentSpec{
					common.ComponentMixCoord: {Replicas: ptr.To(int32(0))},
				},
			},
			wantErr: "replicas must be >= 1",
		},
		{
			name: "valid cluster",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
				Components: map[string]corev1alpha1.ComponentSpec{
					common.ComponentProxy:    {Replicas: ptr.To(int32(1))},
					common.ComponentMixCoord: {Replicas: ptr.To(int32(1))},
					common.ComponentDataNode: {Replicas: ptr.To(int32(2))},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestContext(t, tt.spec)
			err := validateInstance(c)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestValidateStorageNotDecreased(t *testing.T) {
	newContextWithExistingStorage := func(t *testing.T, spec corev1alpha1.InstanceSpec, existingSize string) *controller.Context {
		t.Helper()
		scheme := runtime.NewScheme()
		require.NoError(t, corev1alpha1.AddToScheme(scheme))
		require.NoError(t, milvusapi.AddToScheme(scheme))

		instance := &corev1alpha1.Instance{
			ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
			Spec:       spec,
		}
		provider := &corev1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: common.ProviderName}}
		objects := []runtime.Object{instance, provider}
		if existingSize != "" {
			existing := &milvusapi.Milvus{
				ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
				Spec: milvusapi.MilvusSpec{
					Dep: &milvusapi.MilvusDependencies{
						Storage: milvusapi.MilvusStorage{
							InCluster: &milvusapi.InClusterConfig{
								Values: milvusapi.Values{
									"persistence": map[string]any{"size": existingSize},
								},
							},
						},
					},
				},
			}
			objects = append(objects, existing)
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).
			WithRuntimeObjects(objects...).Build()
		return controller.NewContext(context.Background(), fakeClient, instance, common.ProviderName)
	}

	standaloneSpec := func(size string) corev1alpha1.InstanceSpec {
		params := standalone.StandaloneTopologyParameters{
			Dependencies: &standalone.StandaloneDependencies{
				Storage: &dependencies.Storage{Persistence: &dependencies.Persistence{Size: size}},
			},
		}
		return corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone", Parameters: topologyParams(t, params)},
		}
	}

	t.Run("decrease is rejected", func(t *testing.T) {
		c := newContextWithExistingStorage(t, standaloneSpec("10Gi"), "20Gi")
		err := validateStorageNotDecreased(c, "standalone")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "storage size cannot be decreased")
	})

	t.Run("increase is allowed", func(t *testing.T) {
		c := newContextWithExistingStorage(t, standaloneSpec("40Gi"), "20Gi")
		require.NoError(t, validateStorageNotDecreased(c, "standalone"))
	})

	t.Run("same size is allowed", func(t *testing.T) {
		c := newContextWithExistingStorage(t, standaloneSpec("20Gi"), "20Gi")
		require.NoError(t, validateStorageNotDecreased(c, "standalone"))
	})

	t.Run("no existing CR skips guard", func(t *testing.T) {
		c := newContextWithExistingStorage(t, standaloneSpec("10Gi"), "")
		require.NoError(t, validateStorageNotDecreased(c, "standalone"))
	})
}

func TestValidateTopologyUnchanged(t *testing.T) {
	existing := &milvusapi.Milvus{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Spec:       milvusapi.MilvusSpec{Mode: milvusapi.MilvusModeStandalone},
	}

	t.Run("switch is rejected", func(t *testing.T) {
		c := newTestContextWithObjects(t, corev1alpha1.InstanceSpec{}, existing.DeepCopyObject().(*milvusapi.Milvus))
		err := validateTopologyUnchanged(c, "cluster")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "topology cannot be changed from standalone to cluster")
	})

	t.Run("same topology is allowed", func(t *testing.T) {
		c := newTestContextWithObjects(t, corev1alpha1.InstanceSpec{}, existing.DeepCopyObject().(*milvusapi.Milvus))
		require.NoError(t, validateTopologyUnchanged(c, "standalone"))
	})

	t.Run("no existing CR skips guard", func(t *testing.T) {
		c := newTestContextWithObjects(t, corev1alpha1.InstanceSpec{})
		require.NoError(t, validateTopologyUnchanged(c, "cluster"))
	})
}
