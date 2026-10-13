package provider

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

func newTestContextWithObjects(t *testing.T, spec corev1alpha1.InstanceSpec, objs ...client.Object) *controller.Context {
	t.Helper()
	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Spec:       spec,
	}
	provider := &corev1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: common.ProviderName}}
	all := append([]client.Object{instance, provider}, objs...)
	fakeClient := fakeClientBuilder(t).WithObjects(all...).Build()
	return controller.NewContext(context.Background(), fakeClient, instance, common.ProviderName)
}

func lbService(serviceType corev1.ServiceType) *corev1alpha1.Service {
	return &corev1alpha1.Service{ServiceType: serviceType}
}

func TestBuildMilvusSpecServiceExposure(t *testing.T) {
	t.Run("standalone maps service type and annotations", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentStandalone: {
					Service: &corev1alpha1.Service{
						ServiceType: corev1.ServiceTypeLoadBalancer,
						Annotations: map[string]string{"foo": "bar"},
					},
				},
			},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		assert.Equal(t, corev1.ServiceTypeLoadBalancer, spec.Com.Standalone.ServiceType)
		assert.Equal(t, map[string]string{"foo": "bar"}, spec.Com.Standalone.ServiceAnnotations)
	})

	t.Run("cluster maps proxy service type", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentProxy: {Service: lbService(corev1.ServiceTypeNodePort)},
			},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		assert.Equal(t, corev1.ServiceTypeNodePort, spec.Com.Proxy.ServiceType)
	})

	t.Run("defaults leave service type empty", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology:   &corev1alpha1.TopologySpec{Type: "standalone"},
			Components: map[string]corev1alpha1.ComponentSpec{common.ComponentStandalone: {}},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		assert.Empty(t, spec.Com.Standalone.ServiceType)
	})
}

func TestResolveEndpointClusterIP(t *testing.T) {
	c := newTestContextWithObjects(t, corev1alpha1.InstanceSpec{})
	cr := &milvusapi.Milvus{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Spec:       milvusapi.MilvusSpec{Com: milvusapi.MilvusComponents{Standalone: &milvusapi.MilvusStandalone{}}},
	}

	host, port, ready, _ := resolveEndpoint(c, cr)
	assert.True(t, ready)
	assert.Equal(t, "test-milvus-milvus.db.svc.cluster.local", host)
	assert.Equal(t, "19530", port)
}

func TestResolveEndpointLoadBalancer(t *testing.T) {
	c := newTestContextWithObjects(t, corev1alpha1.InstanceSpec{})
	cr := &milvusapi.Milvus{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Spec: milvusapi.MilvusSpec{Com: milvusapi.MilvusComponents{
			Standalone: &milvusapi.MilvusStandalone{ServiceComponent: milvusapi.ServiceComponent{ServiceType: corev1.ServiceTypeLoadBalancer}},
		}},
	}

	_, _, ready, message := resolveEndpoint(c, cr)
	assert.False(t, ready)
	assert.Contains(t, message, "load balancer")

	cr.Status.Endpoint = "203.0.113.10:19530"
	host, port, ready, _ := resolveEndpoint(c, cr)
	assert.True(t, ready)
	assert.Equal(t, "203.0.113.10", host)
	assert.Equal(t, "19530", port)
}

func TestResolveEndpointNodePort(t *testing.T) {
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus-milvus", Namespace: "db"},
		Spec: corev1.ServiceSpec{
			Type:  corev1.ServiceTypeNodePort,
			Ports: []corev1.ServicePort{{Name: "milvus", Port: 19530, NodePort: 30001}},
		},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
		Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{
			{Type: corev1.NodeInternalIP, Address: "10.0.0.5"},
			{Type: corev1.NodeExternalIP, Address: "198.51.100.7"},
		}},
	}
	c := newTestContextWithObjects(t, corev1alpha1.InstanceSpec{}, service, node)

	cr := &milvusapi.Milvus{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Spec: milvusapi.MilvusSpec{Com: milvusapi.MilvusComponents{
			Standalone: &milvusapi.MilvusStandalone{ServiceComponent: milvusapi.ServiceComponent{ServiceType: corev1.ServiceTypeNodePort}},
		}},
	}

	host, port, ready, _ := resolveEndpoint(c, cr)
	assert.True(t, ready)
	assert.Equal(t, "198.51.100.7", host, "external IP is preferred")
	assert.Equal(t, "30001", port)
}

func TestResolveEndpointNodePortNotReady(t *testing.T) {
	c := newTestContextWithObjects(t, corev1alpha1.InstanceSpec{})
	cr := &milvusapi.Milvus{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Spec: milvusapi.MilvusSpec{Com: milvusapi.MilvusComponents{
			Standalone: &milvusapi.MilvusStandalone{ServiceComponent: milvusapi.ServiceComponent{ServiceType: corev1.ServiceTypeNodePort}},
		}},
	}

	_, _, ready, message := resolveEndpoint(c, cr)
	assert.False(t, ready)
	assert.Contains(t, message, "NodePort")
}

func TestServiceTypeFromSpecClusterIgnoresDefaultedStandalone(t *testing.T) {
	// The operator's webhook defaults spec.components.standalone in cluster mode too.
	cr := &milvusapi.Milvus{Spec: milvusapi.MilvusSpec{
		Mode: milvusapi.MilvusModeCluster,
		Com: milvusapi.MilvusComponents{
			Standalone: &milvusapi.MilvusStandalone{ServiceComponent: milvusapi.ServiceComponent{ServiceType: corev1.ServiceTypeClusterIP}},
			Proxy:      &milvusapi.MilvusProxy{ServiceComponent: milvusapi.ServiceComponent{ServiceType: corev1.ServiceTypeNodePort}},
		},
	}}
	assert.Equal(t, corev1.ServiceTypeNodePort, serviceTypeFromSpec(cr))

	cr.Spec.Com.Proxy.ServiceType = ""
	assert.Equal(t, corev1.ServiceTypeClusterIP, serviceTypeFromSpec(cr))
}

func TestValidateServiceExposure(t *testing.T) {
	valid := map[string]corev1alpha1.ComponentSpec{
		common.ComponentStandalone: {Service: lbService(corev1.ServiceTypeLoadBalancer)},
	}
	assert.NoError(t, validateServiceExposure(valid, "standalone"))

	invalid := map[string]corev1alpha1.ComponentSpec{
		common.ComponentStandalone: {Service: lbService(corev1.ServiceTypeExternalName)},
	}
	err := validateServiceExposure(invalid, "standalone")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ClusterIP, LoadBalancer or NodePort")
}
