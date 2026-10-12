package provider

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"

	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

func catalogProviderSpec() corev1alpha1.ProviderSpec {
	versions := []string{"2.6.10", "2.6.11", "2.6.15", "2.6.15-gpu"}
	components := map[string]corev1alpha1.Component{}
	catalog := make([]corev1alpha1.ComponentVersion, 0, len(versions))
	bundles := make([]corev1alpha1.VersionBundle, 0, len(versions))
	for _, version := range versions {
		catalog = append(catalog, corev1alpha1.ComponentVersion{Version: version, Image: "milvusdb/milvus:v" + version})
		bundle := map[string]string{}
		for _, name := range []string{common.ComponentStandalone, common.ComponentProxy, common.ComponentMixCoord, common.ComponentDataNode, common.ComponentQueryNode, common.ComponentStreaming} {
			components[name] = corev1alpha1.Component{Type: "milvus"}
			bundle[name] = version
		}
		bundles = append(bundles, corev1alpha1.VersionBundle{Name: version, Components: bundle})
	}
	return corev1alpha1.ProviderSpec{
		Components:     components,
		ComponentTypes: map[string]corev1alpha1.ComponentType{"milvus": {Versions: catalog}},
		Versions:       bundles,
	}
}

func clusterAtVersion(version string, components map[string]corev1alpha1.ComponentSpec) corev1alpha1.InstanceSpec {
	return corev1alpha1.InstanceSpec{
		Version:    version,
		Topology:   &corev1alpha1.TopologySpec{Type: "cluster"},
		Components: components,
	}
}

// deployedMilvus is the Milvus CR the provider applied for spec.
func deployedMilvus(t *testing.T, spec corev1alpha1.InstanceSpec) *milvusapi.Milvus {
	t.Helper()
	milvusSpec, err := BuildMilvusSpec(newTestContextWithProviderSpec(t, catalogProviderSpec(), spec))
	require.NoError(t, err)
	return &milvusapi.Milvus{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Spec:       milvusSpec,
	}
}

func TestValidateNoDowngrade(t *testing.T) {
	tests := []struct {
		name     string
		deployed *milvusapi.Milvus
		spec     corev1alpha1.InstanceSpec
		wantErr  string
	}{
		{
			name:     "bundle downgrade is rejected",
			deployed: deployedMilvus(t, clusterAtVersion("2.6.11", nil)),
			spec:     clusterAtVersion("2.6.10", nil),
			wantErr:  "cannot be downgraded from 2.6.11 to 2.6.10",
		},
		{
			name:     "bundle upgrade is allowed",
			deployed: deployedMilvus(t, clusterAtVersion("2.6.11", nil)),
			spec:     clusterAtVersion("2.6.15", nil),
		},
		{
			name:     "same release with a build suffix is allowed",
			deployed: deployedMilvus(t, clusterAtVersion("2.6.15", nil)),
			spec:     clusterAtVersion("2.6.15-gpu", nil),
		},
		{
			name:     "component pinned below its deployed version is rejected",
			deployed: deployedMilvus(t, clusterAtVersion("2.6.15", nil)),
			spec: clusterAtVersion("2.6.15", map[string]corev1alpha1.ComponentSpec{
				common.ComponentQueryNode: {Version: "2.6.11"},
			}),
			wantErr: `component "queryNode" cannot be downgraded from 2.6.15 to 2.6.11`,
		},
		{
			name: "component without its own version is compared at the instance version",
			deployed: &milvusapi.Milvus{
				ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
				Spec: milvusapi.MilvusSpec{
					Mode: milvusapi.MilvusModeCluster,
					Com: milvusapi.MilvusComponents{
						ComponentSpec: milvusapi.ComponentSpec{Version: "2.6.11"},
						MixCoord:      &milvusapi.MilvusMixCoord{},
					},
				},
			},
			spec:    clusterAtVersion("2.6.10", nil),
			wantErr: `component "mixCoord" cannot be downgraded from 2.6.11 to 2.6.10`,
		},
		{
			name: "new instance has nothing to compare",
			spec: clusterAtVersion("2.6.10", nil),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestContextWithProviderSpec(t, catalogProviderSpec(), tt.spec)
			if tt.deployed != nil {
				require.NoError(t, c.Client().Create(context.Background(), tt.deployed))
			}
			err := validateNoDowngrade(c)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
