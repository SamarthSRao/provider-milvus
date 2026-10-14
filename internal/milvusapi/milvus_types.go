package milvusapi

import (
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const GroupVersion = "milvus.io/v1beta1"

var SchemeGroupVersion = schema.GroupVersion{Group: "milvus.io", Version: "v1beta1"}

// MilvusMode is the deployment mode for a Milvus instance.
type MilvusMode string

const (
	MilvusModeCluster    MilvusMode = "cluster"
	MilvusModeStandalone MilvusMode = "standalone"
)

const (
	// ImageUpdateModeRollingUpgrade updates components one after another in
	// the operator's dependency order.
	ImageUpdateModeRollingUpgrade = "rollingUpgrade"
	// ImageUpdateModeAll updates every component's image at once.
	ImageUpdateModeAll = "all"
)

// ConditionMilvusUpdated is False while the operator rolls out a spec change.
const ConditionMilvusUpdated = "MilvusUpdated"

// MilvusHealthStatus describes the observed health of the Milvus control plane.
type MilvusHealthStatus string

const (
	StatusPending   MilvusHealthStatus = "Pending"
	StatusHealthy   MilvusHealthStatus = "Healthy"
	StatusUnhealthy MilvusHealthStatus = "Unhealthy"
	StatusStopped   MilvusHealthStatus = "Stopped"
	StatusDeleting  MilvusHealthStatus = "Deleting"
)

// ComponentSpec holds shared image/version information for a Milvus component.
type ComponentSpec struct {
	Image     string                       `json:"image,omitempty"`
	Version   string                       `json:"version,omitempty"`
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
	// PodLabels are added to the pod template only, never to the selector.
	PodLabels                 map[string]string                 `json:"podLabels,omitempty"`
	SchedulerName             string                            `json:"schedulerName,omitempty"`
	NodeSelector              map[string]string                 `json:"nodeSelector,omitempty"`
	Affinity                  *corev1.Affinity                  `json:"affinity,omitempty"`
	Tolerations               []corev1.Toleration               `json:"tolerations,omitempty"`
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
	PodAnnotations            map[string]string                 `json:"podAnnotations,omitempty"`
	Env                       []corev1.EnvVar                   `json:"env,omitempty"`
	Volumes                   []Values                          `json:"volumes,omitempty"`
	VolumeMounts              []corev1.VolumeMount              `json:"volumeMounts,omitempty"`
	// SecurityContext applies to the Milvus container, not the pod.
	SecurityContext    Values `json:"securityContext,omitempty"`
	ServiceAccountName string `json:"serviceAccountName,omitempty"`
}

// Component is a generic Milvus component with replicas and image metadata.
type Component struct {
	ComponentSpec  `json:",inline"`
	Replicas       *int32   `json:"replicas,omitempty"`
	InitContainers []Values `json:"initContainers,omitempty"`
	// HPA makes the operator run a HorizontalPodAutoscaler, which then owns the
	// replica count.
	HPA *HPASpec `json:"hpa,omitempty"`
}

// HPASpec is the operator's HorizontalPodAutoscaler configuration.
type HPASpec struct {
	MinReplicas *int32                     `json:"minReplicas,omitempty"`
	MaxReplicas int32                      `json:"maxReplicas"`
	Metrics     []autoscalingv2.MetricSpec `json:"metrics,omitempty"`
}

// ServiceComponent adds a port number to a component.
type ServiceComponent struct {
	Component `json:",inline"`
	Port      int32 `json:"port,omitempty"`
	// ServiceType controls how the operator exposes the gRPC service.
	ServiceType corev1.ServiceType `json:"serviceType,omitempty"`
	// ServiceAnnotations are applied to the exposed Service (e.g. cloud
	// load-balancer settings).
	ServiceAnnotations map[string]string `json:"serviceAnnotations,omitempty"`
}

// MilvusStandalone defines the standalone deployment component.
type MilvusStandalone struct {
	ServiceComponent `json:",inline"`
}

// MilvusProxy defines the proxy component in cluster mode.
type MilvusProxy struct {
	ServiceComponent `json:",inline"`
	Groups           []DeploymentGroup `json:"groups,omitempty"`
}

// DeploymentGroup is one independently deployed workload of a component; it
// inherits the component spec and overrides only the fields it sets.
type DeploymentGroup struct {
	Name         string               `json:"name"`
	Replicas     *int32               `json:"replicas"`
	Annotations  map[string]string    `json:"annotations,omitempty"`
	ExtraEnv     []corev1.EnvVar      `json:"extraEnv,omitempty"`
	NodeSelector *map[string]string   `json:"nodeSelector,omitempty"`
	Affinity     *corev1.Affinity     `json:"affinity,omitempty"`
	Tolerations  *[]corev1.Toleration `json:"tolerations,omitempty"`
}

// MilvusMixCoord is the unified coordinator. Milvus 2.6 merges the former
// root/index/data/query coordinators into this single component.
type MilvusMixCoord struct {
	Component `json:",inline"`
}

// MilvusDataNode defines the data node component.
type MilvusDataNode struct {
	Component `json:",inline"`
	Groups    []DeploymentGroup `json:"groups,omitempty"`
}

// MilvusQueryNode defines the query node component.
type MilvusQueryNode struct {
	Component `json:",inline"`
	Groups    []DeploymentGroup `json:"groups,omitempty"`
}

// MilvusStreamingNode defines the streaming node component introduced by the
// Milvus 2.6 streaming architecture.
type MilvusStreamingNode struct {
	Component `json:",inline"`
	Groups    []DeploymentGroup `json:"groups,omitempty"`
}

// MilvusComponents contains the concrete Milvus deployment components.
type MilvusComponents struct {
	ComponentSpec    `json:",inline"`
	ImageUpdateMode  string               `json:"imageUpdateMode,omitempty"`
	Standalone       *MilvusStandalone    `json:"standalone,omitempty"`
	Proxy            *MilvusProxy         `json:"proxy,omitempty"`
	MixCoord         *MilvusMixCoord      `json:"mixCoord,omitempty"`
	DataNode         *MilvusDataNode      `json:"dataNode,omitempty"`
	QueryNode        *MilvusQueryNode     `json:"queryNode,omitempty"`
	StreamingNode    *MilvusStreamingNode `json:"streamingNode,omitempty"`
	EnableManualMode bool                 `json:"enableManualMode,omitempty"`

	// DisableMetric stops the operator from creating its own PodMonitor.
	DisableMetric bool `json:"disableMetric,omitempty"`
}

// MilvusSpec is the desired state of a Milvus deployment.
type MilvusSpec struct {
	Mode MilvusMode          `json:"mode,omitempty"`
	Com  MilvusComponents    `json:"components,omitempty"`
	Dep  *MilvusDependencies `json:"dependencies,omitempty"`
	// Conf is the Milvus engine configuration, deep-merged into the generated
	// Milvus config file by the operator (maps to the CRD's spec.config).
	Conf Values `json:"config,omitempty"`
}

type Values map[string]any

type InClusterConfig struct {
	Values         Values `json:"values,omitempty"`
	DeletionPolicy string `json:"deletionPolicy,omitempty"`
	PVCDeletion    bool   `json:"pvcDeletion,omitempty"`
}

// MilvusEtcd configures the etcd metadata store dependency.
type MilvusEtcd struct {
	Endpoints []string         `json:"endpoints,omitempty"`
	External  bool             `json:"external,omitempty"`
	InCluster *InClusterConfig `json:"inCluster,omitempty"`
}

// MilvusPulsar configures the Pulsar message-stream dependency (cluster mode).
type MilvusPulsar struct {
	InCluster *InClusterConfig `json:"inCluster,omitempty"`
	External  bool             `json:"external,omitempty"`
	Endpoint  string           `json:"endpoint,omitempty"`
}

type MilvusStorage struct {
	Type      string           `json:"type,omitempty"`
	SecretRef string           `json:"secretRef,omitempty"`
	Endpoint  string           `json:"endpoint,omitempty"`
	InCluster *InClusterConfig `json:"inCluster,omitempty"`
	External  bool             `json:"external,omitempty"`
}

type MilvusDependencies struct {
	Etcd          MilvusEtcd    `json:"etcd,omitempty"`
	MsgStreamType string        `json:"msgStreamType,omitempty"`
	Pulsar        MilvusPulsar  `json:"pulsar,omitempty"`
	Storage       MilvusStorage `json:"storage,omitempty"`
}

// MilvusStatus is the observed state of a Milvus deployment.
type MilvusStatus struct {
	Status             MilvusHealthStatus `json:"status,omitempty"`
	Endpoint           string             `json:"endpoint,omitempty"`
	Conditions         []MilvusCondition  `json:"conditions,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
}

// MilvusCondition is one readiness condition the operator reports.
type MilvusCondition struct {
	Type    string                 `json:"type"`
	Status  corev1.ConditionStatus `json:"status"`
	Reason  string                 `json:"reason,omitempty"`
	Message string                 `json:"message,omitempty"`
}

// Milvus is a minimal CRD model used by the provider to render the Milvus control-plane resource.
type Milvus struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              MilvusSpec   `json:"spec,omitempty"`
	Status            MilvusStatus `json:"status,omitempty"`
}

// MilvusList is the list type for a Milvus resource.
type MilvusList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Milvus `json:"items"`
}

func (m *Milvus) DeepCopyObject() runtime.Object {
	if m == nil {
		return nil
	}
	out := *m
	return &out
}

func (m *MilvusList) DeepCopyObject() runtime.Object {
	if m == nil {
		return nil
	}
	out := *m
	if m.Items != nil {
		out.Items = make([]Milvus, len(m.Items))
		copy(out.Items, m.Items)
	}
	return &out
}

func AddToScheme(scheme *runtime.Scheme) error {
	if scheme == nil {
		return nil
	}
	scheme.AddKnownTypes(SchemeGroupVersion, &Milvus{}, &MilvusList{})
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
