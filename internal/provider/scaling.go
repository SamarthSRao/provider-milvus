package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/definition/components"
	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// autoscalableComponents are the stateless components a HorizontalPodAutoscaler
// may scale. MixCoord runs active-standby, streaming nodes own WAL channels.
var autoscalableComponents = []string{
	common.ComponentProxy,
	common.ComponentDataNode,
	common.ComponentQueryNode,
}

const defaultTargetCPUUtilization int32 = 80

// applyAutoscaling hands the component's replica count to an autoscaler that
// keeps at least the component's replicas.
func applyAutoscaling(component *milvusapi.Component, autoscaling *components.Autoscaling) {
	if autoscaling == nil {
		return
	}
	targetCPU := autoscaling.TargetCPUUtilization
	if targetCPU == nil && autoscaling.TargetMemoryUtilization == nil {
		targetCPU = ptr.To(defaultTargetCPUUtilization)
	}
	var metrics []autoscalingv2.MetricSpec
	for _, target := range []struct {
		resource    corev1.ResourceName
		utilization *int32
	}{{corev1.ResourceCPU, targetCPU}, {corev1.ResourceMemory, autoscaling.TargetMemoryUtilization}} {
		if target.utilization == nil {
			continue
		}
		metrics = append(metrics, autoscalingv2.MetricSpec{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name: target.resource,
				Target: autoscalingv2.MetricTarget{
					Type:               autoscalingv2.UtilizationMetricType,
					AverageUtilization: target.utilization,
				},
			},
		})
	}
	component.HPA = &milvusapi.HPASpec{
		MinReplicas: component.Replicas,
		MaxReplicas: autoscaling.MaxReplicas,
		Metrics:     metrics,
	}
}

func validateAutoscaling(name string, component corev1alpha1.ComponentSpec, params components.MilvusParameters) error {
	autoscaling := params.Autoscaling
	if autoscaling == nil {
		return nil
	}
	if !slices.Contains(autoscalableComponents, name) {
		return fmt.Errorf("component %q does not support autoscaling; use it on proxy, dataNode or queryNode", name)
	}
	if len(params.Groups) > 0 {
		return fmt.Errorf("component %q cannot combine autoscaling with groups", name)
	}
	minReplicas := ptr.Deref(component.Replicas, 1)
	if autoscaling.MaxReplicas < minReplicas {
		return fmt.Errorf("component %q autoscaling.maxReplicas (%d) must be at least its replicas (%d)", name, autoscaling.MaxReplicas, minReplicas)
	}
	for _, target := range []struct {
		field       string
		resource    corev1.ResourceName
		utilization *int32
	}{
		{"targetCPUUtilization", corev1.ResourceCPU, autoscaling.TargetCPUUtilization},
		{"targetMemoryUtilization", corev1.ResourceMemory, autoscaling.TargetMemoryUtilization},
	} {
		if target.utilization == nil {
			continue
		}
		if *target.utilization < 1 {
			return fmt.Errorf("component %q autoscaling.%s must be >= 1", name, target.field)
		}
		if !hasResource(component.Resources, target.resource) {
			return fmt.Errorf("component %q autoscaling.%s needs resources.requests.%s or resources.limits.%s", name, target.field, target.resource, target.resource)
		}
	}
	if autoscaling.TargetCPUUtilization == nil && autoscaling.TargetMemoryUtilization == nil &&
		!hasResource(component.Resources, corev1.ResourceCPU) {
		return fmt.Errorf("component %q autoscaling targets CPU by default and needs resources.requests.cpu or resources.limits.cpu", name)
	}
	return nil
}

// hasResource reports whether pods get a request for the resource, either set
// or defaulted from the limit, which utilization targets are relative to.
func hasResource(resources *corev1.ResourceRequirements, name corev1.ResourceName) bool {
	if resources == nil {
		return false
	}
	_, request := resources.Requests[name]
	_, limit := resources.Limits[name]
	return request || limit
}

// loadedReplicasAPI reports how many replicas each loaded collection has.
type loadedReplicasAPI interface {
	LoadedReplicas(ctx context.Context, baseURL, password string) (map[string]int32, error)
}

// LoadedReplicas sums the replicas of each loaded collection over all
// resource groups. Every replica needs a query node of its own.
func (m milvusRESTClient) LoadedReplicas(ctx context.Context, baseURL, password string) (map[string]int32, error) {
	var groups struct {
		Data []string `json:"data"`
	}
	if err := m.call(ctx, baseURL+"/v2/vectordb/resource_groups/list", password, map[string]string{}, &groups); err != nil {
		return nil, err
	}
	replicas := map[string]int32{}
	for _, group := range groups.Data {
		var described struct {
			ResourceGroup struct {
				NumLoadedReplica map[string]int32 `json:"num_loaded_replica"`
			} `json:"resource_group"`
		}
		if err := m.call(ctx, baseURL+"/v2/vectordb/resource_groups/describe", password, map[string]string{"name": group}, &described); err != nil {
			return nil, err
		}
		for collection, n := range described.ResourceGroup.NumLoadedReplica {
			replicas[collection] += n
		}
	}
	return replicas, nil
}

// call posts body and decodes the response into out. Milvus reports API
// errors as HTTP 200 with a non-zero code.
func (m milvusRESTClient) call(ctx context.Context, url, password string, body, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+rootUsername+":"+password)

	resp, err := m.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("milvus returned HTTP %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	var status struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &status); err != nil {
		return fmt.Errorf("decode milvus response: %w", err)
	}
	if status.Code != 0 {
		return fmt.Errorf("milvus error %d: %s", status.Code, status.Message)
	}
	return json.Unmarshal(raw, out)
}

// replicaHosts are the components each replica of a loaded collection needs a
// pod of: Milvus 2.6 refuses to load more replicas than either has nodes.
var replicaHosts = []string{common.ComponentQueryNode, common.ComponentStreaming}

// validateReplicaHostsScaleIn refuses to run fewer query or streaming nodes
// than a loaded collection has replicas: the surplus replicas would lose their
// nodes and the collection would stop serving. Milvus is asked only when one
// of the floors drops; when it cannot answer, the change goes through.
func (p *Provider) validateReplicaHostsScaleIn(c *controller.Context) error {
	cr := &milvusapi.Milvus{}
	if err := c.Get(cr, c.Name()); err != nil || cr.Status.Status != milvusapi.StatusHealthy {
		return nil
	}
	shrinking := map[string]int32{}
	for _, name := range replicaHosts {
		requested, deployed := requestedNodes(c, name), deployedNodes(cr, name)
		if requested != nil && deployed != nil && *requested < *deployed {
			shrinking[name] = *requested
		}
	}
	if len(shrinking) == 0 {
		return nil
	}
	credentials := &corev1.Secret{}
	if err := c.Get(credentials, credentialsSecretName(c.Name())); err != nil {
		return nil
	}
	loaded, err := p.loadedReplicas.LoadedReplicas(c.Context(), milvusInternalURL(c), string(credentials.Data[secretKeyAppliedPassword]))
	if err != nil {
		log.FromContext(c.Context()).Info("Cannot check loaded replicas before scaling in", "error", err.Error())
		return nil
	}
	for _, name := range replicaHosts {
		requested, ok := shrinking[name]
		if !ok {
			continue
		}
		for _, collection := range slices.Sorted(maps.Keys(loaded)) {
			if loaded[collection] > requested {
				return fmt.Errorf("%s replicas cannot go below %d: collection %q is loaded with %d replicas; release it or reload it with fewer replicas first",
					name, loaded[collection], collection, loaded[collection])
			}
		}
	}
	return nil
}

// requestedNodes is the fewest pods the spec runs for the component: the
// groups' total, or the replicas (also the autoscaling minimum).
func requestedNodes(c *controller.Context, name string) *int32 {
	component, ok := c.Instance().Spec.Components[name]
	if !ok {
		return nil
	}
	if groups := componentParameters(c, name).Groups; len(groups) > 0 {
		var total int32
		for _, group := range groups {
			total += group.Replicas
		}
		return &total
	}
	return ptr.To(ptr.Deref(component.Replicas, 1))
}

func deployedNodes(cr *milvusapi.Milvus, name string) *int32 {
	var component *milvusapi.Component
	switch {
	case name == common.ComponentQueryNode && cr.Spec.Com.QueryNode != nil:
		component = &cr.Spec.Com.QueryNode.Component
	case name == common.ComponentStreaming && cr.Spec.Com.StreamingNode != nil:
		component = &cr.Spec.Com.StreamingNode.Component
	default:
		return nil
	}
	if component.HPA != nil && component.HPA.MinReplicas != nil {
		return component.HPA.MinReplicas
	}
	return component.Replicas
}
