package provider

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/definition/components"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

func componentParameters(c *controller.Context, name string) components.MilvusParameters {
	var params components.MilvusParameters
	c.TryDecodeComponentParameters(c.Instance().Spec.Components[name], &params)
	return params
}

// applyPodCustomization hands the pod-level settings to the operator, which
// renders them onto the component's pod template and Milvus container.
func applyPodCustomization(component *milvusapi.Component, pod *components.PodCustomization) {
	if pod == nil {
		return
	}
	component.PodAnnotations = pod.Annotations
	component.Env = withEnvDefaults(pod.Env)
	component.VolumeMounts = pod.VolumeMounts
	component.SecurityContext = pod.SecurityContext
	for _, raw := range pod.Volumes {
		var volume corev1.Volume
		component.Volumes = append(component.Volumes, withDefaults(raw, &volume, func() { defaultVolume(&volume) }))
	}
	for _, raw := range pod.InitContainers {
		var container corev1.Container
		component.InitContainers = append(component.InitContainers, withDefaults(raw, &container, func() {
			container.Env = withEnvDefaults(container.Env)
		}))
	}
}

// withEnvDefaults sets what the API server would default. The operator diffs
// the pod template it renders against the live Deployment, so a missing
// server default reads as a change on every reconcile and query nodes roll
// forever; the same holds for defaultVolume.
func withEnvDefaults(env []corev1.EnvVar) []corev1.EnvVar {
	for i := range env {
		if ref := env[i].ValueFrom; ref != nil && ref.FieldRef != nil && ref.FieldRef.APIVersion == "" {
			ref.FieldRef.APIVersion = "v1"
		}
	}
	return env
}

func defaultVolume(volume *corev1.Volume) {
	if volume.HostPath != nil && volume.HostPath.Type == nil {
		volume.HostPath.Type = ptr.To(corev1.HostPathUnset)
	}
	if volume.DownwardAPI != nil {
		if volume.DownwardAPI.DefaultMode == nil {
			volume.DownwardAPI.DefaultMode = ptr.To(corev1.DownwardAPIVolumeSourceDefaultMode)
		}
		defaultDownwardAPIItems(volume.DownwardAPI.Items)
	}
	if volume.Projected != nil {
		if volume.Projected.DefaultMode == nil {
			volume.Projected.DefaultMode = ptr.To(corev1.ProjectedVolumeSourceDefaultMode)
		}
		for _, source := range volume.Projected.Sources {
			if source.DownwardAPI != nil {
				defaultDownwardAPIItems(source.DownwardAPI.Items)
			}
		}
	}
}

func defaultDownwardAPIItems(items []corev1.DownwardAPIVolumeFile) {
	for i := range items {
		if ref := items[i].FieldRef; ref != nil && ref.APIVersion == "" {
			ref.APIVersion = "v1"
		}
	}
}

// withDefaults decodes a validated free-form object into target, applies the
// defaults and re-encodes it; an undecodable object is passed through as-is.
func withDefaults(raw map[string]any, target any, applyDefaults func()) milvusapi.Values {
	if err := decodeKubernetesObject(raw, target); err != nil {
		return raw
	}
	applyDefaults()
	defaulted, err := runtime.DefaultUnstructuredConverter.ToUnstructured(target)
	if err != nil {
		return raw
	}
	return defaulted
}

// validateComponentParameters rejects malformed component parameters, which
// would otherwise be dropped silently and deploy pods without the requested
// settings.
func validateComponentParameters(c *controller.Context) error {
	instanceComponents := c.Instance().Spec.Components
	for _, name := range slices.Sorted(maps.Keys(instanceComponents)) {
		component := instanceComponents[name]
		if component.Parameters == nil || component.Parameters.Raw == nil {
			continue
		}
		var params components.MilvusParameters
		if err := c.DecodeComponentParameters(component, &params); err != nil {
			return fmt.Errorf("component %q has invalid parameters: %w", name, err)
		}
		if err := validatePodCustomization(name, params.Pod); err != nil {
			return err
		}
		if err := validateDeploymentGroups(name, component.Replicas, params.Groups); err != nil {
			return err
		}
		if err := validateAutoscaling(name, component, params); err != nil {
			return err
		}
	}
	return nil
}

// validatePodCustomization checks the free-form Kubernetes objects strictly so
// a typo fails here instead of being pruned by the operator.
func validatePodCustomization(name string, pod *components.PodCustomization) error {
	if pod == nil {
		return nil
	}
	for i, env := range pod.Env {
		if env.Name == "" {
			return fmt.Errorf("component %q pod.env[%d].name is required", name, i)
		}
	}
	for i, raw := range pod.Volumes {
		var volume corev1.Volume
		if err := decodeKubernetesObject(raw, &volume); err != nil {
			return fmt.Errorf("component %q pod.volumes[%d] is invalid: %w", name, i, err)
		}
		if volume.Name == "" {
			return fmt.Errorf("component %q pod.volumes[%d].name is required", name, i)
		}
	}
	if pod.SecurityContext != nil {
		var securityContext corev1.SecurityContext
		if err := decodeKubernetesObject(pod.SecurityContext, &securityContext); err != nil {
			return fmt.Errorf("component %q pod.securityContext is invalid: %w", name, err)
		}
	}
	for i, raw := range pod.InitContainers {
		var container corev1.Container
		if err := decodeKubernetesObject(raw, &container); err != nil {
			return fmt.Errorf("component %q pod.initContainers[%d] is invalid: %w", name, i, err)
		}
		if container.Name == "" || container.Image == "" {
			return fmt.Errorf("component %q pod.initContainers[%d] requires name and image", name, i)
		}
	}
	return nil
}

func decodeKubernetesObject(raw map[string]any, target any) error {
	data, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}
