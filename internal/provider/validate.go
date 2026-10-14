package provider

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/definition/dependencies"
	"github.com/openeverest/provider-milvus/definition/topologies/cluster"
	"github.com/openeverest/provider-milvus/definition/topologies/standalone"
	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// Minimum per-component sizing. Values mirror the topology UI schema
// (definition/topologies/*/topology.yaml) so API and UI reject the same specs.
var (
	minComponentCPU    = resource.MustParse("100m")
	minComponentMemory = resource.MustParse("512Mi")
	minStorageSize     = resource.MustParse("1Gi")
)

// clusterCoordinators are the coordinator components required to have at least
// one replica in cluster mode.
var clusterCoordinators = []string{
	common.ComponentMixCoord,
}

// allowedComponentsForTopology returns the component names valid for the given
// normalized topology type.
func allowedComponentsForTopology(topologyType string) []string {
	if topologyType == "cluster" {
		return []string{
			common.ComponentProxy,
			common.ComponentMixCoord,
			common.ComponentDataNode,
			common.ComponentQueryNode,
			common.ComponentStreaming,
		}
	}
	return []string{common.ComponentStandalone}
}

// validateInstance runs all spec validations for the instance.
func validateInstance(c *controller.Context) error {
	instance := c.Instance()

	topologyType := "standalone"
	if instance.Spec.Topology != nil && instance.Spec.Topology.Type != "" {
		topologyType = normalizeTopologyName(instance.Spec.Topology.Type)
	}
	if topologyType != "standalone" && topologyType != "cluster" {
		requested := ""
		if instance.Spec.Topology != nil {
			requested = instance.Spec.Topology.Type
		}
		return fmt.Errorf("unsupported topology %q; expected standalone or cluster", requested)
	}

	if err := validateTopologyUnchanged(c, topologyType); err != nil {
		return err
	}

	if err := validateComponentsForTopology(instance.Spec.Components, topologyType); err != nil {
		return err
	}

	if err := validateTopologyRules(instance.Spec.Components, topologyType); err != nil {
		return err
	}

	if err := validateStorageNotDecreased(c, topologyType); err != nil {
		return err
	}

	if err := validateDependencies(c, topologyType); err != nil {
		return err
	}

	if err := validateRootPathsUnchanged(c); err != nil {
		return err
	}

	if err := validateNoDowngrade(c); err != nil {
		return err
	}

	if err := validateServiceExposure(instance.Spec.Components, topologyType); err != nil {
		return err
	}

	if _, err := milvusEngineConfig(c); err != nil {
		return err
	}

	if err := validateComponentParameters(c); err != nil {
		return err
	}

	if err := validateMonitoring(c); err != nil {
		return err
	}

	if topologyType == "cluster" {
		if err := validateMixCoordStandby(c); err != nil {
			return err
		}
	}

	return nil
}

// validateMixCoordStandby rejects several MixCoord replicas when the user's
// configuration disables active-standby: they would all try to be active.
func validateMixCoordStandby(c *controller.Context) error {
	replicas := c.Instance().Spec.Components[common.ComponentMixCoord].Replicas
	if replicas == nil || *replicas <= 1 {
		return nil
	}
	userConfig, err := milvusEngineConfig(c)
	if err != nil {
		return err
	}
	for _, section := range coordinatorConfigSections {
		settings, _ := userConfig[section].(map[string]any)
		if enabled, set := settings["enableActiveStandby"].(bool); set && !enabled {
			return fmt.Errorf("mixCoord replicas > 1 requires active-standby; remove %s.enableActiveStandby: false from the configuration", section)
		}
	}
	return nil
}

// validateTopologyUnchanged rejects switching an existing instance between
// standalone and cluster: the operator cannot migrate a running deployment.
func validateTopologyUnchanged(c *controller.Context, topologyType string) error {
	existing := &milvusapi.Milvus{}
	if err := c.Get(existing, c.Name()); err != nil || existing.Spec.Mode == "" {
		return nil
	}
	if current := string(existing.Spec.Mode); current != topologyType {
		return fmt.Errorf("topology cannot be changed from %s to %s", current, topologyType)
	}
	return nil
}

// validateServiceExposure rejects unsupported service types on the component
// that owns the client-facing service (standalone or proxy).
func validateServiceExposure(components map[string]corev1alpha1.ComponentSpec, topologyType string) error {
	name := exposedComponentName(topologyType)
	component := components[name]
	if component.Service == nil {
		return nil
	}
	switch component.Service.ServiceType {
	case "",
		corev1.ServiceTypeClusterIP,
		corev1.ServiceTypeLoadBalancer,
		corev1.ServiceTypeNodePort:
		return nil
	default:
		return fmt.Errorf("component %q service.serviceType must be one of ClusterIP, LoadBalancer or NodePort", name)
	}
}

// validateComponentsForTopology rejects components that do not belong to the
// topology and validates replicas, resource sizing, and request/limit balance
// for every allowed component that is present.
func validateComponentsForTopology(components map[string]corev1alpha1.ComponentSpec, topologyType string) error {
	allowed := allowedComponentsForTopology(topologyType)
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		allowedSet[name] = struct{}{}
	}

	for name := range components {
		if _, ok := allowedSet[name]; !ok {
			return fmt.Errorf("component %q is not valid for %s topology", name, topologyType)
		}
	}

	for _, name := range allowed {
		component, ok := components[name]
		if !ok {
			continue
		}
		if component.Replicas != nil && *component.Replicas < 1 {
			return fmt.Errorf("component %q replicas must be >= 1", name)
		}
		// Milvus components are stateless; their data lives in the object storage.
		if component.Storage != nil {
			return fmt.Errorf("component %q does not take storage; size the object storage with spec.topology.parameters.dependencies.storage.persistence.size", name)
		}
		if err := validateComponentResources(name, component); err != nil {
			return err
		}
	}

	return nil
}

// validateComponentResources enforces minimum CPU/RAM and request/limit balance
// when resources are specified. Unset resources fall back to operator defaults.
func validateComponentResources(name string, component corev1alpha1.ComponentSpec) error {
	if component.Resources == nil {
		return nil
	}

	limits := component.Resources.Limits
	if cpu, ok := limits[corev1.ResourceCPU]; ok && cpu.Cmp(minComponentCPU) < 0 {
		return fmt.Errorf("component %q resources.limits.cpu must be >= %s", name, minComponentCPU.String())
	}
	if mem, ok := limits[corev1.ResourceMemory]; ok && mem.Cmp(minComponentMemory) < 0 {
		return fmt.Errorf("component %q resources.limits.memory must be >= %s", name, minComponentMemory.String())
	}

	requests := component.Resources.Requests
	if err := validateRequestNotAboveLimit(name, corev1.ResourceCPU, requests, limits); err != nil {
		return err
	}
	if err := validateRequestNotAboveLimit(name, corev1.ResourceMemory, requests, limits); err != nil {
		return err
	}

	return nil
}

// validateRequestNotAboveLimit ensures a resource request does not exceed its
// limit when both are set.
func validateRequestNotAboveLimit(name string, resourceName corev1.ResourceName, requests, limits corev1.ResourceList) error {
	request, hasRequest := requests[resourceName]
	limit, hasLimit := limits[resourceName]
	if hasRequest && hasLimit && request.Cmp(limit) > 0 {
		return fmt.Errorf("component %q resources.requests.%s (%s) must not exceed resources.limits.%s (%s)",
			name, resourceName, request.String(), resourceName, limit.String())
	}
	return nil
}

// validateTopologyRules enforces topology-specific composition rules, such as
// requiring at least one replica for every coordinator in cluster mode.
func validateTopologyRules(components map[string]corev1alpha1.ComponentSpec, topologyType string) error {
	if topologyType != "cluster" {
		// Standalone embeds the coordinators; a second pod would compete with it.
		if replicas := components[common.ComponentStandalone].Replicas; replicas != nil && *replicas > 1 {
			return fmt.Errorf("standalone runs a single replica; use the cluster topology to scale out")
		}
		return nil
	}
	for _, name := range clusterCoordinators {
		component, ok := components[name]
		if !ok {
			continue
		}
		if component.Replicas != nil && *component.Replicas < 1 {
			return fmt.Errorf("cluster mode requires at least 1 replica for coordinator %q", name)
		}
	}
	return nil
}

// validateStorageNotDecreased guards against shrinking persistent storage on
// edit. It compares the requested size against the size already applied to the
// existing Milvus CR; storage may only grow or stay the same.
func validateStorageNotDecreased(c *controller.Context, topologyType string) error {
	requested := requestedStorageSize(c, topologyType)
	if requested == "" {
		return nil
	}
	requestedQty, err := resource.ParseQuantity(requested)
	if err != nil {
		return fmt.Errorf("invalid storage size %q: %w", requested, err)
	}

	existing := &milvusapi.Milvus{}
	if err := c.Get(existing, c.Name()); err != nil {
		// No existing CR yet (create path) or transient read error: nothing to compare.
		return nil
	}
	currentSize := currentStorageSize(existing)
	if currentSize == "" {
		return nil
	}
	currentQty, err := resource.ParseQuantity(currentSize)
	if err != nil {
		return nil
	}

	if requestedQty.Cmp(currentQty) < 0 {
		return fmt.Errorf("storage size cannot be decreased from %s to %s", currentQty.String(), requestedQty.String())
	}
	return nil
}

// requestedStorageSize returns the object-storage PVC size the spec would apply,
// mirroring buildStorage: the explicit storage dependency persistence size, or
// the default. External storage has no bundled PVC.
func requestedStorageSize(c *controller.Context, topologyType string) string {
	storageParam := storageDependencyParam(c, topologyType)
	if storageParam != nil && storageParam.External {
		return ""
	}
	if storageParam != nil && storageParam.Persistence != nil && storageParam.Persistence.Size != "" {
		return storageParam.Persistence.Size
	}
	return defaultStoragePersistence
}

// storageDependencyParam decodes the storage dependency parameter for the
// topology, returning nil when unset.
func storageDependencyParam(c *controller.Context, topologyType string) *dependencies.Storage {
	if topologyType == "cluster" {
		var params cluster.ClusterTopologyParameters
		if c.TryDecodeTopologyParameters(&params) && params.Dependencies != nil {
			return params.Dependencies.Storage
		}
		return nil
	}
	var params standalone.StandaloneTopologyParameters
	if c.TryDecodeTopologyParameters(&params) && params.Dependencies != nil {
		return params.Dependencies.Storage
	}
	return nil
}

// etcdDependencyParam decodes the etcd dependency parameter for the topology,
// returning nil when unset.
func etcdDependencyParam(c *controller.Context, topologyType string) *dependencies.Etcd {
	if topologyType == "cluster" {
		return clusterDependencyParams(c).Etcd
	}
	var params standalone.StandaloneTopologyParameters
	if c.TryDecodeTopologyParameters(&params) && params.Dependencies != nil {
		return params.Dependencies.Etcd
	}
	return nil
}

// validateRootPathsUnchanged rejects moving an existing instance's metadata or
// object prefix: Milvus would start empty and orphan what it wrote before.
func validateRootPathsUnchanged(c *controller.Context) error {
	existing := &milvusapi.Milvus{}
	if err := c.Get(existing, c.Name()); err != nil || existing.Spec.Mode == "" {
		return nil
	}
	desired, err := BuildMilvusSpec(c)
	if err != nil {
		return err
	}
	for _, prefix := range []struct{ section, defaultValue string }{
		{section: "etcd", defaultValue: c.Name()},
		{section: "minio", defaultValue: defaultMinioRootPath},
	} {
		current := rootPathOrDefault(existing.Spec.Conf, prefix.section, prefix.defaultValue)
		requested := rootPathOrDefault(desired.Conf, prefix.section, prefix.defaultValue)
		if current != requested {
			return fmt.Errorf("%s rootPath cannot be changed from %q to %q", prefix.section, current, requested)
		}
	}
	return nil
}

// defaultMinioRootPath is the operator's object prefix when none is set.
const defaultMinioRootPath = "files"

func rootPathOrDefault(config milvusapi.Values, section, defaultValue string) string {
	settings, _ := config[section].(map[string]any)
	if rootPath, _ := settings["rootPath"].(string); rootPath != "" {
		return rootPath
	}
	return defaultValue
}

// currentStorageSize extracts the persistence size applied to an existing
// Milvus CR, mirroring the layout written by buildStorage.
func currentStorageSize(m *milvusapi.Milvus) string {
	if m.Spec.Dep == nil || m.Spec.Dep.Storage.InCluster == nil {
		return ""
	}
	persistence, ok := m.Spec.Dep.Storage.InCluster.Values["persistence"].(map[string]any)
	if !ok {
		return ""
	}
	size, ok := persistence["size"].(string)
	if !ok {
		return ""
	}
	return size
}

// validateDependencies validates the bundled/external dependency configuration
// carried in the topology parameters.
func validateDependencies(c *controller.Context, topologyType string) error {
	var etcd *dependencies.Etcd
	var pulsar *dependencies.Pulsar
	var storage *dependencies.Storage

	if topologyType == "cluster" {
		var params cluster.ClusterTopologyParameters
		if err := decodeTopologyParametersIfPresent(c, &params); err != nil {
			return err
		}
		requestedStream := ""
		if params.Dependencies != nil {
			etcd = params.Dependencies.Etcd
			storage = params.Dependencies.Storage
			requestedStream = params.Dependencies.MessageStreamType
		}
		if err := validateMessageStream(c, requestedStream); err != nil {
			return err
		}
		if params.Dependencies != nil && messageStreamType(c, requestedStream) == dependencies.MessageStreamPulsar {
			pulsar = params.Dependencies.Pulsar
		}
	} else {
		var params standalone.StandaloneTopologyParameters
		if err := decodeTopologyParametersIfPresent(c, &params); err != nil {
			return err
		}
		if params.Dependencies != nil {
			etcd = params.Dependencies.Etcd
			storage = params.Dependencies.Storage
		}
	}

	if err := validateEtcdDependency(etcd); err != nil {
		return err
	}
	if err := validatePulsarDependency(pulsar); err != nil {
		return err
	}
	return validateStorageDependency(storage)
}

// validateMessageStream accepts the supported write-ahead logs and rejects
// switching an existing instance, which would lose its WAL.
func validateMessageStream(c *controller.Context, requested string) error {
	switch requested {
	case "", dependencies.MessageStreamWoodpecker, dependencies.MessageStreamPulsar:
	default:
		return fmt.Errorf("messageStreamType must be %s or %s", dependencies.MessageStreamWoodpecker, dependencies.MessageStreamPulsar)
	}
	if requested == "" {
		return nil
	}
	if existing := existingMessageStreamType(c); existing != "" && existing != requested {
		return fmt.Errorf("message stream cannot be changed from %s to %s; set messageStreamType to %s", existing, requested, existing)
	}
	return nil
}

// decodeTopologyParametersIfPresent decodes the instance's topology parameters
// when they are set, surfacing malformed input as an error instead of silently
// falling back to defaults (which would deploy a spec that ignores the request).
func decodeTopologyParametersIfPresent(c *controller.Context, target any) error {
	topology := c.Instance().Spec.Topology
	if topology == nil || topology.Parameters == nil || topology.Parameters.Raw == nil {
		return nil
	}
	if err := c.DecodeTopologyParameters(target); err != nil {
		return fmt.Errorf("invalid topology parameters: %w", err)
	}
	return nil
}

func validateEtcdDependency(etcd *dependencies.Etcd) error {
	if etcd == nil {
		return nil
	}
	if etcd.External {
		if len(etcd.Endpoints) == 0 {
			return fmt.Errorf("etcd.endpoints is required when etcd.external is true")
		}
		return nil
	}
	if err := validateDependencyReplicas("etcd", etcd.Replicas); err != nil {
		return err
	}
	if err := validateDependencyResources("etcd", etcd.Resources); err != nil {
		return err
	}
	return validatePersistence("etcd.persistence", etcd.Persistence)
}

func validatePulsarDependency(pulsar *dependencies.Pulsar) error {
	if pulsar == nil {
		return nil
	}
	if pulsar.External {
		if pulsar.Endpoint == "" {
			return fmt.Errorf("pulsar.endpoint is required when pulsar.external is true")
		}
		return nil
	}
	if err := validatePulsarComponent("pulsar.broker", pulsar.Broker); err != nil {
		return err
	}
	if err := validatePulsarComponent("pulsar.proxy", pulsar.Proxy); err != nil {
		return err
	}
	if bk := pulsar.BookKeeper; bk != nil {
		if err := validateDependencyReplicas("pulsar.bookkeeper", bk.Replicas); err != nil {
			return err
		}
		if err := validateDependencyResources("pulsar.bookkeeper", bk.Resources); err != nil {
			return err
		}
		if err := validatePersistence("pulsar.bookkeeper.journal", bk.Journal); err != nil {
			return err
		}
		if err := validatePersistence("pulsar.bookkeeper.ledgers", bk.Ledgers); err != nil {
			return err
		}
	}
	if zk := pulsar.ZooKeeper; zk != nil {
		if err := validateDependencyReplicas("pulsar.zookeeper", zk.Replicas); err != nil {
			return err
		}
		if err := validateDependencyResources("pulsar.zookeeper", zk.Resources); err != nil {
			return err
		}
		if err := validatePersistence("pulsar.zookeeper.data", zk.Data); err != nil {
			return err
		}
	}
	return nil
}

// validatePulsarComponent validates a stateless Pulsar sub-component (broker or
// proxy).
func validatePulsarComponent(name string, component *dependencies.PulsarComponent) error {
	if component == nil {
		return nil
	}
	if err := validateDependencyReplicas(name, component.Replicas); err != nil {
		return err
	}
	return validateDependencyResources(name, component.Resources)
}

func validateStorageDependency(storage *dependencies.Storage) error {
	if storage == nil {
		return nil
	}
	if storage.External {
		if storage.Endpoint == "" {
			return fmt.Errorf("storage.endpoint is required when storage.external is true")
		}
		if storage.CredentialsSecret == "" && !storage.UseIAM {
			return fmt.Errorf("storage.credentialsSecret is required when storage.external is true, unless storage.useIAM is set")
		}
		switch storage.Type {
		case "", "MinIO", "S3", "Azure":
			return nil
		default:
			return fmt.Errorf("storage.type must be one of MinIO, S3 or Azure")
		}
	}
	if err := validateDependencyReplicas("storage", storage.Replicas); err != nil {
		return err
	}
	if err := validateDependencyResources("storage", storage.Resources); err != nil {
		return err
	}
	return validatePersistence("storage.persistence", storage.Persistence)
}

// validatePersistence enforces the minimum PVC size when a dependency
// persistence size is specified.
func validatePersistence(name string, persistence *dependencies.Persistence) error {
	if persistence == nil || persistence.Size == "" {
		return nil
	}
	size, err := resource.ParseQuantity(persistence.Size)
	if err != nil {
		return fmt.Errorf("%s.size %q is invalid: %w", name, persistence.Size, err)
	}
	if size.Cmp(minStorageSize) < 0 {
		return fmt.Errorf("%s.size must be >= %s", name, minStorageSize.String())
	}
	return nil
}

func validateDependencyReplicas(name string, replicas *int32) error {
	if replicas != nil && *replicas < 1 {
		return fmt.Errorf("%s.replicas must be >= 1", name)
	}
	return nil
}

// validateDependencyResources parses the CPU/memory quantity strings and
// ensures requests do not exceed limits.
func validateDependencyResources(name string, resources *dependencies.Resources) error {
	if resources == nil {
		return nil
	}
	requests, err := parseResourceList(name, "requests", resources.Requests)
	if err != nil {
		return err
	}
	limits, err := parseResourceList(name, "limits", resources.Limits)
	if err != nil {
		return err
	}
	if err := validateRequestNotAboveLimit(name, corev1.ResourceCPU, requests, limits); err != nil {
		return err
	}
	return validateRequestNotAboveLimit(name, corev1.ResourceMemory, requests, limits)
}

// parseResourceList converts a dependency ResourceList into a corev1.ResourceList,
// validating that every provided quantity string is well-formed.
func parseResourceList(name, kind string, list *dependencies.ResourceList) (corev1.ResourceList, error) {
	result := corev1.ResourceList{}
	if list == nil {
		return result, nil
	}
	if list.CPU != "" {
		cpu, err := resource.ParseQuantity(string(list.CPU))
		if err != nil {
			return nil, fmt.Errorf("%s.resources.%s.cpu %q is invalid: %w", name, kind, list.CPU, err)
		}
		result[corev1.ResourceCPU] = cpu
	}
	if list.Memory != "" {
		mem, err := resource.ParseQuantity(string(list.Memory))
		if err != nil {
			return nil, fmt.Errorf("%s.resources.%s.memory %q is invalid: %w", name, kind, list.Memory, err)
		}
		result[corev1.ResourceMemory] = mem
	}
	return result, nil
}
