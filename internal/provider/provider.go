package provider

import (
	"fmt"
	"sort"
	"strings"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/openeverest/provider-milvus/definition/components"
	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// Compile-time check that Provider implements the required interface.
var _ controller.ProviderInterface = (*Provider)(nil)

// Provider implements controller.ProviderInterface for the provider-milvus provider.
type Provider struct {
	controller.BaseProvider
	rootPassword rootPasswordAPI
}

// New creates a new Provider instance.
func New() *Provider {
	return &Provider{
		BaseProvider: controller.BaseProvider{
			ProviderName: common.ProviderName,
			SchemeFuncs: []func(*runtime.Scheme) error{
				milvusapi.AddToScheme,
				monitoringv1.AddToScheme,
				batchv1.AddToScheme,
			},
			WatchConfigs: []controller.WatchConfig{
				controller.WatchOwned(&milvusapi.Milvus{}),
				controller.WatchOwned(&corev1.Secret{}, credentialsSecretChanged()),
				// Standalone Restores are not owned by the Instance they target.
				controller.WatchExternal(&backupv1alpha1.Restore{},
					handler.EnqueueRequestsFromMapFunc(enqueueRestoreInstance)),
				// A finished scheduled run is what retention acts on.
				controller.WatchExternal(&backupv1alpha1.Backup{},
					handler.EnqueueRequestsFromMapFunc(enqueueScheduledBackupInstance)),
			},
		},
		rootPassword: newMilvusRESTClient(),
	}
}

func normalizeTopologyName(t string) string {
	switch strings.ToLower(t) {
	case "", "standalone":
		return "standalone"
	case "cluster":
		return "cluster"
	default:
		return strings.ToLower(t)
	}
}

func makeComponentReplica(replicas int32) *int32 {
	return ptr.To(replicas)
}

func componentReplicasOrDefault(components map[string]corev1alpha1.ComponentSpec, name string, defaultReplicas int32) *int32 {
	component := components[name]
	if component.Replicas != nil {
		return component.Replicas
	}
	return makeComponentReplica(defaultReplicas)
}

func componentResourcesOrNil(components map[string]corev1alpha1.ComponentSpec, name string) *corev1.ResourceRequirements {
	component := components[name]
	if component.Resources == nil || (len(component.Resources.Limits) == 0 && len(component.Resources.Requests) == 0) {
		return nil
	}
	return &corev1.ResourceRequirements{
		Limits:   component.Resources.Limits.DeepCopy(),
		Requests: component.Resources.Requests.DeepCopy(),
	}
}

// makeMilvusComponent labels the component's pods so the runtime counts them
// into the Instance's status.components.
func makeMilvusComponent(c *controller.Context, name string, image milvusImage) milvusapi.Component {
	components := c.Instance().Spec.Components
	component := milvusapi.Component{
		ComponentSpec: milvusapi.ComponentSpec{
			Image:     image.image,
			Version:   image.version,
			Resources: componentResourcesOrNil(components, name),
			PodLabels: c.PodLabels(name),
		},
		Replicas: componentReplicasOrDefault(components, name, 1),
	}
	applySchedulingPolicy(&component.ComponentSpec, components[name].SchedulingPolicy)
	applyPodCustomization(&component, componentParameters(c, name).Pod)
	return component
}

// milvusImage is a container image plus the plain semver the operator gates
// features on; it cannot parse build suffixes such as "-gpu".
type milvusImage struct {
	image   string
	version string
}

// componentImageResolver picks each component's image: an explicit
// spec.components.<name>.image wins, then the catalog image of the component's
// version (pinned, or taken from the version bundle), then the instance-wide
// fallback.
type componentImageResolver struct {
	providerSpec *corev1alpha1.ProviderSpec
	bundle       *corev1alpha1.VersionBundle
	components   map[string]corev1alpha1.ComponentSpec
	fallback     milvusImage
}

func (r componentImageResolver) resolve(name string) milvusImage {
	component := r.components[name]
	resolved := r.fallback
	version := component.Version
	if version == "" && r.bundle != nil {
		version = r.bundle.Components[name]
	}
	if version != "" {
		if image := controller.GetImageForVersion(r.providerSpec, name, version); image != "" {
			resolved = milvusImage{image: image, version: operatorVersion(version)}
		}
	}
	if component.Image != "" {
		resolved.image = component.Image
	}
	return resolved
}

func operatorVersion(version string) string {
	semver, _, _ := strings.Cut(version, "-")
	return semver
}

// milvusEngineConfig collects the `configuration` YAML from every component's
// parameters and deep-merges it into a single Values map. Milvus uses one shared
// engine config (spec.config), so configuration provided on any component is
// merged; components are processed in sorted name order for deterministic output.
func milvusEngineConfig(c *controller.Context) (milvusapi.Values, error) {
	instanceComponents := c.Instance().Spec.Components
	names := make([]string, 0, len(instanceComponents))
	for name := range instanceComponents {
		names = append(names, name)
	}
	sort.Strings(names)

	merged := milvusapi.Values{}
	for _, name := range names {
		var params components.MilvusParameters
		if !c.TryDecodeComponentParameters(instanceComponents[name], &params) || params.Configuration == "" {
			continue
		}
		parsed := map[string]any{}
		if err := yaml.Unmarshal([]byte(params.Configuration), &parsed); err != nil {
			return nil, fmt.Errorf("component %q has invalid configuration: %w", name, err)
		}
		deepMergeValues(merged, parsed)
	}
	if len(merged) == 0 {
		return nil, nil
	}
	return merged, nil
}

// coordinatorConfigSections are the roles hosted by MixCoord; each must allow
// active-standby for a second MixCoord replica to act as a hot standby and for
// the operator to roll coordinators without downtime.
var coordinatorConfigSections = []string{"rootCoord", "dataCoord", "indexCoord", "queryCoord"}

// withActiveStandbyDefaults enables active-standby on every coordinator role
// unless the user's configuration sets it explicitly.
func withActiveStandbyDefaults(userConfig milvusapi.Values) milvusapi.Values {
	config := milvusapi.Values{}
	for _, section := range coordinatorConfigSections {
		config[section] = map[string]any{"enableActiveStandby": true}
	}
	deepMergeValues(config, userConfig)
	return config
}

// deepMergeValues recursively merges src into dst. Nested maps are merged;
// any non-map value in src overrides the corresponding key in dst.
func deepMergeValues(dst, src map[string]any) {
	for key, srcVal := range src {
		srcMap, srcIsMap := srcVal.(map[string]any)
		dstMap, dstIsMap := dst[key].(map[string]any)
		if srcIsMap && dstIsMap {
			deepMergeValues(dstMap, srcMap)
			continue
		}
		dst[key] = srcVal
	}
}

func resolveMilvusImage(c *controller.Context, componentName, version string) string {
	providerSpec, err := c.ProviderSpec()
	if err != nil || providerSpec == nil {
		return ""
	}
	if version != "" {
		if image := controller.GetImageForVersion(providerSpec, componentName, version); image != "" {
			return image
		}
	}
	return controller.GetDefaultImageForComponent(providerSpec, componentName)
}

func BuildMilvusSpec(c *controller.Context) (milvusapi.MilvusSpec, error) {
	instance := c.Instance()
	topologyType := "standalone"
	if instance.Spec.Topology != nil && instance.Spec.Topology.Type != "" {
		topologyType = normalizeTopologyName(instance.Spec.Topology.Type)
	}

	providerSpec, err := c.ProviderSpec()
	if err != nil {
		return milvusapi.MilvusSpec{}, err
	}

	resolvedVersion := controller.EffectiveVersionBundleName(providerSpec, instance)
	var bundle *corev1alpha1.VersionBundle
	if resolvedVersion != "" {
		bundle, _ = controller.ResolveVersionBundle(providerSpec, resolvedVersion)
	}
	if resolvedVersion == "" {
		resolvedVersion = "2.6.15"
	}

	baseImage := "milvusdb/milvus:v2.6.15"
	if image := resolveMilvusImage(c, common.ComponentStandalone, resolvedVersion); image != "" {
		baseImage = image
	}
	images := componentImageResolver{
		providerSpec: providerSpec,
		bundle:       bundle,
		components:   instance.Spec.Components,
		fallback:     milvusImage{image: baseImage, version: operatorVersion(resolvedVersion)},
	}

	mode := milvusapi.MilvusModeStandalone
	if topologyType == "cluster" {
		mode = milvusapi.MilvusModeCluster
	}

	spec := milvusapi.MilvusSpec{
		Mode: mode,
		Com: milvusapi.MilvusComponents{
			ComponentSpec: milvusapi.ComponentSpec{
				Image:   images.fallback.image,
				Version: images.fallback.version,
			},
			// The provider owns the PodMonitor (see syncPodMonitor).
			DisableMetric: true,
		},
	}

	engineConfig, err := milvusEngineConfig(c)
	if err != nil {
		return milvusapi.MilvusSpec{}, err
	}
	if mode == milvusapi.MilvusModeCluster {
		engineConfig = withActiveStandbyDefaults(engineConfig)
	}
	spec.Conf = engineConfig
	storageParam := storageDependencyParam(c, topologyType)
	for _, dependencyConfig := range []milvusapi.Values{
		externalStorageConfig(storageParam),
		externalEtcdConfig(etcdDependencyParam(c, topologyType)),
	} {
		if dependencyConfig == nil {
			continue
		}
		if spec.Conf == nil {
			spec.Conf = milvusapi.Values{}
		}
		deepMergeValues(spec.Conf, dependencyConfig)
	}
	spec.Com.ServiceAccountName = storageServiceAccount(storageParam)

	if topologyType == "standalone" {
		spec.Com.Standalone = &milvusapi.MilvusStandalone{
			ServiceComponent: milvusapi.ServiceComponent{
				Component: makeMilvusComponent(c, common.ComponentStandalone, images.resolve(common.ComponentStandalone)),
				Port:      19530,
			},
		}
		applyServiceExposure(&spec.Com.Standalone.ServiceComponent, instance.Spec.Components[common.ComponentStandalone].Service)
		spec.Dep = buildDependencies(c, topologyType)
		applyImageUpdateMode(&spec.Com)
		return spec, nil
	}

	spec.Com.Proxy = &milvusapi.MilvusProxy{
		ServiceComponent: milvusapi.ServiceComponent{
			Component: makeMilvusComponent(c, common.ComponentProxy, images.resolve(common.ComponentProxy)),
			Port:      19530,
		},
	}
	applyServiceExposure(&spec.Com.Proxy.ServiceComponent, instance.Spec.Components[common.ComponentProxy].Service)
	spec.Com.Proxy.Groups = applyDeploymentGroups(c, common.ComponentProxy, &spec.Com.Proxy.Component)

	spec.Com.MixCoord = &milvusapi.MilvusMixCoord{Component: makeMilvusComponent(c, common.ComponentMixCoord, images.resolve(common.ComponentMixCoord))}
	spec.Com.DataNode = &milvusapi.MilvusDataNode{Component: makeMilvusComponent(c, common.ComponentDataNode, images.resolve(common.ComponentDataNode))}
	spec.Com.DataNode.Groups = applyDeploymentGroups(c, common.ComponentDataNode, &spec.Com.DataNode.Component)
	spec.Com.QueryNode = &milvusapi.MilvusQueryNode{Component: makeMilvusComponent(c, common.ComponentQueryNode, images.resolve(common.ComponentQueryNode))}
	spec.Com.QueryNode.Groups = applyDeploymentGroups(c, common.ComponentQueryNode, &spec.Com.QueryNode.Component)
	spec.Com.StreamingNode = &milvusapi.MilvusStreamingNode{Component: makeMilvusComponent(c, common.ComponentStreaming, images.resolve(common.ComponentStreaming))}
	spec.Com.StreamingNode.Groups = applyDeploymentGroups(c, common.ComponentStreaming, &spec.Com.StreamingNode.Component)
	spec.Dep = buildDependencies(c, topologyType)
	applyImageUpdateMode(&spec.Com)

	return spec, nil
}

// Validate checks if the Instance spec is valid.
func (p *Provider) Validate(c *controller.Context) error {
	l := log.FromContext(c.Context())
	l.Info("Validating instance", "name", c.Name())

	return validateInstance(c)
}

// Sync ensures all required resources exist and are configured correctly.
func (p *Provider) Sync(c *controller.Context) error {
	l := log.FromContext(c.Context())
	l.Info("Syncing instance", "name", c.Name())

	creds, err := ensureCredentials(c)
	if err != nil {
		return err
	}

	spec, err := BuildMilvusSpec(c)
	if err != nil {
		return err
	}
	applyAuthConfig(&spec, credentialsSecretName(c.Name()))
	if err := applyStorageCredentials(c, &spec); err != nil {
		return err
	}

	cr := &milvusapi.Milvus{
		ObjectMeta: c.ObjectMeta(c.Name()),
		Spec:       spec,
	}
	if err := c.Apply(cr); err != nil {
		return err
	}
	if err := syncPodMonitor(c); err != nil {
		return err
	}
	if err := syncDataSource(c); err != nil {
		return err
	}
	if err := p.syncRootPassword(c, creds); err != nil {
		return err
	}
	// Last: a schedule error is a BackupConfigError, which ends the pass.
	return syncBackupSchedules(c)
}

// Status computes the current status of the database instance.
func (p *Provider) Status(c *controller.Context) (controller.Status, error) {
	l := log.FromContext(c.Context())
	l.Info("Computing status", "name", c.Name())

	cr := &milvusapi.Milvus{}
	if err := c.Get(cr, c.Name()); err != nil {
		return controller.Provisioning("waiting for Milvus CR to be created"), nil
	}
	// Connection details are published only once seeding has finished.
	if ds := c.GetDataSourceStatus(); ds != nil && !ds.Done {
		return controller.Restoring(ds.Message), nil
	}
	restoring, err := activeRestore(c)
	if err != nil {
		return controller.Status{}, err
	}
	if restoring {
		return controller.Restoring("a restore is running"), nil
	}

	switch cr.Status.Status {
	case milvusapi.StatusHealthy:
		host, port, ready, message := resolveEndpoint(c, cr)
		if !ready {
			return controller.Provisioning(message), nil
		}

		creds, err := ensureCredentials(c)
		if err != nil {
			return controller.Status{}, err
		}
		// Clients keep the working password until a rotation lands.
		password := creds.appliedPassword

		details := controller.ConnectionDetails{
			Type:     "milvus",
			Provider: common.ProviderName,
			Host:     host,
			Port:     port,
			Username: rootUsername,
			Password: password,
			URI:      fmt.Sprintf("http://%s:%s", host, port),
			AdditionalProperties: map[string]string{
				"token": fmt.Sprintf("%s:%s", rootUsername, password),
			},
		}
		// Milvus keeps serving during a rollout, so the connection stays published.
		if message, rolling := rolloutMessage(cr); rolling {
			status := controller.Updating(message)
			status.ConnectionDetails = details
			return status, nil
		}
		return controller.ReadyWithConnectionDetails(details), nil
	case milvusapi.StatusPending, milvusapi.StatusDeleting:
		return controller.Provisioning(notReadyMessage(cr, "Milvus is being initialized or updated")), nil
	case milvusapi.StatusStopped:
		return controller.Pending("Milvus is stopped"), nil
	case milvusapi.StatusUnhealthy:
		// Query nodes roll blue/green, so Milvus reports Unhealthy mid-rollout.
		if message, rolling := rolloutMessage(cr); rolling {
			return controller.Updating(notReadyMessage(cr, message)), nil
		}
		return controller.Provisioning(notReadyMessage(cr, "Milvus is unhealthy")), nil
	default:
		return controller.Provisioning("Milvus is initializing"), nil
	}
}

// notReadyConditions are checked in dependency order, so the root cause wins
// over the Milvus components waiting on it.
var notReadyConditions = []string{"EtcdReady", "StorageReady", "MsgStreamReady", "MilvusReady"}

// notReadyMessage names the first failing operator condition, or returns
// fallback when none is reported.
func notReadyMessage(cr *milvusapi.Milvus, fallback string) string {
	for _, conditionType := range notReadyConditions {
		for _, condition := range cr.Status.Conditions {
			if condition.Type != conditionType || condition.Status != corev1.ConditionFalse {
				continue
			}
			detail := condition.Message
			if detail == "" {
				detail = condition.Reason
			}
			return fmt.Sprintf("%s: %s: %s", fallback, conditionType, detail)
		}
	}
	return fallback
}

// rolloutMessage reports whether the operator is still rolling out the
// applied spec, such as an upgrade, and describes its progress.
func rolloutMessage(cr *milvusapi.Milvus) (string, bool) {
	if cr.Status.ObservedGeneration < cr.Generation {
		return "Milvus is applying the latest changes", true
	}
	for _, condition := range cr.Status.Conditions {
		if condition.Type != milvusapi.ConditionMilvusUpdated || condition.Status != corev1.ConditionFalse {
			continue
		}
		if condition.Message != "" {
			return condition.Message, true
		}
		return "Milvus is updating: " + condition.Reason, true
	}
	return "", false
}

// Cleanup handles deletion of provider-managed resources.
func (p *Provider) Cleanup(c *controller.Context) error {
	l := log.FromContext(c.Context())
	l.Info("Cleaning up instance", "name", c.Name())
	return nil
}
