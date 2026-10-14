package provider

import (
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"k8s.io/utils/ptr"

	"github.com/openeverest/provider-milvus/definition/dependencies"
	"github.com/openeverest/provider-milvus/definition/topologies/cluster"
	"github.com/openeverest/provider-milvus/definition/topologies/standalone"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// Per-topology defaults keep bundled dependencies small so they don't
// over-provision on modest clusters. Users override any field via topology
// parameters; unset fields fall back to these values.
var (
	defaultStandaloneEtcdReplicas int32 = 1
	defaultClusterEtcdReplicas    int32 = 3

	defaultEtcdResources = &dependencies.Resources{
		Requests: &dependencies.ResourceList{CPU: "100m", Memory: "256Mi"},
	}
	// defaultEtcdPersistence keeps the etcd data PVC modest instead of the
	// chart's larger default.
	defaultEtcdPersistence = "10Gi"

	defaultStorageResources = &dependencies.Resources{
		Requests: &dependencies.ResourceList{CPU: "100m", Memory: "256Mi"},
	}
	defaultStorageReplicas int32 = 1
	// defaultStoragePersistence sizes MinIO when no data-bearing component
	// storage is provided.
	defaultStoragePersistence = "10Gi"

	defaultPulsarBroker = dependencies.PulsarComponent{
		Replicas:  ptr.To(int32(1)),
		Resources: &dependencies.Resources{Requests: &dependencies.ResourceList{CPU: "200m", Memory: "512Mi"}},
	}
	defaultPulsarProxy = dependencies.PulsarComponent{
		Replicas:  ptr.To(int32(1)),
		Resources: &dependencies.Resources{Requests: &dependencies.ResourceList{CPU: "100m", Memory: "256Mi"}},
	}
	// Pulsar bookie and zookeeper volumes default to the chart's very large
	// sizes (journal 100Gi, ledgers 200Gi, data 20Gi); pin modest predictable
	// values here so a fresh cluster does not over-provision.
	defaultPulsarBookKeeper = dependencies.PulsarBookKeeper{
		Replicas:  ptr.To(int32(2)),
		Resources: &dependencies.Resources{Requests: &dependencies.ResourceList{CPU: "200m", Memory: "512Mi"}},
		Journal:   &dependencies.Persistence{Size: "5Gi"},
		Ledgers:   &dependencies.Persistence{Size: "10Gi"},
	}
	defaultPulsarZooKeeper = dependencies.PulsarZooKeeper{
		Replicas:  ptr.To(int32(1)),
		Resources: &dependencies.Resources{Requests: &dependencies.ResourceList{CPU: "100m", Memory: "256Mi"}},
		Data:      &dependencies.Persistence{Size: "5Gi"},
	}
)

// buildDependencies renders the Milvus dependency spec (etcd, Pulsar, MinIO)
// from the topology parameters, applying per-topology defaults and honouring
// external-dependency overrides.
func buildDependencies(c *controller.Context, topologyType string) *milvusapi.MilvusDependencies {
	if topologyType != "cluster" {
		var params standalone.StandaloneTopologyParameters
		var etcdParam *dependencies.Etcd
		var storageParam *dependencies.Storage
		if c.TryDecodeTopologyParameters(&params) && params.Dependencies != nil {
			etcdParam = params.Dependencies.Etcd
			storageParam = params.Dependencies.Storage
		}
		return &milvusapi.MilvusDependencies{
			Etcd:    buildEtcd(etcdParam, topologyType),
			Storage: buildStorage(storageParam),
		}
	}

	deps := clusterDependencyParams(c)
	dep := &milvusapi.MilvusDependencies{
		Etcd:          buildEtcd(deps.Etcd, topologyType),
		Storage:       buildStorage(deps.Storage),
		MsgStreamType: messageStreamType(c, deps.MessageStreamType),
	}
	if dep.MsgStreamType == dependencies.MessageStreamPulsar {
		dep.Pulsar = buildPulsar(deps.Pulsar)
	}
	return dep
}

// clusterDependencyParams decodes the cluster dependency parameters, returning
// an empty set when none are given.
func clusterDependencyParams(c *controller.Context) cluster.ClusterDependencies {
	var params cluster.ClusterTopologyParameters
	if c.TryDecodeTopologyParameters(&params) && params.Dependencies != nil {
		return *params.Dependencies
	}
	return cluster.ClusterDependencies{}
}

// messageStreamType resolves the cluster's write-ahead log. An existing
// instance keeps the stream it was created with, because switching loses the
// WAL; new instances default to Woodpecker, which needs no extra dependency.
func messageStreamType(c *controller.Context, requested string) string {
	if requested != "" {
		return requested
	}
	if existing := existingMessageStreamType(c); existing != "" {
		return existing
	}
	return dependencies.MessageStreamWoodpecker
}

// existingMessageStreamType reports the stream of the already-applied Milvus
// CR. CRs created before the stream was selectable carry only the operator's
// Pulsar default.
func existingMessageStreamType(c *controller.Context) string {
	existing := &milvusapi.Milvus{}
	if err := c.Get(existing, c.Name()); err != nil || existing.Spec.Dep == nil {
		return ""
	}
	if existing.Spec.Dep.MsgStreamType != "" {
		return existing.Spec.Dep.MsgStreamType
	}
	if existing.Spec.Dep.Pulsar.InCluster != nil || existing.Spec.Dep.Pulsar.External {
		return dependencies.MessageStreamPulsar
	}
	return ""
}

// externalStorageConfig is the engine config an external object store needs
// beyond the dependency spec: bucket, prefix, TLS, region and credentials mode.
func externalStorageConfig(param *dependencies.Storage) milvusapi.Values {
	if param == nil || !param.External {
		return nil
	}
	minio := map[string]any{}
	for key, value := range map[string]string{
		"bucketName":    param.Bucket,
		"rootPath":      param.RootPath,
		"region":        param.Region,
		"cloudProvider": param.CloudProvider,
	} {
		if value != "" {
			minio[key] = value
		}
	}
	if param.UseSSL {
		minio["useSSL"] = true
	}
	if param.UseIAM {
		minio["useIAM"] = true
	}
	if len(minio) == 0 {
		return nil
	}
	return milvusapi.Values{"minio": minio}
}

// externalEtcdConfig sets the metadata prefix of an external etcd. Woodpecker
// keys its logs by the channel prefix (the instance name by default) under a
// fixed etcd prefix, so the channel prefix follows rootPath as well to keep
// same-named instances from different namespaces apart.
func externalEtcdConfig(param *dependencies.Etcd) milvusapi.Values {
	if param == nil || !param.External || param.RootPath == "" {
		return nil
	}
	return milvusapi.Values{
		"etcd":       map[string]any{"rootPath": param.RootPath},
		"msgChannel": map[string]any{"chanNamePrefix": map[string]any{"cluster": param.RootPath}},
	}
}

// storageServiceAccount is the ServiceAccount carrying the cloud identity of
// an external object store, if any.
func storageServiceAccount(param *dependencies.Storage) string {
	if param == nil || !param.External {
		return ""
	}
	return param.ServiceAccountName
}

// bundledInCluster wraps rendered Helm values so removing the Instance (and the
// owned Milvus CR) tears the bundled dependency down — StatefulSets, Pods and
// PVCs — instead of leaking orphans. The operator otherwise defaults bundled
// dependencies to Retain. This matches other providers, which delete both pods
// and volumes on teardown.
func bundledInCluster(values milvusapi.Values) *milvusapi.InClusterConfig {
	return &milvusapi.InClusterConfig{
		Values:         values,
		DeletionPolicy: "Delete",
		PVCDeletion:    true,
	}
}

// buildEtcd renders the etcd dependency, switching to external endpoints when
// requested, otherwise sizing the bundled cluster.
func buildEtcd(param *dependencies.Etcd, topologyType string) milvusapi.MilvusEtcd {
	if param != nil && param.External {
		return milvusapi.MilvusEtcd{External: true, Endpoints: param.Endpoints}
	}

	defaultReplicas := defaultStandaloneEtcdReplicas
	if topologyType == "cluster" {
		defaultReplicas = defaultClusterEtcdReplicas
	}
	replicas := defaultReplicas
	resources := defaultEtcdResources
	persistenceSize := defaultEtcdPersistence
	if param != nil {
		if param.Replicas != nil {
			replicas = *param.Replicas
		}
		resources = mergeResources(param.Resources, defaultEtcdResources)
		if param.Persistence != nil && param.Persistence.Size != "" {
			persistenceSize = param.Persistence.Size
		}
	}

	values := milvusapi.Values{"replicaCount": int(replicas)}
	if res := resourcesToValues(resources); res != nil {
		values["resources"] = res
	}
	if persistenceSize != "" {
		values["persistence"] = map[string]any{"size": persistenceSize}
	}
	return milvusapi.MilvusEtcd{InCluster: bundledInCluster(values)}
}

// buildStorage renders the MinIO object-storage dependency. External storage
// bypasses sizing; bundled storage carries the PVC size plus resource and
// replica tuning (a replica count above one enables MinIO distributed mode).
// buildStorage renders the MinIO object-storage dependency. External storage
// bypasses sizing; bundled storage carries the PVC size plus resource and
// replica tuning (a replica count above one enables MinIO distributed mode).
// The PVC size comes solely from the storage dependency parameter, falling back
// to a predictable default.
func buildStorage(param *dependencies.Storage) milvusapi.MilvusStorage {
	if param != nil && param.External {
		return milvusapi.MilvusStorage{
			External:  true,
			Endpoint:  param.Endpoint,
			Type:      param.Type,
			SecretRef: param.CredentialsSecret,
		}
	}

	replicas := defaultStorageReplicas
	resources := defaultStorageResources
	persistenceSize := defaultStoragePersistence
	if param != nil {
		if param.Replicas != nil {
			replicas = *param.Replicas
		}
		resources = mergeResources(param.Resources, defaultStorageResources)
		if param.Persistence != nil && param.Persistence.Size != "" {
			persistenceSize = param.Persistence.Size
		}
	}

	values := milvusapi.Values{
		"mode":     minioMode(replicas),
		"replicas": int(replicas),
		// Same images as the operator's Silo defaults; pinned so instances keep
		// them on the legacy MinIO chart, whose default is the gated
		// Docker Hub minio/minio.
		"image":       map[string]any{"repository": "pgsty/silo", "tag": "RELEASE.2026-09-03T13-18-01Z"},
		"mcImage":     map[string]any{"repository": "pgsty/mc", "tag": "RELEASE.2026-09-13T00-00-00Z"},
		"persistence": map[string]any{"size": persistenceSize},
	}
	if res := resourcesToValues(resources); res != nil {
		values["resources"] = res
	}
	return milvusapi.MilvusStorage{InCluster: bundledInCluster(values)}
}

// buildPulsar renders the Pulsar message-stream dependency, sizing each
// sub-component (broker, bookkeeper, zookeeper, proxy) or switching to an
// external endpoint.
func buildPulsar(param *dependencies.Pulsar) milvusapi.MilvusPulsar {
	if param != nil && param.External {
		return milvusapi.MilvusPulsar{External: true, Endpoint: param.Endpoint}
	}

	broker := defaultPulsarBroker
	bookkeeper := defaultPulsarBookKeeper
	zookeeper := defaultPulsarZooKeeper
	proxy := defaultPulsarProxy
	if param != nil {
		broker = mergePulsarComponent(param.Broker, defaultPulsarBroker)
		bookkeeper = mergePulsarBookKeeper(param.BookKeeper, defaultPulsarBookKeeper)
		zookeeper = mergePulsarZooKeeper(param.ZooKeeper, defaultPulsarZooKeeper)
		proxy = mergePulsarComponent(param.Proxy, defaultPulsarProxy)
	}

	values := milvusapi.Values{
		"broker":     pulsarComponentValues(broker),
		"bookkeeper": bookKeeperValues(bookkeeper),
		"zookeeper":  zooKeeperValues(zookeeper),
		"proxy":      pulsarComponentValues(proxy),
	}
	return milvusapi.MilvusPulsar{InCluster: bundledInCluster(values)}
}

// minioMode maps a replica count to the MinIO deployment mode.
func minioMode(replicas int32) string {
	if replicas > 1 {
		return "distributed"
	}
	return "standalone"
}

// mergePulsarComponent overlays user-provided replica/resource settings on top
// of a default sub-component.
func mergePulsarComponent(param *dependencies.PulsarComponent, def dependencies.PulsarComponent) dependencies.PulsarComponent {
	if param == nil {
		return def
	}
	merged := def
	if param.Replicas != nil {
		merged.Replicas = param.Replicas
	}
	merged.Resources = mergeResources(param.Resources, def.Resources)
	return merged
}

// pulsarComponentValues renders a Pulsar sub-component into Helm values.
func pulsarComponentValues(component dependencies.PulsarComponent) map[string]any {
	values := map[string]any{}
	if component.Replicas != nil {
		values["replicaCount"] = int(*component.Replicas)
	}
	if res := resourcesToValues(component.Resources); res != nil {
		values["resources"] = res
	}
	return values
}

// mergePulsarBookKeeper overlays user-provided replica/resource/volume settings
// on top of the default bookkeeper sub-component.
func mergePulsarBookKeeper(param *dependencies.PulsarBookKeeper, def dependencies.PulsarBookKeeper) dependencies.PulsarBookKeeper {
	if param == nil {
		return def
	}
	merged := def
	if param.Replicas != nil {
		merged.Replicas = param.Replicas
	}
	merged.Resources = mergeResources(param.Resources, def.Resources)
	merged.Journal = mergePersistence(param.Journal, def.Journal)
	merged.Ledgers = mergePersistence(param.Ledgers, def.Ledgers)
	return merged
}

// mergePulsarZooKeeper overlays user-provided replica/resource/volume settings
// on top of the default zookeeper sub-component.
func mergePulsarZooKeeper(param *dependencies.PulsarZooKeeper, def dependencies.PulsarZooKeeper) dependencies.PulsarZooKeeper {
	if param == nil {
		return def
	}
	merged := def
	if param.Replicas != nil {
		merged.Replicas = param.Replicas
	}
	merged.Resources = mergeResources(param.Resources, def.Resources)
	merged.Data = mergePersistence(param.Data, def.Data)
	return merged
}

// mergePersistence returns param when it sets a size, otherwise the default.
func mergePersistence(param, def *dependencies.Persistence) *dependencies.Persistence {
	if param != nil && param.Size != "" {
		return param
	}
	return def
}

// bookKeeperValues renders the bookkeeper sub-component (including its journal
// and ledger volumes) into Helm values.
func bookKeeperValues(bk dependencies.PulsarBookKeeper) map[string]any {
	values := map[string]any{}
	if bk.Replicas != nil {
		values["replicaCount"] = int(*bk.Replicas)
	}
	if res := resourcesToValues(bk.Resources); res != nil {
		values["resources"] = res
	}
	volumes := map[string]any{}
	if bk.Journal != nil && bk.Journal.Size != "" {
		volumes["journal"] = map[string]any{"size": bk.Journal.Size}
	}
	if bk.Ledgers != nil && bk.Ledgers.Size != "" {
		volumes["ledgers"] = map[string]any{"size": bk.Ledgers.Size}
	}
	if len(volumes) > 0 {
		values["volumes"] = volumes
	}
	return values
}

// zooKeeperValues renders the zookeeper sub-component (including its data
// volume) into Helm values.
func zooKeeperValues(zk dependencies.PulsarZooKeeper) map[string]any {
	values := map[string]any{}
	if zk.Replicas != nil {
		values["replicaCount"] = int(*zk.Replicas)
	}
	if res := resourcesToValues(zk.Resources); res != nil {
		values["resources"] = res
	}
	if zk.Data != nil && zk.Data.Size != "" {
		values["volumes"] = map[string]any{"data": map[string]any{"size": zk.Data.Size}}
	}
	return values
}

// mergeResources returns param when it specifies a resource list, falling back
// to def for each of requests and limits independently.
func mergeResources(param, def *dependencies.Resources) *dependencies.Resources {
	if param == nil {
		return def
	}
	merged := &dependencies.Resources{Requests: param.Requests, Limits: param.Limits}
	if def != nil {
		if merged.Requests == nil {
			merged.Requests = def.Requests
		}
		if merged.Limits == nil {
			merged.Limits = def.Limits
		}
	}
	return merged
}

// resourcesToValues renders resource requests/limits into a Helm values map,
// returning nil when nothing is set.
func resourcesToValues(resources *dependencies.Resources) map[string]any {
	if resources == nil {
		return nil
	}
	values := map[string]any{}
	if req := resourceListToValues(resources.Requests); req != nil {
		values["requests"] = req
	}
	if lim := resourceListToValues(resources.Limits); lim != nil {
		values["limits"] = lim
	}
	if len(values) == 0 {
		return nil
	}
	return values
}

// resourceListToValues renders a CPU/memory pair into a Helm values map,
// returning nil when both are empty.
func resourceListToValues(list *dependencies.ResourceList) map[string]any {
	if list == nil {
		return nil
	}
	values := map[string]any{}
	if list.CPU != "" {
		values["cpu"] = string(list.CPU)
	}
	if list.Memory != "" {
		values["memory"] = string(list.Memory)
	}
	if len(values) == 0 {
		return nil
	}
	return values
}
