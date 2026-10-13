package provider

// Run `make manifests` to regenerate config/rbac/role.yaml from these markers.
// This file contains kubebuilder RBAC markers for controller-gen.
// See: https://book.kubebuilder.io/reference/markers/rbac

// Base RBAC (required by all providers):
// +kubebuilder:rbac:groups=core.openeverest.io,resources=instances,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=core.openeverest.io,resources=instances/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.openeverest.io,resources=instances/finalizers,verbs=update
// +kubebuilder:rbac:groups=core.openeverest.io,resources=providers,verbs=get;list;watch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// The runtime writes the connection details returned by Status() into a Secret.
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete

// Milvus operator resources.
// +kubebuilder:rbac:groups=milvus.io,resources=milvuses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=milvus.io,resources=milvuses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=milvus.io,resources=milvuses/finalizers,verbs=update

// Supporting resources created by the operator and used by the provider
// while reconciling the instance.
// +kubebuilder:rbac:groups="",resources=services;configmaps;persistentvolumeclaims;pods,verbs=get;list;watch;create;update;patch;delete
// Nodes are read to resolve a reachable address for NodePort-exposed instances.
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch;create;update;patch;delete
// PodMonitor of the Prometheus integration; the operator's own one is removed.
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=podmonitors,verbs=get;list;watch;create;update;patch;delete

// Backups and restores: the runtime's Backup/Restore reconcilers, and the
// milvus-backup Jobs the provider runs for them.
// +kubebuilder:rbac:groups=backup.openeverest.io,resources=backups;restores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=backup.openeverest.io,resources=backups/status;restores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=backup.openeverest.io,resources=backups/finalizers;restores/finalizers,verbs=update
// +kubebuilder:rbac:groups=backup.openeverest.io,resources=backupclasses;backupstorages,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs;cronjobs,verbs=get;list;watch;create;update;patch;delete
