package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"golang.org/x/mod/semver"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	commonv1alpha1 "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	milvusbackup "github.com/openeverest/provider-milvus/definition/backupclasses/milvus-backup"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

var (
	_ controller.BackupProvider = (*Provider)(nil)
	_ controller.BackupWatcher  = (*Provider)(nil)
	_ controller.RestoreWatcher = (*Provider)(nil)
)

// minRestoreVersion is the first Milvus 2.6 release that BulkInserts
// milvus-backup data correctly (milvus-io/milvus#48114).
const minRestoreVersion = "2.6.12"

var collectionSuffixPattern = regexp.MustCompile(`^[A-Za-z0-9_]*$`)

func jobRef(name string) *commonv1alpha1.TypedObjectRef {
	return &commonv1alpha1.TypedObjectRef{Group: "batch", Kind: "Job", Name: name}
}

// healthyMilvus returns the instance's Milvus CR, or a reason to wait.
func healthyMilvus(c *controller.Context) (*milvusapi.Milvus, string, error) {
	cr := &milvusapi.Milvus{}
	if err := c.Get(cr, c.Name()); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, "waiting for Milvus to be created", nil
		}
		return nil, "", fmt.Errorf("get Milvus: %w", err)
	}
	if cr.Status.Status != milvusapi.StatusHealthy {
		return nil, "waiting for Milvus to be healthy", nil
	}
	return cr, "", nil
}

func decodeParameters(raw *runtime.RawExtension, into any) error {
	if raw == nil || len(raw.Raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw.Raw, into)
}

// SyncBackup runs `milvus-backup create` in a Job owned by the Backup, or
// reports the CronJob Job a scheduled Backup mirrors.
func (p *Provider) SyncBackup(c *controller.Context, backup *backupv1alpha1.Backup) (controller.BackupExecutionStatus, error) {
	name := createJobName(backup)
	pending := func(message string) (controller.BackupExecutionStatus, error) {
		return controller.BackupExecutionStatus{State: backupv1alpha1.BackupStatePending, Message: message}, nil
	}
	failed := func(message string) (controller.BackupExecutionStatus, error) {
		return controller.BackupExecutionStatus{State: backupv1alpha1.BackupStateFailed, Message: message}, nil
	}

	job, err := existingJob(c, backup.Namespace, name)
	if err != nil {
		return controller.BackupExecutionStatus{}, err
	}
	if job == nil {
		if backup.Spec.ScheduleName != "" {
			return scheduledRunWithoutJob(backup), nil
		}
		var params milvusbackup.MilvusBackupParameters
		if err := decodeParameters(backup.Spec.Parameters, &params); err != nil {
			return failed(fmt.Sprintf("invalid parameters: %v", err))
		}
		cr, wait, err := healthyMilvus(c)
		if err != nil || wait != "" {
			if err != nil {
				return controller.BackupExecutionStatus{}, err
			}
			return pending(wait)
		}
		if other, err := activeBackupJob(c, backup.Namespace, c.Name(), name); err != nil || other != "" {
			if err != nil {
				return controller.BackupExecutionStatus{}, err
			}
			return pending(fmt.Sprintf("waiting for %s to finish", other))
		}
		location, err := resolveBackupLocation(c, backup)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return pending(fmt.Sprintf("waiting for BackupStorage %q", backup.Spec.StorageRef.Name))
			}
			return failed(err.Error())
		}
		spec, err := operationJobSpec(c, cr, location, backup)
		if err != nil {
			return failed(err.Error())
		}
		spec.op = operationCreate
		spec.args = append([]string{"create", "-n", location.name}, filterArg(params.Collections)...)
		if params.IncludeRBAC {
			spec.args = append(spec.args, "--rbac")
		}
		if job, err = ensureBackupJob(c, spec); err != nil {
			return controller.BackupExecutionStatus{}, err
		}
	}

	state := readBackupJobState(c, job)
	status := controller.BackupExecutionStatus{
		OperatorBackupRef: jobRef(job.Name),
		StartedAt:         state.startedAt,
		CompletedAt:       state.completedAt,
		Message:           state.message,
	}
	switch {
	case state.succeeded:
		status.State = backupv1alpha1.BackupStateSucceeded
	case state.failed:
		status.State = backupv1alpha1.BackupStateFailed
	case state.running:
		status.State = backupv1alpha1.BackupStateRunning
	default:
		status.State = backupv1alpha1.BackupStatePending
	}
	return status, nil
}

// operationJobSpec fills the parts every Job against a live instance shares:
// config, credentials and service account.
func operationJobSpec(c *controller.Context, cr *milvusapi.Milvus, location backupLocation, owner client.Object) (backupJobSpec, error) {
	milvusStore, err := milvusObjectStore(c, c.Name())
	if err != nil {
		return backupJobSpec{}, err
	}
	backupStore, err := backupStorageObjectStore(location.storage, location.rootPath)
	if err != nil {
		return backupJobSpec{}, err
	}
	config, err := milvusBackupConfig(c.Name(), c.Namespace(), milvusStore, backupStore)
	if err != nil {
		return backupJobSpec{}, err
	}
	spec := backupJobSpec{
		owner:     owner,
		instance:  c.Name(),
		namespace: c.Namespace(),
		config:    config,
		env:       append(milvusEnv(c.Name(), milvusStorageSecret(cr, milvusStore)), backupStorageEnv(location.storage)...),
	}
	if milvusStore.UseIAM {
		spec.saName = cr.Spec.Com.ServiceAccountName
	}
	return spec, nil
}

func existingJob(c *controller.Context, namespace, name string) (*batchv1.Job, error) {
	job := &batchv1.Job{}
	err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: namespace, Name: name}, job)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get job %q: %w", name, err)
	}
	return job, nil
}

// CleanupBackup deletes the backup's data with `milvus-backup delete` unless
// the Backup retains it or never produced any.
func (p *Provider) CleanupBackup(c *controller.Context, backup *backupv1alpha1.Backup) (bool, error) {
	if backup.Spec.DeletionPolicy == backupv1alpha1.BackupDeletionPolicyRetain {
		return true, nil
	}
	produced, err := backupProducedData(c, backup)
	if err != nil || !produced {
		return err == nil, err
	}
	name := backupJobName(backup.Name, operationDelete)
	job, err := existingJob(c, backup.Namespace, name)
	if err != nil {
		return false, err
	}
	if job == nil {
		location, err := resolveBackupLocation(c, backup)
		if err != nil {
			// The storage is gone, so is the backup's data as far as we can reach it.
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
		backupStore, err := backupStorageObjectStore(location.storage, location.rootPath)
		if err != nil {
			return true, nil
		}
		// delete never contacts Milvus; its storage section only needs to be valid.
		config, err := milvusBackupConfig(c.Name(), backup.Namespace, backupStore, backupStore)
		if err != nil {
			return false, err
		}
		storageEnv := backupStorageEnv(location.storage)
		env := append([]corev1.EnvVar{}, storageEnv...)
		for _, ev := range storageEnv {
			ev.Name = strings.Replace(ev.Name, "BACKUP_", "MILVUS_", 1)
			env = append(env, ev)
		}
		job, err = ensureBackupJob(c, backupJobSpec{
			owner:     backup,
			op:        operationDelete,
			instance:  c.Name(),
			namespace: backup.Namespace,
			args:      []string{"delete", "-n", location.name},
			config:    config,
			env:       env,
		})
		if err != nil {
			return false, err
		}
	}
	state := readBackupJobState(c, job)
	if state.failed {
		// A failed delete must not block namespace deletion forever; the data
		// it could not remove stays under the backup's path.
		log.FromContext(c.Context()).Info("milvus-backup delete failed; leaving data in storage",
			"backup", backup.Name, "message", state.message)
		return true, nil
	}
	return state.succeeded, nil
}

// backupProducedData reports whether milvus-backup finished writing the
// backup. The runtime already moved the Backup to Deleting, so the create Job
// is what still knows; without it, a completion time is the best evidence.
func backupProducedData(c *controller.Context, backup *backupv1alpha1.Backup) (bool, error) {
	job, err := existingJob(c, backup.Namespace, createJobName(backup))
	if err != nil {
		return false, err
	}
	if job == nil {
		return backup.Status.CompletedAt != nil, nil
	}
	succeeded, _ := jobFinished(job)
	return succeeded, nil
}

// SyncRestore runs `milvus-backup restore` into the target instance in a Job
// owned by the Restore.
func (p *Provider) SyncRestore(c *controller.Context, restore *backupv1alpha1.Restore) (controller.RestoreExecutionStatus, error) {
	name := backupJobName(restore.Name, operationRestore)
	pending := func(message string) (controller.RestoreExecutionStatus, error) {
		return controller.RestoreExecutionStatus{State: backupv1alpha1.RestoreStatePending, Message: message}, nil
	}
	failed := func(message string) (controller.RestoreExecutionStatus, error) {
		return controller.RestoreExecutionStatus{State: backupv1alpha1.RestoreStateFailed, Message: message}, nil
	}

	job, err := existingJob(c, restore.Namespace, name)
	if err != nil {
		return controller.RestoreExecutionStatus{}, err
	}
	if job == nil {
		if restore.Spec.DataSource.Type != backupv1alpha1.DataSourceTypeBackup || restore.Spec.DataSource.Backup == nil {
			return failed("only restores from a Backup are supported")
		}
		var params milvusbackup.MilvusRestoreParameters
		if err := decodeParameters(restore.Spec.Parameters, &params); err != nil {
			return failed(fmt.Sprintf("invalid parameters: %v", err))
		}
		if !collectionSuffixPattern.MatchString(params.CollectionSuffix) {
			return failed("collectionSuffix may only contain letters, digits and underscores")
		}
		source := &backupv1alpha1.Backup{}
		if err := c.Get(source, restore.Spec.DataSource.Backup.BackupRef.Name); err != nil {
			if apierrors.IsNotFound(err) {
				return pending("waiting for the source Backup")
			}
			return controller.RestoreExecutionStatus{}, fmt.Errorf("get source Backup: %w", err)
		}
		switch source.Status.State {
		case backupv1alpha1.BackupStateSucceeded:
		case backupv1alpha1.BackupStateFailed, backupv1alpha1.BackupStateError:
			return failed(fmt.Sprintf("source Backup %q failed", source.Name))
		default:
			return pending(fmt.Sprintf("waiting for source Backup %q to succeed", source.Name))
		}
		cr, wait, err := healthyMilvus(c)
		if err != nil || wait != "" {
			if err != nil {
				return controller.RestoreExecutionStatus{}, err
			}
			return pending(wait)
		}
		if lowest := lowestComponentVersion(cr.Spec.Com); lowest != "" && semver.Compare("v"+lowest, "v"+minRestoreVersion) < 0 {
			return failed(fmt.Sprintf("restoring needs Milvus %s or newer; this instance runs %s", minRestoreVersion, lowest))
		}
		if other, err := activeBackupJob(c, restore.Namespace, c.Name(), name); err != nil || other != "" {
			if err != nil {
				return controller.RestoreExecutionStatus{}, err
			}
			return pending(fmt.Sprintf("waiting for %s to finish", other))
		}
		location, err := resolveBackupLocation(c, source)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return pending(fmt.Sprintf("waiting for BackupStorage %q", source.Spec.StorageRef.Name))
			}
			return failed(err.Error())
		}
		spec, err := operationJobSpec(c, cr, location, restore)
		if err != nil {
			return failed(err.Error())
		}
		spec.op = operationRestore
		// Indexes are not in the backup; without rebuilding them restored
		// collections cannot be loaded.
		spec.args = append([]string{"restore", "-n", location.name, "--rebuild_index"}, filterArg(params.Collections)...)
		if !params.KeepExistingCollections {
			spec.args = append(spec.args, "--drop_exist_collection")
		}
		if params.CollectionSuffix != "" {
			spec.args = append(spec.args, "--suffix", params.CollectionSuffix)
		}
		if params.IncludeRBAC {
			spec.args = append(spec.args, "--rbac")
		}
		if job, err = ensureBackupJob(c, spec); err != nil {
			return controller.RestoreExecutionStatus{}, err
		}
	}

	state := readBackupJobState(c, job)
	status := controller.RestoreExecutionStatus{
		OperatorRestoreRef: jobRef(job.Name),
		StartedAt:          state.startedAt,
		CompletedAt:        state.completedAt,
		Message:            state.message,
	}
	switch {
	case state.succeeded:
		status.State = backupv1alpha1.RestoreStateSucceeded
	case state.failed:
		status.State = backupv1alpha1.RestoreStateFailed
	case state.running:
		status.State = backupv1alpha1.RestoreStateRunning
	default:
		status.State = backupv1alpha1.RestoreStatePending
	}
	return status, nil
}

// CleanupRestore has nothing to do: the Job and its ConfigMap are owned by
// the Restore and garbage-collected with it.
func (p *Provider) CleanupRestore(_ *controller.Context, _ *backupv1alpha1.Restore) (bool, error) {
	return true, nil
}

// BackupWatches routes Job status changes straight to the owning Backup.
func (p *Provider) BackupWatches() []controller.WatchConfig {
	return []controller.WatchConfig{
		controller.WatchOwned(&batchv1.Job{}),
		controller.WatchExternal(&batchv1.Job{}, handler.EnqueueRequestsFromMapFunc(enqueueScheduledBackup)),
	}
}

// RestoreWatches routes Job status changes straight to the owning Restore.
func (p *Provider) RestoreWatches() []controller.WatchConfig {
	return []controller.WatchConfig{controller.WatchOwned(&batchv1.Job{})}
}

// activeRestore reports whether a Restore into the instance is unfinished.
func activeRestore(c *controller.Context) (bool, error) {
	restores := &backupv1alpha1.RestoreList{}
	if err := c.Client().List(c.Context(), restores,
		client.InNamespace(c.Namespace()),
		client.MatchingFields{controller.IndexRestoreInstanceName: c.Name()},
	); err != nil {
		return false, fmt.Errorf("list restores: %w", err)
	}
	for i := range restores.Items {
		restore := &restores.Items[i]
		if restore.DeletionTimestamp != nil {
			continue
		}
		switch restore.Status.State {
		case backupv1alpha1.RestoreStateSucceeded, backupv1alpha1.RestoreStateFailed:
		default:
			return true, nil
		}
	}
	return false, nil
}

// syncDataSource seeds a new instance from spec.dataSource once Milvus is
// healthy: milvus-backup restores through the running instance.
func syncDataSource(c *controller.Context) error {
	if c.Instance().Spec.DataSource == nil {
		return nil
	}
	if _, wait, err := healthyMilvus(c); err != nil || wait != "" {
		if err != nil {
			return err
		}
		c.SetDataSourceStatus(controller.DataSourceStatus{
			State:   controller.DataSourceStateWaiting,
			Reason:  corev1alpha1.ReasonDataSourceWaitingForCluster,
			Message: wait,
		})
		return nil
	}
	if _, err := c.ReconcileDataSource(); err != nil {
		return fmt.Errorf("reconcile data source: %w", err)
	}
	return nil
}

// enqueueRestoreInstance maps a Restore to the Instance it targets, so the
// Instance leaves the Restoring phase as soon as the Restore finishes.
func enqueueRestoreInstance(_ context.Context, obj client.Object) []reconcile.Request {
	restore, ok := obj.(*backupv1alpha1.Restore)
	if !ok || restore.Spec.InstanceRef.Name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{
		Namespace: restore.Namespace,
		Name:      restore.Spec.InstanceRef.Name,
	}}}
}
