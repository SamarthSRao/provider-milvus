package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	commonv1alpha1 "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	milvusbackup "github.com/openeverest/provider-milvus/definition/backupclasses/milvus-backup"
	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

var _ controller.BackupMirror = (*Provider)(nil)

// Labels the CronJob stamps on every Job it starts, so Mirror can turn the
// Job into a Backup.
const (
	backupScheduleLabel = "backup.milvus.openeverest.io/schedule"
	backupStorageLabel  = "backup.milvus.openeverest.io/storage"
)

// CronJob names leave room for the 11-character suffix of the Jobs they start.
const maxCronJobNameLength = 52

// keptFailedRuns bounds the failed Backups each schedule leaves behind; they
// hold the error of recent runs but no data.
const keptFailedRuns = 3

// The CronJob starts Jobs under generated names; the backup is named after the
// Job, mapped onto milvus-backup's [A-Za-z0-9_] names like milvusBackupName.
const scheduledCreateScript = `exec /app/milvus-backup create -n "$(printf %s "$JOB_NAME" | tr .- __)" "$@"`

var dnsLabelInvalid = regexp.MustCompile(`[^a-z0-9-]+`)

// createJobName is the Job that writes the backup: owned by the Backup when
// taken on demand, started by a CronJob and named after it when scheduled.
func createJobName(backup *backupv1alpha1.Backup) string {
	if backup.Spec.ScheduleName != "" {
		return backup.Name
	}
	return backupJobName(backup.Name, operationCreate)
}

// scheduledRunWithoutJob keeps the outcome of a scheduled run once the
// CronJob's history limit removed its Job.
func scheduledRunWithoutJob(backup *backupv1alpha1.Backup) controller.BackupExecutionStatus {
	switch backup.Status.State {
	case backupv1alpha1.BackupStateSucceeded, backupv1alpha1.BackupStateFailed:
		return controller.BackupExecutionStatus{
			State:             backup.Status.State,
			Message:           backup.Status.Message,
			OperatorBackupRef: backup.Status.OperatorBackupRef,
			StartedAt:         backup.Status.StartedAt,
			CompletedAt:       backup.Status.CompletedAt,
			Size:              backup.Status.Size,
		}
	default:
		return controller.BackupExecutionStatus{
			State:   backupv1alpha1.BackupStateFailed,
			Message: "the scheduled backup's Job was removed before it finished",
		}
	}
}

// scheduleCronJobName names the CronJob of a schedule, shortened with a hash
// when the schedule name is long or not a valid DNS label.
func scheduleCronJobName(instance, schedule string) string {
	name := instance + "-" + strings.Trim(dnsLabelInvalid.ReplaceAllString(strings.ToLower(schedule), "-"), "-")
	if name == instance+"-"+schedule && len(name) <= maxCronJobNameLength {
		return name
	}
	sum := sha256.Sum256([]byte(instance + "/" + schedule))
	hash := hex.EncodeToString(sum[:])[:8]
	if keep := maxCronJobNameLength - len(hash) - 1; len(name) > keep {
		name = name[:keep]
	}
	return strings.TrimRight(name, "-") + "-" + hash
}

// syncBackupSchedules runs one CronJob per enabled schedule, removes the
// CronJobs of schedules that are gone, and applies retention.
func syncBackupSchedules(c *controller.Context) error {
	desired := map[string]bool{}
	backup := c.Instance().Spec.Backup
	if backup != nil && backup.Enabled {
		cr, wait, err := healthyMilvus(c)
		if err != nil {
			return err
		}
		suspend, err := schedulesSuspended(c)
		if err != nil {
			return err
		}
		for _, storage := range backup.Storages {
			for _, schedule := range storage.Schedules {
				if !schedule.Enabled {
					continue
				}
				name := scheduleCronJobName(c.Name(), schedule.Name)
				desired[name] = true
				if wait != "" {
					// The Job config needs Milvus's rendered config; keep any
					// existing CronJob until it can be refreshed.
					continue
				}
				if err := applyBackupSchedule(c, cr, name, storage.StorageRef.Name, schedule, suspend); err != nil {
					return err
				}
			}
		}
	}
	if err := pruneBackupSchedules(c, desired); err != nil {
		return err
	}
	return enforceRetention(c, time.Now())
}

// schedulesSuspended holds scheduled runs while data is being restored into
// the instance: a backup taken mid-restore would capture half the data.
func schedulesSuspended(c *controller.Context) (bool, error) {
	if ds := c.GetDataSourceStatus(); ds != nil && !ds.Done {
		return true, nil
	}
	return activeRestore(c)
}

func applyBackupSchedule(c *controller.Context, cr *milvusapi.Milvus, name, storageName string, schedule corev1alpha1.InstanceBackupSchedule, suspend bool) error {
	if _, err := cron.ParseStandard(schedule.Cron); err != nil || strings.HasPrefix(schedule.Cron, "TZ=") || strings.HasPrefix(schedule.Cron, "CRON_TZ=") {
		return &controller.BackupConfigError{
			Reason:  "InvalidSchedule",
			Message: fmt.Sprintf("schedule %q: %q is not a standard 5-field cron expression", schedule.Name, schedule.Cron),
		}
	}
	var params milvusbackup.MilvusBackupParameters
	if err := decodeParameters(schedule.Parameters, &params); err != nil {
		return &controller.BackupConfigError{
			Reason:  "InvalidSchedule",
			Message: fmt.Sprintf("schedule %q: invalid parameters: %v", schedule.Name, err),
		}
	}
	bs, err := c.BackupStorage(storageName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return &controller.BackupConfigError{
				Reason:  "StorageNotFound",
				Message: fmt.Sprintf("schedule %q: BackupStorage %q not found", schedule.Name, storageName),
			}
		}
		return err
	}
	location := backupLocation{storage: bs, rootPath: backupRootPath(c.Namespace(), c.Name())}
	spec, err := operationJobSpec(c, cr, location, nil)
	if err != nil {
		return &controller.BackupConfigError{Reason: "InvalidStorage", Message: fmt.Sprintf("schedule %q: %v", schedule.Name, err)}
	}
	spec.args = filterArg(params.Collections)
	if params.IncludeRBAC {
		spec.args = append(spec.args, "--rbac")
	}

	labels := backupJobLabels(c.Name(), operationCreate)
	labels[backupScheduleLabel] = schedule.Name
	labels[backupStorageLabel] = storageName

	configMap := &corev1.ConfigMap{
		ObjectMeta: withLabels(c.ObjectMeta(name), labels),
		Data:       map[string]string{"backup.yaml": spec.config},
	}
	if err := c.Apply(configMap); err != nil {
		return fmt.Errorf("apply schedule config: %w", err)
	}

	jobSpec := backupJobTemplate(spec, name, labels)
	container := &jobSpec.Template.Spec.Containers[0]
	container.Command = []string{"/bin/sh", "-c", scheduledCreateScript, "milvus-backup"}
	container.Env = append(container.Env, corev1.EnvVar{
		Name: "JOB_NAME",
		ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
			APIVersion: "v1",
			FieldPath:  "metadata.labels['" + batchv1.JobNameLabel + "']",
		}},
	})
	cronJob := &batchv1.CronJob{
		ObjectMeta: withLabels(c.ObjectMeta(name), labels),
		Spec: batchv1.CronJobSpec{
			Schedule: schedule.Cron,
			// Kubernetes evaluates CronJob schedules in UTC by default.
			TimeZone:                   ptr.To("Etc/UTC"),
			Suspend:                    ptr.To(suspend),
			ConcurrencyPolicy:          batchv1.ForbidConcurrent,
			StartingDeadlineSeconds:    ptr.To(int64(300)),
			SuccessfulJobsHistoryLimit: ptr.To(int32(3)),
			FailedJobsHistoryLimit:     ptr.To(int32(3)),
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       jobSpec,
			},
		},
	}
	if err := c.Apply(cronJob); err != nil {
		return &controller.BackupConfigError{Reason: "ScheduleNotApplied", Message: fmt.Sprintf("schedule %q: %v", schedule.Name, err)}
	}
	return nil
}

func withLabels(meta metav1.ObjectMeta, labels map[string]string) metav1.ObjectMeta {
	for key, value := range labels {
		meta.Labels[key] = value
	}
	return meta
}

// pruneBackupSchedules deletes the CronJobs, and their configs, of schedules
// that were removed or disabled. Backups they produced stay.
func pruneBackupSchedules(c *controller.Context, desired map[string]bool) error {
	cronJobs := &batchv1.CronJobList{}
	if err := c.List(cronJobs, client.MatchingLabels{backupJobInstanceLabel: c.Name()}, client.HasLabels{backupScheduleLabel}); err != nil {
		return fmt.Errorf("list backup schedules: %w", err)
	}
	for i := range cronJobs.Items {
		name := cronJobs.Items[i].Name
		if desired[name] {
			continue
		}
		if err := c.Delete(&cronJobs.Items[i]); err != nil {
			return fmt.Errorf("delete backup schedule %q: %w", name, err)
		}
		if err := c.Delete(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace()}}); err != nil {
			return fmt.Errorf("delete backup schedule config %q: %w", name, err)
		}
	}
	return nil
}

// enforceRetention deletes the successful backups each schedule no longer
// keeps, and failed runs beyond the newest few; deleting a Backup removes its
// data unless it is retained.
func enforceRetention(c *controller.Context, now time.Time) error {
	backup := c.Instance().Spec.Backup
	if backup == nil {
		return nil
	}
	retention := map[string]*corev1alpha1.BackupScheduleRetention{}
	for _, storage := range backup.Storages {
		for _, schedule := range storage.Schedules {
			retention[schedule.Name] = schedule.Retention
		}
	}
	if len(retention) == 0 {
		return nil
	}
	backups, err := c.BackupsForInstance()
	if err != nil {
		return err
	}
	for _, expired := range expiredBackups(backups, retention, now) {
		if err := c.Delete(expired); err != nil {
			return fmt.Errorf("delete expired backup %q: %w", expired.Name, err)
		}
	}
	return nil
}

func expiredBackups(backups []backupv1alpha1.Backup, retention map[string]*corev1alpha1.BackupScheduleRetention, now time.Time) []*backupv1alpha1.Backup {
	succeeded := map[string][]*backupv1alpha1.Backup{}
	failed := map[string][]*backupv1alpha1.Backup{}
	for i := range backups {
		b := &backups[i]
		if _, scheduled := retention[b.Spec.ScheduleName]; !scheduled || b.DeletionTimestamp != nil {
			continue
		}
		switch {
		case b.Status.State == backupv1alpha1.BackupStateSucceeded && b.Status.CompletedAt != nil:
			succeeded[b.Spec.ScheduleName] = append(succeeded[b.Spec.ScheduleName], b)
		case b.Status.State == backupv1alpha1.BackupStateFailed:
			failed[b.Spec.ScheduleName] = append(failed[b.Spec.ScheduleName], b)
		}
	}
	var expired []*backupv1alpha1.Backup
	for schedule, policy := range retention {
		if runs := failed[schedule]; len(runs) > keptFailedRuns {
			sort.Slice(runs, func(i, j int) bool { return runs[i].CreationTimestamp.After(runs[j].CreationTimestamp.Time) })
			expired = append(expired, runs[keptFailedRuns:]...)
		}
		if policy == nil {
			continue
		}
		runs := succeeded[schedule]
		sort.Slice(runs, func(i, j int) bool { return runs[i].Status.CompletedAt.After(runs[j].Status.CompletedAt.Time) })
		switch policy.Type {
		case corev1alpha1.BackupScheduleRetentionTypeCount:
			if policy.Count != nil && len(runs) > int(*policy.Count) {
				expired = append(expired, runs[*policy.Count:]...)
			}
		case corev1alpha1.BackupScheduleRetentionTypeTime:
			cutoff, ok := retentionCutoff(policy.Duration, now)
			if !ok {
				continue
			}
			for _, run := range runs {
				if run.Status.CompletedAt.Time.Before(cutoff) {
					expired = append(expired, run)
				}
			}
		}
	}
	return expired
}

// retentionCutoff parses the API's <n>d, <n>w or <n>m windows.
func retentionCutoff(duration string, now time.Time) (time.Time, bool) {
	if len(duration) < 2 {
		return time.Time{}, false
	}
	n, err := strconv.Atoi(duration[:len(duration)-1])
	if err != nil || n < 1 {
		return time.Time{}, false
	}
	switch duration[len(duration)-1] {
	case 'd':
		return now.AddDate(0, 0, -n), true
	case 'w':
		return now.AddDate(0, 0, -7*n), true
	case 'm':
		return now.AddDate(0, -n, 0), true
	}
	return time.Time{}, false
}

// OperatorBackupType makes the runtime mirror the Jobs that backup CronJobs
// start into Backups.
func (p *Provider) OperatorBackupType() client.Object {
	return &batchv1.Job{}
}

// Mirror turns a Job started by a backup schedule into a Backup of the same
// name; every other Job is skipped.
func (p *Provider) Mirror(ctx context.Context, cl client.Client, obj client.Object) (*backupv1alpha1.Backup, error) {
	job, ok := obj.(*batchv1.Job)
	if !ok || job.Labels[backupScheduleLabel] == "" || job.Labels[backupJobInstanceLabel] == "" {
		return nil, nil
	}
	instance := &corev1alpha1.Instance{}
	if err := cl.Get(ctx, client.ObjectKey{Namespace: job.Namespace, Name: job.Labels[backupJobInstanceLabel]}, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get Instance of scheduled backup %q: %w", job.Name, err)
	}
	if instance.Spec.ProviderRef.Name != common.ProviderName || instance.Spec.Backup == nil {
		return nil, nil
	}
	backup := &backupv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: job.Name, Namespace: job.Namespace},
		Spec: backupv1alpha1.BackupSpec{
			Origin: backupv1alpha1.BackupOrigin{
				Type:        backupv1alpha1.BackupOriginTypeInstance,
				InstanceRef: &commonv1alpha1.ObjectRef{Name: instance.Name},
			},
			ClassRef:     instance.Spec.Backup.ClassRef,
			StorageRef:   commonv1alpha1.ObjectRef{Name: job.Labels[backupStorageLabel]},
			ScheduleName: job.Labels[backupScheduleLabel],
		},
	}
	for _, storage := range instance.Spec.Backup.Storages {
		for _, schedule := range storage.Schedules {
			if schedule.Name == backup.Spec.ScheduleName {
				backup.Spec.Parameters = schedule.Parameters
			}
		}
	}
	return backup, nil
}

// enqueueScheduledBackup maps a scheduled run's Job to the Backup mirroring
// it; that Job is owned by its CronJob, not by the Backup.
func enqueueScheduledBackup(_ context.Context, obj client.Object) []reconcile.Request {
	if obj.GetLabels()[backupScheduleLabel] == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}}}
}
