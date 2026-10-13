package provider

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	commonv1alpha1 "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	milvusbackup "github.com/openeverest/provider-milvus/definition/backupclasses/milvus-backup"
	"github.com/openeverest/provider-milvus/internal/common"
)

func scheduledInstance(schedules ...corev1alpha1.InstanceBackupSchedule) *corev1alpha1.Instance {
	return &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Spec: corev1alpha1.InstanceSpec{
			ProviderRef: commonv1alpha1.ObjectRef{Name: common.ProviderName},
			Backup: &corev1alpha1.InstanceBackupSpec{
				Enabled:  true,
				ClassRef: commonv1alpha1.ObjectRef{Name: "milvus-backup"},
				Storages: []corev1alpha1.InstanceBackupStorage{{
					StorageRef: commonv1alpha1.ObjectRef{Name: "s3"},
					Schedules:  schedules,
				}},
			},
		},
	}
}

func newScheduleContext(t *testing.T, instance *corev1alpha1.Instance, objs ...client.Object) *controller.Context {
	t.Helper()
	all := append([]client.Object{instance}, objs...)
	return controller.NewContext(context.Background(), fakeClientBuilder(t).WithObjects(all...).Build(), instance, common.ProviderName)
}

func daily(params *milvusbackup.MilvusBackupParameters) corev1alpha1.InstanceBackupSchedule {
	schedule := corev1alpha1.InstanceBackupSchedule{Name: "daily", Enabled: true, Cron: "0 3 * * *"}
	if params != nil {
		raw, _ := json.Marshal(params)
		schedule.Parameters = &runtime.RawExtension{Raw: raw}
	}
	return schedule
}

func getCronJob(t *testing.T, c *controller.Context, name string) *batchv1.CronJob {
	t.Helper()
	cronJob := &batchv1.CronJob{}
	require.NoError(t, c.Get(cronJob, name))
	return cronJob
}

func TestScheduleCronJobName(t *testing.T) {
	assert.Equal(t, "test-milvus-daily", scheduleCronJobName("test-milvus", "daily"))

	odd := scheduleCronJobName("test-milvus", "Nightly_Full")
	assert.Regexp(t, `^test-milvus-nightly-full-[0-9a-f]{8}$`, odd)
	assert.NotEqual(t, odd, scheduleCronJobName("test-milvus", "nightly-full"), "a different schedule keeps a different CronJob")

	long := scheduleCronJobName("an-instance-with-a-rather-long-name", "every-fifteen-minutes-on-weekdays")
	assert.LessOrEqual(t, len(long), maxCronJobNameLength)
}

func TestSyncBackupSchedules(t *testing.T) {
	t.Run("runs each enabled schedule as a CronJob", func(t *testing.T) {
		instance := scheduledInstance(daily(&milvusbackup.MilvusBackupParameters{Collections: "db1.*", IncludeRBAC: true}))
		c := newScheduleContext(t, instance, backupFixtures("2.6.15")...)

		require.NoError(t, syncBackupSchedules(c))

		cronJob := getCronJob(t, c, "test-milvus-daily")
		assert.Equal(t, "0 3 * * *", cronJob.Spec.Schedule)
		assert.Equal(t, batchv1.ForbidConcurrent, cronJob.Spec.ConcurrencyPolicy)
		assert.Equal(t, ptr.To(false), cronJob.Spec.Suspend)
		assert.Equal(t, "test-milvus", cronJob.OwnerReferences[0].Name, "removed with the instance")

		labels := cronJob.Spec.JobTemplate.Labels
		assert.Equal(t, "daily", labels[backupScheduleLabel])
		assert.Equal(t, "s3", labels[backupStorageLabel])
		assert.Equal(t, "test-milvus", labels[backupJobInstanceLabel], "on-demand runs wait for scheduled ones")

		container := cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0]
		assert.Equal(t, []string{"/bin/sh", "-c", scheduledCreateScript, "milvus-backup"}, container.Command)
		assert.Equal(t, []string{"--filter", "db1.*", "--rbac", "--config", "/etc/milvus-backup/backup.yaml"}, container.Args)
		jobName := container.Env[len(container.Env)-1]
		assert.Equal(t, "JOB_NAME", jobName.Name)
		assert.Equal(t, "metadata.labels['batch.kubernetes.io/job-name']", jobName.ValueFrom.FieldRef.FieldPath)

		cm := &corev1.ConfigMap{}
		require.NoError(t, c.Get(cm, "test-milvus-daily"))
		assert.Contains(t, cm.Data["backup.yaml"], "rootPath: everest/milvus/db/test-milvus", "scheduled and on-demand backups share a path")
	})

	t.Run("removes the CronJob of a disabled schedule", func(t *testing.T) {
		instance := scheduledInstance(daily(nil))
		c := newScheduleContext(t, instance, backupFixtures("2.6.15")...)
		require.NoError(t, syncBackupSchedules(c))

		instance.Spec.Backup.Storages[0].Schedules[0].Enabled = false
		require.NoError(t, syncBackupSchedules(c))

		cronJobs := &batchv1.CronJobList{}
		require.NoError(t, c.List(cronJobs))
		assert.Empty(t, cronJobs.Items)
	})

	t.Run("suspends schedules while a restore runs", func(t *testing.T) {
		restore := newRestore(nil)
		restore.Status.State = backupv1alpha1.RestoreStateRunning
		c := newScheduleContext(t, scheduledInstance(daily(nil)), append(backupFixtures("2.6.15"), restore)...)

		require.NoError(t, syncBackupSchedules(c))
		assert.Equal(t, ptr.To(true), getCronJob(t, c, "test-milvus-daily").Spec.Suspend)
	})

	t.Run("reports an invalid cron expression on the backup condition", func(t *testing.T) {
		schedule := daily(nil)
		schedule.Cron = "every night"
		c := newScheduleContext(t, scheduledInstance(schedule), backupFixtures("2.6.15")...)

		err := syncBackupSchedules(c)
		bce := controller.AsBackupConfigError(err)
		require.NotNil(t, bce, "got %v", err)
		assert.Equal(t, "InvalidSchedule", bce.Reason)
	})
}

func scheduledJob(name string) *batchv1.Job {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: "db",
		Labels: map[string]string{
			backupJobInstanceLabel: "test-milvus",
			backupScheduleLabel:    "daily",
			backupStorageLabel:     "s3",
		},
	}}
}

func TestMirror(t *testing.T) {
	params := &milvusbackup.MilvusBackupParameters{Collections: "db1.*"}
	c := newScheduleContext(t, scheduledInstance(daily(params)))

	backup, err := New().Mirror(context.Background(), c.Client(), scheduledJob("test-milvus-daily-29123456"))
	require.NoError(t, err)
	require.NotNil(t, backup)
	assert.Equal(t, "test-milvus-daily-29123456", backup.Name)
	assert.Equal(t, "daily", backup.Spec.ScheduleName)
	assert.Equal(t, "s3", backup.Spec.StorageRef.Name)
	assert.Equal(t, "milvus-backup", backup.Spec.ClassRef.Name)
	assert.Equal(t, "test-milvus", backup.Spec.Origin.InstanceRef.Name)
	assert.JSONEq(t, `{"collections":"db1.*"}`, string(backup.Spec.Parameters.Raw))

	onDemand := scheduledJob("daily-1-mb")
	delete(onDemand.Labels, backupScheduleLabel)
	backup, err = New().Mirror(context.Background(), c.Client(), onDemand)
	require.NoError(t, err)
	assert.Nil(t, backup, "only scheduled runs are mirrored")
}

func TestSyncBackupScheduledRun(t *testing.T) {
	scheduled := func() *backupv1alpha1.Backup {
		backup := newBackup("test-milvus-daily-29123456", nil)
		backup.Spec.ScheduleName = "daily"
		return backup
	}

	t.Run("reports the CronJob's Job", func(t *testing.T) {
		c := newScheduleContext(t, scheduledInstance(daily(nil)), append(backupFixtures("2.6.15"), scheduledJob("test-milvus-daily-29123456"))...)
		backup := scheduled()
		setJobCondition(t, c, getJob(t, c, backup.Name), batchv1.JobComplete)

		status, err := New().SyncBackup(c, backup)
		require.NoError(t, err)
		assert.Equal(t, backupv1alpha1.BackupStateSucceeded, status.State)
		assert.Equal(t, backup.Name, status.OperatorBackupRef.Name)

		jobs := &batchv1.JobList{}
		require.NoError(t, c.List(jobs))
		assert.Len(t, jobs.Items, 1, "nothing is started for a scheduled run")
	})

	t.Run("keeps the outcome after the CronJob prunes the Job", func(t *testing.T) {
		c := newScheduleContext(t, scheduledInstance(daily(nil)))
		backup := scheduled()
		backup.Status.State = backupv1alpha1.BackupStateSucceeded
		backup.Status.CompletedAt = &metav1.Time{Time: time.Now()}

		status, err := New().SyncBackup(c, backup)
		require.NoError(t, err)
		assert.Equal(t, backupv1alpha1.BackupStateSucceeded, status.State)
		assert.Equal(t, backup.Status.CompletedAt, status.CompletedAt)
	})

	t.Run("fails a run whose Job vanished before finishing", func(t *testing.T) {
		c := newScheduleContext(t, scheduledInstance(daily(nil)))

		status, err := New().SyncBackup(c, scheduled())
		require.NoError(t, err)
		assert.Equal(t, backupv1alpha1.BackupStateFailed, status.State)
	})
}

func TestEnforceRetention(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	run := func(name, schedule string, age time.Duration, state backupv1alpha1.BackupState) *backupv1alpha1.Backup {
		backup := newBackup(name, nil)
		backup.Spec.ScheduleName = schedule
		backup.Status.State = state
		backup.Status.CompletedAt = &metav1.Time{Time: now.Add(-age)}
		return backup
	}
	remaining := func(t *testing.T, c *controller.Context) []string {
		t.Helper()
		backups := &backupv1alpha1.BackupList{}
		require.NoError(t, c.List(backups))
		names := []string{}
		for _, b := range backups.Items {
			names = append(names, b.Name)
		}
		return names
	}
	day := 24 * time.Hour

	t.Run("keeps the newest successful runs by count", func(t *testing.T) {
		schedule := daily(nil)
		schedule.Retention = &corev1alpha1.BackupScheduleRetention{Type: corev1alpha1.BackupScheduleRetentionTypeCount, Count: ptr.To(int32(2))}
		c := newScheduleContext(t, scheduledInstance(schedule),
			run("d1", "daily", 3*day, backupv1alpha1.BackupStateSucceeded),
			run("d2", "daily", 2*day, backupv1alpha1.BackupStateSucceeded),
			run("d3", "daily", 1*day, backupv1alpha1.BackupStateSucceeded),
			run("d4", "daily", 0, backupv1alpha1.BackupStateFailed),
			run("other", "weekly", 30*day, backupv1alpha1.BackupStateSucceeded),
			newBackup("manual", nil),
		)

		require.NoError(t, enforceRetention(c, now))
		assert.ElementsMatch(t, []string{"d2", "d3", "d4", "other", "manual"}, remaining(t, c))
	})

	t.Run("keeps successful runs inside a time window", func(t *testing.T) {
		schedule := daily(nil)
		schedule.Retention = &corev1alpha1.BackupScheduleRetention{Type: corev1alpha1.BackupScheduleRetentionTypeTime, Duration: "1w"}
		c := newScheduleContext(t, scheduledInstance(schedule),
			run("old", "daily", 8*day, backupv1alpha1.BackupStateSucceeded),
			run("recent", "daily", 6*day, backupv1alpha1.BackupStateSucceeded),
		)

		require.NoError(t, enforceRetention(c, now))
		assert.ElementsMatch(t, []string{"recent"}, remaining(t, c))
	})

	t.Run("keeps only the newest failed runs", func(t *testing.T) {
		failedRun := func(name string, age time.Duration) *backupv1alpha1.Backup {
			backup := run(name, "daily", age, backupv1alpha1.BackupStateFailed)
			backup.CreationTimestamp = metav1.NewTime(now.Add(-age))
			backup.Status.CompletedAt = nil
			return backup
		}
		c := newScheduleContext(t, scheduledInstance(daily(nil)),
			failedRun("f1", 4*day), failedRun("f2", 3*day), failedRun("f3", 2*day), failedRun("f4", day),
		)

		require.NoError(t, enforceRetention(c, now))
		assert.ElementsMatch(t, []string{"f2", "f3", "f4"}, remaining(t, c))
	})
}

func TestRetentionCutoff(t *testing.T) {
	now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	for duration, want := range map[string]time.Time{
		"30d": time.Date(2026, 9, 13, 0, 0, 0, 0, time.UTC),
		"2w":  time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC),
		"3m":  time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC),
	} {
		got, ok := retentionCutoff(duration, now)
		require.True(t, ok, duration)
		assert.Equal(t, want, got, duration)
	}
	_, ok := retentionCutoff("5y", now)
	assert.False(t, ok)
}
