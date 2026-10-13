package provider

import (
	"context"
	"encoding/json"
	"testing"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	commonv1alpha1 "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	milvusbackup "github.com/openeverest/provider-milvus/definition/backupclasses/milvus-backup"
	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// fakeClientBuilder knows every type the provider reads and the Restore index
// the runtime registers.
func fakeClientBuilder(t *testing.T) *fake.ClientBuilder {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1alpha1.AddToScheme, corev1.AddToScheme, batchv1.AddToScheme,
		backupv1alpha1.AddToScheme, milvusapi.AddToScheme, monitoringv1.AddToScheme,
	} {
		require.NoError(t, add(scheme))
	}
	return fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&batchv1.Job{}).
		WithIndex(&backupv1alpha1.Restore{}, controller.IndexRestoreInstanceName, func(obj client.Object) []string {
			restore, ok := obj.(*backupv1alpha1.Restore)
			if !ok {
				return nil
			}
			return []string{restore.Spec.InstanceRef.Name}
		})
}

const bundledUserYAML = `
minio:
  address: test-milvus-minio.db
  bucketName: test-milvus
  port: 9000
  accessKeyID: ignored
`

// backupFixtures is a healthy instance with bundled storage, an S3
// BackupStorage and its credentials.
func backupFixtures(version string) []client.Object {
	return []client.Object{
		&milvusapi.Milvus{
			ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
			Spec: milvusapi.MilvusSpec{
				Mode: milvusapi.MilvusModeStandalone,
				Com: milvusapi.MilvusComponents{
					ComponentSpec: milvusapi.ComponentSpec{Version: version},
					Standalone:    &milvusapi.MilvusStandalone{},
				},
				Dep: &milvusapi.MilvusDependencies{Storage: milvusapi.MilvusStorage{SecretRef: "test-milvus-minio"}},
			},
			Status: milvusapi.MilvusStatus{Status: milvusapi.StatusHealthy},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
			Data:       map[string]string{milvusConfigKey: bundledUserYAML},
		},
		&backupv1alpha1.BackupStorage{
			ObjectMeta: metav1.ObjectMeta{Name: "s3", Namespace: "db"},
			Spec: backupv1alpha1.BackupStorageSpec{
				Type: backupv1alpha1.BackupStorageTypeS3,
				S3: &backupv1alpha1.BackupStorageS3Spec{
					Bucket:               "backups",
					Region:               "us-east-1",
					EndpointURL:          "http://minio.storage.svc:9000",
					CredentialsSecretRef: commonv1alpha1.SecretRef{Name: "s3-creds"},
				},
			},
		},
	}
}

func newBackupContext(t *testing.T, objs ...client.Object) *controller.Context {
	t.Helper()
	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Spec:       corev1alpha1.InstanceSpec{ProviderRef: commonv1alpha1.ObjectRef{Name: common.ProviderName}},
	}
	all := append([]client.Object{instance}, objs...)
	return controller.NewContext(context.Background(), fakeClientBuilder(t).WithObjects(all...).Build(), instance, common.ProviderName)
}

func newBackup(name string, params *milvusbackup.MilvusBackupParameters) *backupv1alpha1.Backup {
	backup := &backupv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "db", UID: "backup-uid"},
		Spec: backupv1alpha1.BackupSpec{
			Origin: backupv1alpha1.BackupOrigin{
				Type:        backupv1alpha1.BackupOriginTypeInstance,
				InstanceRef: &commonv1alpha1.ObjectRef{Name: "test-milvus"},
			},
			ClassRef:   commonv1alpha1.ObjectRef{Name: "milvus-backup"},
			StorageRef: commonv1alpha1.ObjectRef{Name: "s3"},
		},
	}
	if params != nil {
		raw, _ := json.Marshal(params)
		backup.Spec.Parameters = &runtime.RawExtension{Raw: raw}
	}
	return backup
}

func getJob(t *testing.T, c *controller.Context, name string) *batchv1.Job {
	t.Helper()
	job := &batchv1.Job{}
	require.NoError(t, c.Get(job, name))
	return job
}

func envSource(job *batchv1.Job, name string) string {
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == name && env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil {
			return env.ValueFrom.SecretKeyRef.Name + "/" + env.ValueFrom.SecretKeyRef.Key
		}
	}
	return ""
}

func setJobCondition(t *testing.T, c *controller.Context, job *batchv1.Job, condition batchv1.JobConditionType) {
	t.Helper()
	job.Status.StartTime = &metav1.Time{}
	job.Status.Conditions = []batchv1.JobCondition{{Type: condition, Status: corev1.ConditionTrue}}
	require.NoError(t, c.Client().Status().Update(context.Background(), job))
}

func TestParseMilvusObjectStore(t *testing.T) {
	t.Run("bundled storage keeps the operator's bucket and Milvus defaults", func(t *testing.T) {
		store, err := parseMilvusObjectStore(bundledUserYAML)
		require.NoError(t, err)
		assert.Equal(t, backupObjectStore{
			Provider: "minio", Address: "test-milvus-minio.db", Port: 9000,
			Bucket: "test-milvus", RootPath: "files",
		}, store)
	})

	t.Run("external storage keeps its own settings", func(t *testing.T) {
		store, err := parseMilvusObjectStore(`
minio:
  address: s3.us-west-2.amazonaws.com
  port: "443"
  useSSL: true
  bucketName: vectors
  rootPath: prod
  cloudProvider: aws
  useIAM: true
`)
		require.NoError(t, err)
		assert.Equal(t, backupObjectStore{
			Provider: "aws", Address: "s3.us-west-2.amazonaws.com", Port: 443, UseSSL: true,
			Bucket: "vectors", RootPath: "prod", UseIAM: true,
		}, store)
	})

	t.Run("config not rendered yet", func(t *testing.T) {
		_, err := parseMilvusObjectStore("common: {}")
		require.Error(t, err)
	})
}

func TestBackupStorageObjectStore(t *testing.T) {
	storage := func(endpoint string, verifyTLS *bool) *backupv1alpha1.BackupStorage {
		return &backupv1alpha1.BackupStorage{
			ObjectMeta: metav1.ObjectMeta{Name: "s3"},
			Spec: backupv1alpha1.BackupStorageSpec{
				Type: backupv1alpha1.BackupStorageTypeS3,
				S3:   &backupv1alpha1.BackupStorageS3Spec{Bucket: "b", Region: "r", EndpointURL: endpoint, VerifyTLS: verifyTLS},
			},
		}
	}

	store, err := backupStorageObjectStore(storage("http://minio.storage.svc:9000", nil), "root")
	require.NoError(t, err)
	assert.Equal(t, backupObjectStore{Provider: "minio", Address: "minio.storage.svc", Port: 9000, Region: "r", Bucket: "b", RootPath: "root"}, store)

	store, err = backupStorageObjectStore(storage("https://s3.eu-west-1.amazonaws.com", nil), "root")
	require.NoError(t, err)
	assert.Equal(t, "aws", store.Provider)
	assert.Equal(t, 443, store.Port)
	assert.True(t, store.UseSSL)

	_, err = backupStorageObjectStore(storage("https://minio.example.com", ptr.To(false)), "root")
	require.ErrorContains(t, err, "verifyTLS false is not supported")

	// Plain HTTP never verifies certificates, so the flag is irrelevant.
	_, err = backupStorageObjectStore(storage("http://minio.example.com", ptr.To(false)), "root")
	require.NoError(t, err)
}

func TestBackupNames(t *testing.T) {
	assert.Equal(t, "daily_2026_10_12", milvusBackupName("daily-2026-10.12"))
	assert.Equal(t, "nightly-mb", backupJobName("nightly", operationCreate))

	long := "a-very-long-backup-name-that-a-schedule-produced-for-an-instance-1234567890"
	name := backupJobName(long, operationRestore)
	assert.LessOrEqual(t, len(name), 63)
	assert.Equal(t, name, backupJobName(long, operationRestore))
	assert.NotEqual(t, name, backupJobName(long+"x", operationRestore))
}

func TestResolveBackupLocation(t *testing.T) {
	c := newBackupContext(t, backupFixtures("2.6.15")...)

	location, err := resolveBackupLocation(c, newBackup("daily-1", nil))
	require.NoError(t, err)
	assert.Equal(t, "everest/milvus/db/test-milvus", location.rootPath)
	assert.Equal(t, "daily_1", location.name)

	external := newBackup("imported", nil)
	external.Spec.Origin = backupv1alpha1.BackupOrigin{
		Type:     backupv1alpha1.BackupOriginTypeExternal,
		External: &backupv1alpha1.BackupOriginExternal{Path: "/old/prefix/backup_7/"},
	}
	location, err = resolveBackupLocation(c, external)
	require.NoError(t, err)
	assert.Equal(t, "old/prefix", location.rootPath)
	assert.Equal(t, "backup_7", location.name)
}

func TestSyncBackup(t *testing.T) {
	t.Run("runs milvus-backup create with credentials from Secrets", func(t *testing.T) {
		c := newBackupContext(t, backupFixtures("2.6.15")...)
		backup := newBackup("daily-1", &milvusbackup.MilvusBackupParameters{Collections: "db1.*, coll2", IncludeRBAC: true})

		status, err := New().SyncBackup(c, backup)
		require.NoError(t, err)
		assert.Equal(t, backupv1alpha1.BackupStatePending, status.State)
		assert.Equal(t, "daily-1-mb", status.OperatorBackupRef.Name)

		job := getJob(t, c, "daily-1-mb")
		container := job.Spec.Template.Spec.Containers[0]
		assert.Equal(t, []string{"create", "-n", "daily_1", "--filter", "db1.*,coll2", "--rbac", "--config", "/etc/milvus-backup/backup.yaml"}, container.Args)
		// Milvus may have a rotation pending; only the applied password works.
		assert.Equal(t, "test-milvus-credentials/appliedPassword", envSource(job, "MILVUS_PASSWORD"))
		assert.Equal(t, "test-milvus-minio/accesskey", envSource(job, "MILVUS_STORAGE_AUTH_ACCESS_KEY_ID"))
		assert.Equal(t, "s3-creds/AWS_SECRET_ACCESS_KEY", envSource(job, "BACKUP_STORAGE_AUTH_SECRET_ACCESS_KEY"))
		assert.Equal(t, ptr.To(false), job.Spec.Template.Spec.AutomountServiceAccountToken)
		assert.Equal(t, "backup-uid", string(job.OwnerReferences[0].UID))

		cm := &corev1.ConfigMap{}
		require.NoError(t, c.Get(cm, "daily-1-mb"))
		config := cm.Data["backup.yaml"]
		assert.Contains(t, config, "bucketName: test-milvus")
		assert.Contains(t, config, "rootPath: everest/milvus/db/test-milvus")
		assert.Contains(t, config, "address: test-milvus-milvus.db.svc")
		assert.NotContains(t, config, "ignored", "credentials stay out of the ConfigMap")
	})

	t.Run("maps the Job outcome", func(t *testing.T) {
		c := newBackupContext(t, backupFixtures("2.6.15")...)
		p := New()
		backup := newBackup("daily-1", nil)
		_, err := p.SyncBackup(c, backup)
		require.NoError(t, err)
		job := getJob(t, c, "daily-1-mb")

		setJobCondition(t, c, job, batchv1.JobComplete)
		status, err := p.SyncBackup(c, backup)
		require.NoError(t, err)
		assert.Equal(t, backupv1alpha1.BackupStateSucceeded, status.State)
	})

	t.Run("reports the tail of milvus-backup's output on failure", func(t *testing.T) {
		c := newBackupContext(t, backupFixtures("2.6.15")...)
		p := New()
		backup := newBackup("daily-1", nil)
		_, err := p.SyncBackup(c, backup)
		require.NoError(t, err)
		require.NoError(t, c.Client().Create(context.Background(), &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "daily-1-mb-x", Namespace: "db", Labels: map[string]string{batchv1.JobNameLabel: "daily-1-mb"}},
			Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: "line 1\nline 2\nline 3\nbucket does not exist"}},
			}}},
		}))
		setJobCondition(t, c, getJob(t, c, "daily-1-mb"), batchv1.JobFailed)

		status, err := p.SyncBackup(c, backup)
		require.NoError(t, err)
		assert.Equal(t, backupv1alpha1.BackupStateFailed, status.State)
		assert.Equal(t, "line 2\nline 3\nbucket does not exist", status.Message)
	})

	t.Run("prefers milvus-backup's error line", func(t *testing.T) {
		assert.Equal(t, "Error: create: create backup storage client: Access Key Id does not exist",
			failureSummary("config: backup.yaml\n[INFO] create backup storage client\nError: create: create backup storage client: Access Key Id does not exist\n"))
	})

	t.Run("waits for a healthy Milvus", func(t *testing.T) {
		objs := backupFixtures("2.6.15")
		milvus, ok := objs[0].(*milvusapi.Milvus)
		require.True(t, ok)
		milvus.Status.Status = milvusapi.StatusPending
		c := newBackupContext(t, objs...)

		status, err := New().SyncBackup(c, newBackup("daily-1", nil))
		require.NoError(t, err)
		assert.Equal(t, backupv1alpha1.BackupStatePending, status.State)
		assert.Equal(t, "waiting for Milvus to be healthy", status.Message)
	})

	t.Run("runs one milvus-backup Job per instance at a time", func(t *testing.T) {
		c := newBackupContext(t, backupFixtures("2.6.15")...)
		p := New()
		_, err := p.SyncBackup(c, newBackup("first", nil))
		require.NoError(t, err)

		status, err := p.SyncBackup(c, newBackup("second", nil))
		require.NoError(t, err)
		assert.Equal(t, backupv1alpha1.BackupStatePending, status.State)
		assert.Equal(t, "waiting for first-mb to finish", status.Message)

		setJobCondition(t, c, getJob(t, c, "first-mb"), batchv1.JobComplete)
		_, err = p.SyncBackup(c, newBackup("second", nil))
		require.NoError(t, err)
		getJob(t, c, "second-mb")
	})
}

func newRestore(params *milvusbackup.MilvusRestoreParameters) *backupv1alpha1.Restore {
	restore := &backupv1alpha1.Restore{
		ObjectMeta: metav1.ObjectMeta{Name: "restore-1", Namespace: "db", UID: "restore-uid"},
		Spec: backupv1alpha1.RestoreSpec{
			InstanceRef: commonv1alpha1.ObjectRef{Name: "test-milvus"},
			DataSource: backupv1alpha1.DataSource{
				Type:   backupv1alpha1.DataSourceTypeBackup,
				Backup: &backupv1alpha1.DataSourceBackup{BackupRef: commonv1alpha1.ObjectRef{Name: "daily-1"}},
			},
		},
	}
	if params != nil {
		raw, _ := json.Marshal(params)
		restore.Spec.Parameters = &runtime.RawExtension{Raw: raw}
	}
	return restore
}

func succeededBackup(name, origin string) *backupv1alpha1.Backup {
	backup := newBackup(name, nil)
	backup.Spec.Origin.InstanceRef.Name = origin
	backup.Status.State = backupv1alpha1.BackupStateSucceeded
	return backup
}

func TestSyncRestore(t *testing.T) {
	t.Run("replaces collections from another instance's backup and rebuilds indexes", func(t *testing.T) {
		c := newBackupContext(t, append(backupFixtures("2.6.15"), succeededBackup("daily-1", "other"))...)

		status, err := New().SyncRestore(c, newRestore(nil))
		require.NoError(t, err)
		assert.Equal(t, backupv1alpha1.RestoreStatePending, status.State)

		job := getJob(t, c, "restore-1-mr")
		assert.Equal(t, []string{"restore", "-n", "daily_1", "--rebuild_index", "--drop_exist_collection", "--config", "/etc/milvus-backup/backup.yaml"},
			job.Spec.Template.Spec.Containers[0].Args)
		cm := &corev1.ConfigMap{}
		require.NoError(t, c.Get(cm, "restore-1-mr"))
		assert.Contains(t, cm.Data["backup.yaml"], "rootPath: everest/milvus/db/other", "data is read from the source instance's path")
		assert.Contains(t, cm.Data["backup.yaml"], "bucketName: test-milvus", "and written into the target's bucket")
	})

	t.Run("can keep existing collections and restore next to them", func(t *testing.T) {
		c := newBackupContext(t, append(backupFixtures("2.6.15"), succeededBackup("daily-1", "test-milvus"))...)

		_, err := New().SyncRestore(c, newRestore(&milvusbackup.MilvusRestoreParameters{
			Collections: "coll", CollectionSuffix: "_restored", KeepExistingCollections: true,
		}))
		require.NoError(t, err)
		assert.Equal(t, []string{"restore", "-n", "daily_1", "--rebuild_index", "--filter", "coll", "--suffix", "_restored", "--config", "/etc/milvus-backup/backup.yaml"},
			getJob(t, c, "restore-1-mr").Spec.Template.Spec.Containers[0].Args)
	})

	t.Run("refuses Milvus versions that corrupt restored data", func(t *testing.T) {
		c := newBackupContext(t, append(backupFixtures("2.6.11"), succeededBackup("daily-1", "test-milvus"))...)

		status, err := New().SyncRestore(c, newRestore(nil))
		require.NoError(t, err)
		assert.Equal(t, backupv1alpha1.RestoreStateFailed, status.State)
		assert.Equal(t, "restoring needs Milvus 2.6.12 or newer; this instance runs 2.6.11", status.Message)
	})

	t.Run("waits for the source Backup", func(t *testing.T) {
		running := succeededBackup("daily-1", "test-milvus")
		running.Status.State = backupv1alpha1.BackupStateRunning
		c := newBackupContext(t, append(backupFixtures("2.6.15"), running)...)

		status, err := New().SyncRestore(c, newRestore(nil))
		require.NoError(t, err)
		assert.Equal(t, backupv1alpha1.RestoreStatePending, status.State)
	})

	t.Run("rejects an unsafe collection suffix", func(t *testing.T) {
		c := newBackupContext(t, append(backupFixtures("2.6.15"), succeededBackup("daily-1", "test-milvus"))...)

		status, err := New().SyncRestore(c, newRestore(&milvusbackup.MilvusRestoreParameters{CollectionSuffix: "a; rm"}))
		require.NoError(t, err)
		assert.Equal(t, backupv1alpha1.RestoreStateFailed, status.State)
	})
}

func TestCleanupBackup(t *testing.T) {
	// The runtime moves a Backup to Deleting before it asks for cleanup.
	finishedBackup := func(t *testing.T, c *controller.Context, name string, outcome batchv1.JobConditionType) *backupv1alpha1.Backup {
		t.Helper()
		backup := newBackup(name, nil)
		_, err := New().SyncBackup(c, backup)
		require.NoError(t, err)
		setJobCondition(t, c, getJob(t, c, name+"-mb"), outcome)
		backup.Status.State = backupv1alpha1.BackupStateDeleting
		return backup
	}

	t.Run("deletes the data of a successful backup", func(t *testing.T) {
		c := newBackupContext(t, backupFixtures("2.6.15")...)
		p := New()
		backup := finishedBackup(t, c, "daily-1", batchv1.JobComplete)

		done, err := p.CleanupBackup(c, backup)
		require.NoError(t, err)
		assert.False(t, done)
		job := getJob(t, c, "daily-1-md")
		assert.Equal(t, []string{"delete", "-n", "daily_1", "--config", "/etc/milvus-backup/backup.yaml"}, job.Spec.Template.Spec.Containers[0].Args)
		assert.Equal(t, "s3-creds/AWS_ACCESS_KEY_ID", envSource(job, "MILVUS_STORAGE_AUTH_ACCESS_KEY_ID"))

		setJobCondition(t, c, job, batchv1.JobComplete)
		done, err = p.CleanupBackup(c, backup)
		require.NoError(t, err)
		assert.True(t, done)
	})

	t.Run("keeps retained data and has nothing to delete for failed backups", func(t *testing.T) {
		c := newBackupContext(t, backupFixtures("2.6.15")...)
		retained := finishedBackup(t, c, "daily-1", batchv1.JobComplete)
		retained.Spec.DeletionPolicy = backupv1alpha1.BackupDeletionPolicyRetain
		failed := finishedBackup(t, c, "daily-2", batchv1.JobFailed)

		for _, backup := range []*backupv1alpha1.Backup{retained, failed} {
			done, err := New().CleanupBackup(c, backup)
			require.NoError(t, err)
			assert.True(t, done)
		}
		jobs := &batchv1.JobList{}
		require.NoError(t, c.Client().List(context.Background(), jobs, client.MatchingLabels{backupJobOperationLabel: string(operationDelete)}))
		assert.Empty(t, jobs.Items)
	})
}

func TestStatusReportsRestoring(t *testing.T) {
	objs := backupFixtures("2.6.15")
	restore := newRestore(nil)
	restore.Status.State = backupv1alpha1.RestoreStateRunning
	c := newBackupContext(t, append(objs, restore)...)

	status, err := New().Status(c)
	require.NoError(t, err)
	assert.Equal(t, corev1alpha1.InstancePhaseRestoring, status.Phase)

	restore.Status.State = backupv1alpha1.RestoreStateSucceeded
	require.NoError(t, c.Client().Update(context.Background(), restore))
	status, err = New().Status(c)
	require.NoError(t, err)
	assert.NotEqual(t, corev1alpha1.InstancePhaseRestoring, status.Phase)
}
