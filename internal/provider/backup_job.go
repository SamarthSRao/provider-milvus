package provider

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
)

// Labels on every milvus-backup Job, used to serialize the Jobs of an instance.
const (
	backupJobInstanceLabel  = "backup.milvus.openeverest.io/instance"
	backupJobOperationLabel = "backup.milvus.openeverest.io/operation"
)

type backupOperation string

const (
	operationCreate  backupOperation = "create"
	operationRestore backupOperation = "restore"
	operationDelete  backupOperation = "delete"
)

var operationSuffix = map[backupOperation]string{
	operationCreate:  "-mb",
	operationRestore: "-mr",
	operationDelete:  "-md",
}

const defaultMilvusBackupImage = "docker.io/milvusdb/milvus-backup:v0.6.0"

// backupJobName names the Job (and its ConfigMap) of an operation on owner,
// shortened with a hash when it would exceed the Kubernetes name limit.
func backupJobName(owner string, op backupOperation) string {
	const maxNameLength = 63
	name := owner + operationSuffix[op]
	if len(name) <= maxNameLength {
		return name
	}
	sum := sha256.Sum256([]byte(name))
	hash := hex.EncodeToString(sum[:])[:8]
	keep := maxNameLength - len(operationSuffix[op]) - len(hash) - 1
	return strings.TrimRight(owner[:keep], "-.") + "-" + hash + operationSuffix[op]
}

// milvusBackupName maps a Backup name onto milvus-backup's [A-Za-z0-9_] names.
func milvusBackupName(backupName string) string {
	return strings.NewReplacer("-", "_", ".", "_").Replace(backupName)
}

// backupRootPath keeps every instance's backups apart inside a shared bucket
// and is derivable from a Backup alone, so any instance can restore them.
func backupRootPath(namespace, instance string) string {
	return fmt.Sprintf("everest/milvus/%s/%s", namespace, instance)
}

// backupLocation is where a Backup's data lives in its BackupStorage.
type backupLocation struct {
	storage  *backupv1alpha1.BackupStorage
	rootPath string
	name     string
}

func resolveBackupLocation(c *controller.Context, backup *backupv1alpha1.Backup) (backupLocation, error) {
	bs, err := c.BackupStorage(backup.Spec.StorageRef.Name)
	if err != nil {
		return backupLocation{}, err
	}
	location := backupLocation{storage: bs}
	switch backup.Spec.Origin.Type {
	case backupv1alpha1.BackupOriginTypeExternal:
		if backup.Spec.Origin.External == nil {
			return backupLocation{}, fmt.Errorf("external Backup %q has no origin.external.path", backup.Name)
		}
		path := strings.Trim(backup.Spec.Origin.External.Path, "/")
		cut := strings.LastIndex(path, "/")
		if cut <= 0 {
			return backupLocation{}, fmt.Errorf("external Backup %q path %q must be <rootPath>/<backup name>", backup.Name, path)
		}
		location.rootPath, location.name = path[:cut], path[cut+1:]
	default:
		if backup.Spec.Origin.InstanceRef == nil {
			return backupLocation{}, fmt.Errorf("backup %q has no origin instance", backup.Name)
		}
		location.rootPath = backupRootPath(backup.Namespace, backup.Spec.Origin.InstanceRef.Name)
		location.name = milvusBackupName(backup.Name)
	}
	return location, nil
}

// backupJobSpec describes one milvus-backup run.
type backupJobSpec struct {
	owner     client.Object
	op        backupOperation
	instance  string
	args      []string
	config    string
	env       []corev1.EnvVar
	saName    string
	namespace string
}

func secretEnv(name, secret, key string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: secret},
			Key:                  key,
		}},
	}
}

// backupStorageEnv passes the BackupStorage keys to milvus-backup.
func backupStorageEnv(bs *backupv1alpha1.BackupStorage) []corev1.EnvVar {
	secret := bs.Spec.S3.CredentialsSecretRef.Name
	return []corev1.EnvVar{
		secretEnv("BACKUP_STORAGE_AUTH_ACCESS_KEY_ID", secret, "AWS_ACCESS_KEY_ID"),
		secretEnv("BACKUP_STORAGE_AUTH_SECRET_ACCESS_KEY", secret, "AWS_SECRET_ACCESS_KEY"),
	}
}

// milvusEnv passes the root password Milvus accepts and its storage keys.
func milvusEnv(instance, storageSecret string) []corev1.EnvVar {
	env := []corev1.EnvVar{secretEnv("MILVUS_PASSWORD", credentialsSecretName(instance), secretKeyAppliedPassword)}
	if storageSecret != "" {
		env = append(env,
			secretEnv("MILVUS_STORAGE_AUTH_ACCESS_KEY_ID", storageSecret, storageSecretKeyUser),
			secretEnv("MILVUS_STORAGE_AUTH_SECRET_ACCESS_KEY", storageSecret, storageSecretKeyPassword),
		)
	}
	return env
}

func backupJobResources() corev1.ResourceRequirements {
	var resources corev1.ResourceRequirements
	if raw := os.Getenv("MILVUS_BACKUP_RESOURCES"); raw != "" {
		_ = json.Unmarshal([]byte(raw), &resources)
	}
	return resources
}

func backupJobImage() string {
	if image := os.Getenv("MILVUS_BACKUP_IMAGE"); image != "" {
		return image
	}
	return defaultMilvusBackupImage
}

// ensureBackupJob creates the ConfigMap and Job of an operation once and
// returns the live Job. The Job never changes after creation.
func ensureBackupJob(c *controller.Context, spec backupJobSpec) (*batchv1.Job, error) {
	name := backupJobName(spec.owner.GetName(), spec.op)
	labels := map[string]string{
		"app.kubernetes.io/name":       "milvus-backup",
		"app.kubernetes.io/managed-by": "provider-milvus",
		backupJobInstanceLabel:         spec.instance,
		backupJobOperationLabel:        string(spec.op),
	}

	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: spec.namespace}}
	if _, err := controllerutil.CreateOrUpdate(c.Context(), c.Client(), cm, func() error {
		cm.Labels = labels
		cm.Data = map[string]string{"backup.yaml": spec.config}
		return controllerutil.SetControllerReference(spec.owner, cm, c.Client().Scheme())
	}); err != nil {
		return nil, fmt.Errorf("ensure milvus-backup config: %w", err)
	}

	job := &batchv1.Job{}
	err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: spec.namespace, Name: name}, job)
	if err == nil {
		return job, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get milvus-backup job: %w", err)
	}

	job = &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: spec.namespace, Labels: labels},
		Spec: batchv1.JobSpec{
			// A retried create would collide with the half-written backup.
			BackoffLimit: ptr.To(int32(0)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					ServiceAccountName:           spec.saName,
					AutomountServiceAccountToken: ptr.To(spec.saName != ""),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot:   ptr.To(true),
						RunAsUser:      ptr.To(int64(65532)),
						RunAsGroup:     ptr.To(int64(65532)),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{{
						Name:                     "milvus-backup",
						Image:                    backupJobImage(),
						Command:                  []string{"/app/milvus-backup"},
						Args:                     append(spec.args, "--config", "/etc/milvus-backup/backup.yaml"),
						Env:                      spec.env,
						Resources:                backupJobResources(),
						TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: ptr.To(false),
							ReadOnlyRootFilesystem:   ptr.To(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "config", MountPath: "/etc/milvus-backup", ReadOnly: true},
							{Name: "tmp", MountPath: "/tmp"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: name},
						}}},
						{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		},
	}
	if err := controllerutil.SetControllerReference(spec.owner, job, c.Client().Scheme()); err != nil {
		return nil, err
	}
	if err := c.Client().Create(c.Context(), job); err != nil {
		return nil, fmt.Errorf("create milvus-backup job: %w", err)
	}
	return job, nil
}

// activeBackupJob returns another unfinished milvus-backup Job of the
// instance: two runs at once would pause and resume GC under each other.
func activeBackupJob(c *controller.Context, namespace, instance, except string) (string, error) {
	jobs := &batchv1.JobList{}
	if err := c.Client().List(c.Context(), jobs,
		client.InNamespace(namespace),
		client.MatchingLabels{backupJobInstanceLabel: instance},
	); err != nil {
		return "", fmt.Errorf("list milvus-backup jobs: %w", err)
	}
	names := []string{}
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if job.Name == except || job.DeletionTimestamp != nil {
			continue
		}
		if _, finished := jobFinished(job); !finished {
			names = append(names, job.Name)
		}
	}
	if len(names) == 0 {
		return "", nil
	}
	sort.Strings(names)
	return names[0], nil
}

// jobFinished reports whether the Job completed (true) or failed (false), and
// whether it finished at all.
func jobFinished(job *batchv1.Job) (succeeded, finished bool) {
	for _, condition := range job.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		switch condition.Type {
		case batchv1.JobComplete:
			return true, true
		case batchv1.JobFailed:
			return false, true
		}
	}
	return false, false
}

// jobFinishedAt is when the Job reached its terminal condition.
func jobFinishedAt(job *batchv1.Job) *metav1.Time {
	if job.Status.CompletionTime != nil {
		return job.Status.CompletionTime
	}
	for _, condition := range job.Status.Conditions {
		if condition.Status == corev1.ConditionTrue &&
			(condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed) {
			return &condition.LastTransitionTime
		}
	}
	return nil
}

// jobFailureMessage is the tail of milvus-backup's output, which Kubernetes
// keeps as the termination message of a failed container.
func jobFailureMessage(c *controller.Context, job *batchv1.Job) string {
	pods := &corev1.PodList{}
	if err := c.Client().List(c.Context(), pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{batchv1.JobNameLabel: job.Name},
	); err != nil {
		return "milvus-backup failed"
	}
	for i := range pods.Items {
		for _, status := range pods.Items[i].Status.ContainerStatuses {
			if status.State.Terminated != nil && status.State.Terminated.Message != "" {
				return failureSummary(status.State.Terminated.Message)
			}
		}
	}
	return "milvus-backup failed"
}

// failureSummary keeps milvus-backup's final "Error:" line, or the last few
// log lines when it printed none.
func failureSummary(output string) string {
	output = strings.TrimSpace(output)
	if i := strings.LastIndex(output, "\nError:"); i >= 0 {
		return strings.TrimSpace(output[i+1:])
	}
	return lastLines(output, 3)
}

func lastLines(text string, n int) string {
	lines := strings.Split(strings.TrimSpace(text), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// backupJobState maps a milvus-backup Job onto Backup/Restore states.
type backupJobState struct {
	running     bool
	succeeded   bool
	failed      bool
	message     string
	startedAt   *metav1.Time
	completedAt *metav1.Time
}

func readBackupJobState(c *controller.Context, job *batchv1.Job) backupJobState {
	state := backupJobState{startedAt: job.Status.StartTime}
	succeeded, finished := jobFinished(job)
	switch {
	case finished && succeeded:
		state.succeeded = true
		state.completedAt = jobFinishedAt(job)
	case finished:
		state.failed = true
		state.completedAt = jobFinishedAt(job)
		state.message = jobFailureMessage(c, job)
	default:
		state.running = job.Status.StartTime != nil
	}
	return state
}

// filterArg turns a comma-separated collection list into --filter.
func filterArg(collections string) []string {
	parts := []string{}
	for _, part := range strings.Split(collections, ",") {
		if part = strings.TrimSpace(part); part != "" {
			parts = append(parts, part)
		}
	}
	if len(parts) == 0 {
		return nil
	}
	return []string{"--filter", strings.Join(parts, ",")}
}
