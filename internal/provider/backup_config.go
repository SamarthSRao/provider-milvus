package provider

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// milvusConfigKey is where the operator renders the Milvus config overrides
// in the ConfigMap named after the instance.
const milvusConfigKey = "user.yaml"

// Milvus defaults for object-storage keys the operator does not render.
const (
	defaultMilvusStoragePort = 9000
	defaultMilvusRootPath    = "files"
)

// backupObjectStore is one side of a milvus-backup storage config.
type backupObjectStore struct {
	Provider string
	Address  string
	Port     int
	UseSSL   bool
	Region   string
	Bucket   string
	RootPath string
	UseIAM   bool
}

func (s backupObjectStore) values() map[string]any {
	auth := map[string]any{"type": "static"}
	if s.UseIAM {
		auth["type"] = "iam"
	}
	return map[string]any{
		"provider":   s.Provider,
		"address":    s.Address,
		"port":       s.Port,
		"useSSL":     s.UseSSL,
		"region":     s.Region,
		"bucketName": s.Bucket,
		"rootPath":   s.RootPath,
		"auth":       auth,
	}
}

// milvusObjectStore resolves the object storage Milvus itself uses from the
// config the operator rendered, so operator defaults (such as the bucket
// named after the instance) are read rather than re-derived.
func milvusObjectStore(c *controller.Context, instance string) (backupObjectStore, error) {
	cm := &corev1.ConfigMap{}
	if err := c.Get(cm, instance); err != nil {
		return backupObjectStore{}, fmt.Errorf("read Milvus config: %w", err)
	}
	return parseMilvusObjectStore(cm.Data[milvusConfigKey])
}

func parseMilvusObjectStore(userYAML string) (backupObjectStore, error) {
	var config struct {
		Minio struct {
			Address       string `json:"address"`
			Port          any    `json:"port"`
			UseSSL        bool   `json:"useSSL"`
			Region        string `json:"region"`
			BucketName    string `json:"bucketName"`
			RootPath      string `json:"rootPath"`
			UseIAM        bool   `json:"useIAM"`
			CloudProvider string `json:"cloudProvider"`
		} `json:"minio"`
	}
	if err := yaml.Unmarshal([]byte(userYAML), &config); err != nil {
		return backupObjectStore{}, fmt.Errorf("parse Milvus config: %w", err)
	}
	minio := config.Minio
	if minio.Address == "" || minio.BucketName == "" {
		return backupObjectStore{}, fmt.Errorf("the Milvus config has no object storage address or bucket yet")
	}
	port := defaultMilvusStoragePort
	if minio.Port != nil {
		parsed, err := strconv.Atoi(fmt.Sprint(minio.Port))
		if err != nil {
			return backupObjectStore{}, fmt.Errorf("parse Milvus object storage port %v: %w", minio.Port, err)
		}
		port = parsed
	}
	store := backupObjectStore{
		Provider: "minio",
		Address:  minio.Address,
		Port:     port,
		UseSSL:   minio.UseSSL,
		Region:   minio.Region,
		Bucket:   minio.BucketName,
		RootPath: minio.RootPath,
		UseIAM:   minio.UseIAM,
	}
	if minio.CloudProvider != "" {
		store.Provider = minio.CloudProvider
	}
	if store.RootPath == "" {
		store.RootPath = defaultMilvusRootPath
	}
	return store, nil
}

// backupStorageObjectStore maps an S3 BackupStorage onto the milvus-backup
// backup side, with backups of an instance kept under rootPath.
func backupStorageObjectStore(bs *backupv1alpha1.BackupStorage, rootPath string) (backupObjectStore, error) {
	if bs.Spec.Type != backupv1alpha1.BackupStorageTypeS3 || bs.Spec.S3 == nil {
		return backupObjectStore{}, fmt.Errorf("BackupStorage %q: only s3 storage is supported", bs.Name)
	}
	s3 := bs.Spec.S3
	endpoint, err := url.Parse(s3.EndpointURL)
	if err != nil || endpoint.Host == "" {
		return backupObjectStore{}, fmt.Errorf("BackupStorage %q: invalid endpointURL %q", bs.Name, s3.EndpointURL)
	}
	useSSL := endpoint.Scheme == "https"
	if useSSL && s3.VerifyTLS != nil && !*s3.VerifyTLS {
		return backupObjectStore{}, fmt.Errorf("BackupStorage %q: verifyTLS false is not supported by milvus-backup", bs.Name)
	}
	host, portText, err := net.SplitHostPort(endpoint.Host)
	if err != nil {
		host, portText = endpoint.Host, "80"
		if useSSL {
			portText = "443"
		}
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return backupObjectStore{}, fmt.Errorf("BackupStorage %q: invalid endpoint port %q", bs.Name, portText)
	}
	provider := "minio"
	if strings.HasSuffix(host, ".amazonaws.com") {
		provider = "aws"
	}
	return backupObjectStore{
		Provider: provider,
		Address:  host,
		Port:     port,
		UseSSL:   useSSL,
		Region:   s3.Region,
		Bucket:   s3.Bucket,
		RootPath: rootPath,
	}, nil
}

// milvusBackupConfig renders the milvus-backup config file. Credentials are
// never written here: milvus-backup reads them from environment variables.
func milvusBackupConfig(instance, namespace string, milvus, backup backupObjectStore) (string, error) {
	service := fmt.Sprintf("%s-milvus.%s.svc", instance, namespace)
	config := map[string]any{
		"configVersion": "v2",
		"log": map[string]any{
			"level":   "info",
			"console": true,
			// The root filesystem is read-only; /tmp is an emptyDir.
			"file": map[string]any{"path": "/tmp/milvus-backup.log"},
		},
		"milvus": map[string]any{
			"user":       rootUsername,
			"grpc":       map[string]any{"address": service, "port": milvusGRPCPort},
			"management": map[string]any{"endpoint": fmt.Sprintf("http://%s:%d", service, milvusManagementPort)},
			"storage":    milvus.values(),
		},
		"backup": map[string]any{
			"storage": backup.values(),
		},
	}
	out, err := yaml.Marshal(config)
	if err != nil {
		return "", fmt.Errorf("render milvus-backup config: %w", err)
	}
	return string(out), nil
}

// Ports of the Milvus Service in front of the proxy (or standalone) pods.
const (
	milvusGRPCPort       = 19530
	milvusManagementPort = 9091
)

// milvusStorageSecret is the Secret holding Milvus's own object-storage keys,
// or empty when the instance authenticates with IAM.
func milvusStorageSecret(cr *milvusapi.Milvus, store backupObjectStore) string {
	if store.UseIAM || cr.Spec.Dep == nil {
		return ""
	}
	return cr.Spec.Dep.Storage.SecretRef
}
