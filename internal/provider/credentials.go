package provider

import (
	"crypto/rand"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

const (
	// rootUsername is the built-in Milvus superuser created on first boot.
	rootUsername = "root"
	// credentialsSecretSuffix is appended to the Instance name to form the
	// provider-owned Secret that persists the generated root password.
	credentialsSecretSuffix = "-credentials"
	// secretKeyUsername and secretKeyPassword are the keys under which the
	// credentials are stored in the Secret.
	secretKeyUsername = "username"
	secretKeyPassword = "password"
	// passwordLength is the generated root password length. Milvus caps
	// defaultRootPassword at 72 characters.
	passwordLength = 24
	// rootPasswordEnv overrides common.security.defaultRootPassword: Milvus
	// ranks environment variables above its config files.
	rootPasswordEnv = "COMMON_SECURITY_DEFAULTROOTPASSWORD"

	// storageSecretSuffix matches the Secret the operator points Milvus at for
	// bundled object storage; the operator and the bundled MinIO chart both
	// read these keys.
	storageSecretSuffix      = "-minio"
	storageSecretKeyUser     = "accesskey"
	storageSecretKeyPassword = "secretkey"
	storageUserLength        = 20
	storagePasswordLength    = 40
)

// passwordAlphabet excludes ambiguous characters and starts every generated
// password with a letter, satisfying Milvus password rules.
const (
	passwordLetters  = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ"
	passwordAlphabet = passwordLetters + "23456789"
)

func credentialsSecretName(instanceName string) string {
	return instanceName + credentialsSecretSuffix
}

func storageSecretName(instanceName string) string {
	return instanceName + storageSecretSuffix
}

// generatePassword returns a random password that starts with a letter and
// draws the remaining characters from an unambiguous alphanumeric alphabet.
func generatePassword(length int) (string, error) {
	out := make([]byte, length)
	if err := fillFromAlphabet(out[:1], passwordLetters); err != nil {
		return "", err
	}
	if err := fillFromAlphabet(out[1:], passwordAlphabet); err != nil {
		return "", err
	}
	return string(out), nil
}

func fillFromAlphabet(dst []byte, alphabet string) error {
	buf := make([]byte, len(dst))
	if _, err := rand.Read(buf); err != nil {
		return fmt.Errorf("generate password: %w", err)
	}
	for i, b := range buf {
		dst[i] = alphabet[int(b)%len(alphabet)]
	}
	return nil
}

// ensureCredentials returns the root credentials for the instance, creating the
// backing Secret with a freshly generated password on first reconcile.
func ensureCredentials(c *controller.Context) (username, password string, err error) {
	password, err = generatePassword(passwordLength)
	if err != nil {
		return "", "", err
	}
	data, err := ensureGeneratedSecret(c, credentialsSecretName(c.Name()), map[string][]byte{
		secretKeyUsername: []byte(rootUsername),
		secretKeyPassword: []byte(password),
	})
	if err != nil {
		return "", "", err
	}
	return rootUsername, string(data[secretKeyPassword]), nil
}

// ensureGeneratedSecret returns the data of the named Secret, persisting
// generated on first use so the values stay stable across reconciles.
func ensureGeneratedSecret(c *controller.Context, name string, generated map[string][]byte) (map[string][]byte, error) {
	existing := &corev1.Secret{}
	if err := c.Get(existing, name); err == nil && hasAllKeys(existing.Data, generated) {
		return existing.Data, nil
	}

	secret := &corev1.Secret{
		ObjectMeta: c.ObjectMeta(name),
		Type:       corev1.SecretTypeOpaque,
		Data:       generated,
	}
	if err := c.Apply(secret); err != nil {
		return nil, fmt.Errorf("apply secret %s: %w", name, err)
	}
	return generated, nil
}

func hasAllKeys(data, want map[string][]byte) bool {
	for key := range want {
		if len(data[key]) == 0 {
			return false
		}
	}
	return true
}

// applyAuthConfig enables authentication and seeds the initial root password
// from the credentials Secret, so the password never lands in the Milvus CR or
// the ConfigMap the operator renders from it. Milvus reads it only when it
// first creates the root user.
func applyAuthConfig(spec *milvusapi.MilvusSpec, credentialsSecret string) {
	if spec.Conf == nil {
		spec.Conf = milvusapi.Values{}
	}
	deepMergeValues(spec.Conf, map[string]any{
		"common": map[string]any{
			"security": map[string]any{"authorizationEnabled": true},
		},
	})
	spec.Com.Env = append(spec.Com.Env, corev1.EnvVar{
		Name: rootPasswordEnv,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: credentialsSecret},
				Key:                  secretKeyPassword,
			},
		},
	})
}

// applyStorageCredentials replaces the chart's default minioadmin credentials
// of bundled object storage with generated per-instance ones.
func applyStorageCredentials(c *controller.Context, spec *milvusapi.MilvusSpec) error {
	if spec.Dep == nil || spec.Dep.Storage.External || spec.Dep.Storage.InCluster == nil {
		return nil
	}
	generated, err := generatedStorageCredentials(c)
	if err != nil || !generated {
		return err
	}

	name := storageSecretName(c.Name())
	user, err := generatePassword(storageUserLength)
	if err != nil {
		return err
	}
	password, err := generatePassword(storagePasswordLength)
	if err != nil {
		return err
	}
	if _, err := ensureGeneratedSecret(c, name, map[string][]byte{
		storageSecretKeyUser:     []byte(user),
		storageSecretKeyPassword: []byte(password),
	}); err != nil {
		return err
	}
	spec.Dep.Storage.InCluster.Values["existingSecret"] = name
	return nil
}

// generatedStorageCredentials reports whether bundled object storage reads the
// provider-generated Secret. Instances whose MinIO predates it keep the chart's
// own Secret of the same name: pointing existingSecret at it makes Helm delete
// it, and MinIO would come back with different root credentials.
func generatedStorageCredentials(c *controller.Context) (bool, error) {
	existing := &milvusapi.Milvus{}
	if err := c.Get(existing, c.Name()); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("get Milvus CR: %w", err)
	}
	if existing.Spec.Dep == nil || existing.Spec.Dep.Storage.External || existing.Spec.Dep.Storage.InCluster == nil {
		return true, nil
	}
	return existing.Spec.Dep.Storage.InCluster.Values["existingSecret"] == storageSecretName(c.Name()), nil
}
