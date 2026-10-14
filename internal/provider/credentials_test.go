package provider

import (
	"testing"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

func TestGeneratePassword(t *testing.T) {
	pw, err := generatePassword(passwordLength)
	require.NoError(t, err)

	assert.Len(t, pw, passwordLength)
	assert.LessOrEqual(t, len(pw), 72, "must fit Milvus defaultRootPassword limit")
	assert.Contains(t, passwordLetters, string(pw[0]), "must start with a letter")

	other, err := generatePassword(passwordLength)
	require.NoError(t, err)
	assert.NotEqual(t, pw, other, "passwords must be random")
}

func TestEnsureCredentialsGeneratesAndPersists(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{})

	creds, err := ensureCredentials(c)
	require.NoError(t, err)
	assert.NotEmpty(t, creds.password)
	assert.False(t, creds.rotationPending(), "a fresh password is the one Milvus gets seeded with")

	secret := &corev1.Secret{}
	require.NoError(t, c.Get(secret, credentialsSecretName(c.Name())))
	assert.Equal(t, rootUsername, string(secret.Data[secretKeyUsername]))
	assert.Equal(t, creds.password, string(secret.Data[secretKeyPassword]))
	assert.Equal(t, creds.password, string(secret.Data[secretKeyAppliedPassword]))
}

func TestEnsureCredentialsIsStableAcrossCalls(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{})

	first, err := ensureCredentials(c)
	require.NoError(t, err)
	second, err := ensureCredentials(c)
	require.NoError(t, err)

	assert.Equal(t, first, second, "existing password must be reused")
}

func TestEnsureCredentialsBackfillsAppliedPassword(t *testing.T) {
	legacy := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus-credentials", Namespace: "db"},
		Data: map[string][]byte{
			secretKeyUsername: []byte(rootUsername),
			secretKeyPassword: []byte("seeded"),
		},
	}
	c := newTestContextWithObjects(t, corev1alpha1.InstanceSpec{}, legacy)

	creds, err := ensureCredentials(c)
	require.NoError(t, err)
	assert.Equal(t, rootCredentials{password: "seeded", appliedPassword: "seeded"}, creds)

	secret := &corev1.Secret{}
	require.NoError(t, c.Get(secret, credentialsSecretName(c.Name())))
	assert.Equal(t, "seeded", string(secret.Data[secretKeyAppliedPassword]),
		"the seeded password must be recorded before the user can change it")
}

func TestApplyAuthConfig(t *testing.T) {
	wantEnv := corev1.EnvVar{
		Name: rootPasswordEnv,
		ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "db-credentials"},
			Key:                  secretKeyPassword,
		}},
	}

	t.Run("enables auth and seeds the password from the secret", func(t *testing.T) {
		spec := &milvusapi.MilvusSpec{}
		applyAuthConfig(spec, "db-credentials")

		security := spec.Conf["common"].(map[string]any)["security"].(map[string]any)
		assert.Equal(t, map[string]any{"authorizationEnabled": true}, security,
			"the password must not be written to spec.config")
		assert.Equal(t, []corev1.EnvVar{wantEnv}, spec.Com.Env)
	})

	t.Run("preserves user-provided config", func(t *testing.T) {
		spec := &milvusapi.MilvusSpec{
			Conf: milvusapi.Values{
				"log":    map[string]any{"level": "debug"},
				"common": map[string]any{"gracefulTime": float64(5000)},
			},
		}
		applyAuthConfig(spec, "db-credentials")

		assert.Equal(t, map[string]any{"level": "debug"}, spec.Conf["log"])
		common := spec.Conf["common"].(map[string]any)
		assert.Equal(t, float64(5000), common["gracefulTime"])
		assert.Equal(t, true, common["security"].(map[string]any)["authorizationEnabled"])
	})
}

func bundledStorageSpec(t *testing.T, c *controller.Context) milvusapi.MilvusSpec {
	t.Helper()
	spec, err := BuildMilvusSpec(c)
	require.NoError(t, err)
	require.NotNil(t, spec.Dep.Storage.InCluster)
	return spec
}

func TestApplyStorageCredentials(t *testing.T) {
	standalone := corev1alpha1.InstanceSpec{Topology: &corev1alpha1.TopologySpec{Type: "standalone"}}

	t.Run("new instance gets generated credentials", func(t *testing.T) {
		c := newTestContext(t, standalone)
		spec := bundledStorageSpec(t, c)

		require.NoError(t, applyStorageCredentials(c, &spec))

		name := storageSecretName(c.Name())
		assert.Equal(t, name, spec.Dep.Storage.InCluster.Values["existingSecret"])
		secret := &corev1.Secret{}
		require.NoError(t, c.Get(secret, name))
		// The operator's storage check and the MinIO chart's env both read these keys.
		assert.Len(t, secret.Data["accesskey"], storageUserLength)
		assert.Len(t, secret.Data["secretkey"], storagePasswordLength)
		// The Silo chart reads these.
		assert.Equal(t, secret.Data["accesskey"], secret.Data["rootUser"])
		assert.Equal(t, secret.Data["secretkey"], secret.Data["rootPassword"])
	})

	t.Run("secret generated before Silo keeps its credentials", func(t *testing.T) {
		name := storageSecretName("test-milvus")
		old := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "db"},
			Data:       map[string][]byte{"accesskey": []byte("user"), "secretkey": []byte("password")},
		}
		c := newTestContextWithObjects(t, standalone, old)
		spec := bundledStorageSpec(t, c)

		require.NoError(t, applyStorageCredentials(c, &spec))

		secret := &corev1.Secret{}
		require.NoError(t, c.Get(secret, name))
		assert.Equal(t, map[string][]byte{
			"accesskey":    []byte("user"),
			"secretkey":    []byte("password"),
			"rootUser":     []byte("user"),
			"rootPassword": []byte("password"),
		}, secret.Data)
	})

	t.Run("credentials stay stable across reconciles", func(t *testing.T) {
		c := newTestContext(t, standalone)
		p := New()
		require.NoError(t, p.Sync(c))
		first := &corev1.Secret{}
		require.NoError(t, c.Get(first, storageSecretName(c.Name())))

		require.NoError(t, p.Sync(c))

		second := &corev1.Secret{}
		require.NoError(t, c.Get(second, storageSecretName(c.Name())))
		assert.Equal(t, first.Data, second.Data)
		cr := &milvusapi.Milvus{}
		require.NoError(t, c.Get(cr, c.Name()))
		assert.Equal(t, storageSecretName(c.Name()), cr.Spec.Dep.Storage.InCluster.Values["existingSecret"])
	})

	t.Run("instance created before keeps the chart's secret", func(t *testing.T) {
		legacy := &milvusapi.Milvus{
			ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
			Spec: milvusapi.MilvusSpec{Dep: &milvusapi.MilvusDependencies{
				Storage: milvusapi.MilvusStorage{InCluster: &milvusapi.InClusterConfig{
					Values: milvusapi.Values{"accessKey": "minioadmin"},
				}},
			}},
		}
		c := newTestContextWithObjects(t, standalone, legacy)
		spec := bundledStorageSpec(t, c)

		require.NoError(t, applyStorageCredentials(c, &spec))

		assert.NotContains(t, spec.Dep.Storage.InCluster.Values, "existingSecret")
		err := c.Get(&corev1.Secret{}, storageSecretName(c.Name()))
		assert.True(t, apierrors.IsNotFound(err), "the chart-owned secret must be left alone")
	})

	t.Run("external storage is untouched", func(t *testing.T) {
		c := newTestContext(t, standalone)
		spec := milvusapi.MilvusSpec{Dep: &milvusapi.MilvusDependencies{
			Storage: milvusapi.MilvusStorage{External: true, SecretRef: "s3"},
		}}

		require.NoError(t, applyStorageCredentials(c, &spec))

		err := c.Get(&corev1.Secret{}, storageSecretName(c.Name()))
		assert.True(t, apierrors.IsNotFound(err))
	})
}
