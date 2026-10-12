package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

func TestMilvusRESTClientUpdatePassword(t *testing.T) {
	var gotPath, gotAuth string
	var gotBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		_, _ = w.Write([]byte(`{"code":0,"data":{}}`))
	}))
	defer server.Close()

	require.NoError(t, newMilvusRESTClient().UpdatePassword(t.Context(), server.URL, "old", "new"))

	assert.Equal(t, "/v2/vectordb/users/update_password", gotPath)
	assert.Equal(t, "Bearer root:old", gotAuth)
	assert.Equal(t, map[string]string{"userName": "root", "password": "old", "newPassword": "new"}, gotBody)
}

func TestMilvusRESTClientErrors(t *testing.T) {
	tests := map[string]struct {
		status  int
		body    string
		wantErr string
	}{
		"api error in a 200 response": {status: http.StatusOK, body: `{"code":1800,"message":"auth check failure"}`, wantErr: "milvus error 1800: auth check failure"},
		"http error":                  {status: http.StatusServiceUnavailable, wantErr: "HTTP 503"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()

			err := newMilvusRESTClient().Authenticate(t.Context(), server.URL, "pw")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.NotContains(t, err.Error(), "pw", "errors must not leak the password")
		})
	}
}

type fakeRootPasswordAPI struct {
	accepted string
	updates  [][2]string
}

func (f *fakeRootPasswordAPI) UpdatePassword(_ context.Context, _, oldPassword, newPassword string) error {
	f.updates = append(f.updates, [2]string{oldPassword, newPassword})
	if oldPassword != f.accepted {
		return errors.New("old password not correct")
	}
	f.accepted = newPassword
	return nil
}

func (f *fakeRootPasswordAPI) Authenticate(_ context.Context, _, password string) error {
	if password != f.accepted {
		return errors.New("auth check failure")
	}
	return nil
}

// rotationFixture syncs a standalone instance, marks Milvus healthy and wires
// a fake API that accepts the seeded password.
func rotationFixture(t *testing.T) (*Provider, *controller.Context, *fakeRootPasswordAPI, string) {
	t.Helper()
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology:   &corev1alpha1.TopologySpec{Type: "standalone"},
		Components: map[string]corev1alpha1.ComponentSpec{common.ComponentStandalone: {}},
	})
	p := New()
	require.NoError(t, p.Sync(c))

	creds, err := ensureCredentials(c)
	require.NoError(t, err)
	api := &fakeRootPasswordAPI{accepted: creds.password}
	p.rootPassword = api
	setMilvusStatus(t, c, milvusapi.StatusHealthy)
	return p, c, api, creds.password
}

func setMilvusStatus(t *testing.T, c *controller.Context, status milvusapi.MilvusHealthStatus) {
	t.Helper()
	cr := &milvusapi.Milvus{}
	require.NoError(t, c.Get(cr, c.Name()))
	cr.Status.Status = status
	require.NoError(t, c.Client().Update(c.Context(), cr))
}

// setDesiredPassword stands in for a user editing the credentials Secret.
func setDesiredPassword(t *testing.T, c *controller.Context, password string) {
	t.Helper()
	secret := &corev1.Secret{}
	require.NoError(t, c.Get(secret, credentialsSecretName(c.Name())))
	secret.Data[secretKeyPassword] = []byte(password)
	require.NoError(t, c.Client().Update(c.Context(), secret))
}

func TestSyncRotatesRootPassword(t *testing.T) {
	p, c, api, seeded := rotationFixture(t)
	setDesiredPassword(t, c, "rotated-password")

	require.NoError(t, p.Sync(c))

	assert.Equal(t, [][2]string{{seeded, "rotated-password"}}, api.updates)
	creds, err := ensureCredentials(c)
	require.NoError(t, err)
	assert.False(t, creds.rotationPending())

	status, err := p.Status(c)
	require.NoError(t, err)
	assert.Equal(t, "rotated-password", status.ConnectionDetails.Password)

	require.NoError(t, p.Sync(c))
	assert.Len(t, api.updates, 1, "a completed rotation must not be repeated")
}

func TestSyncWaitsForHealthyMilvusBeforeRotating(t *testing.T) {
	p, c, api, seeded := rotationFixture(t)
	setMilvusStatus(t, c, milvusapi.StatusPending)
	setDesiredPassword(t, c, "rotated-password")

	require.NoError(t, p.Sync(c))

	assert.Empty(t, api.updates)
	creds, err := ensureCredentials(c)
	require.NoError(t, err)
	assert.Equal(t, seeded, creds.appliedPassword)
}

func TestSyncRecordsRotationMilvusAlreadyHas(t *testing.T) {
	p, c, api, _ := rotationFixture(t)
	setDesiredPassword(t, c, "rotated-password")
	// The previous pass changed Milvus but failed before recording it.
	api.accepted = "rotated-password"

	require.NoError(t, p.Sync(c))

	creds, err := ensureCredentials(c)
	require.NoError(t, err)
	assert.False(t, creds.rotationPending())
}

func TestSyncSurfacesFailedRotation(t *testing.T) {
	p, c, api, seeded := rotationFixture(t)
	setDesiredPassword(t, c, "rotated-password")
	api.accepted = "something-else"

	err := p.Sync(c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rotate root password")

	creds, err := ensureCredentials(c)
	require.NoError(t, err)
	assert.Equal(t, seeded, creds.appliedPassword, "the working password must be kept")
	status, err := p.Status(c)
	require.NoError(t, err)
	assert.Equal(t, seeded, status.ConnectionDetails.Password)
}
