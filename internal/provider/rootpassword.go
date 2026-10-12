package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// rootPasswordAPI changes and checks the Milvus root password.
type rootPasswordAPI interface {
	UpdatePassword(ctx context.Context, baseURL, oldPassword, newPassword string) error
	Authenticate(ctx context.Context, baseURL, password string) error
}

// milvusRESTClient talks to the Milvus RESTful API, which the proxy serves on
// the gRPC port.
type milvusRESTClient struct {
	http *http.Client
}

func newMilvusRESTClient() milvusRESTClient {
	return milvusRESTClient{http: &http.Client{Timeout: 10 * time.Second}}
}

func (m milvusRESTClient) UpdatePassword(ctx context.Context, baseURL, oldPassword, newPassword string) error {
	return m.post(ctx, baseURL+"/v2/vectordb/users/update_password", oldPassword, map[string]string{
		"userName":    rootUsername,
		"password":    oldPassword,
		"newPassword": newPassword,
	})
}

func (m milvusRESTClient) Authenticate(ctx context.Context, baseURL, password string) error {
	return m.post(ctx, baseURL+"/v2/vectordb/users/list", password, map[string]string{})
}

func (m milvusRESTClient) post(ctx context.Context, url, password string, body any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+rootUsername+":"+password)

	resp, err := m.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("milvus returned HTTP %d", resp.StatusCode)
	}
	// Milvus reports API errors as HTTP 200 with a non-zero code.
	var result struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil {
		return fmt.Errorf("decode milvus response: %w", err)
	}
	if result.Code != 0 {
		return fmt.Errorf("milvus error %d: %s", result.Code, result.Message)
	}
	return nil
}

// milvusInternalURL is the in-cluster address of the instance's Milvus API,
// served by the same Service whatever the exposure type.
func milvusInternalURL(c *controller.Context) string {
	return fmt.Sprintf("http://%s-milvus.%s.svc:%s", c.Name(), c.Namespace(), milvusPort)
}

// syncRootPassword rotates the Milvus root password to the one in the
// credentials Secret once Milvus is healthy, then records it as applied.
func (p *Provider) syncRootPassword(c *controller.Context, creds rootCredentials) error {
	if !creds.rotationPending() {
		return nil
	}
	cr := &milvusapi.Milvus{}
	if err := c.Get(cr, c.Name()); err != nil || cr.Status.Status != milvusapi.StatusHealthy {
		// The Milvus CR watch reconciles again once it turns healthy.
		return nil
	}

	baseURL := milvusInternalURL(c)
	if err := p.rootPassword.UpdatePassword(c.Context(), baseURL, creds.appliedPassword, creds.password); err != nil {
		// A rotation interrupted before it was recorded leaves the applied
		// password stale, and Milvus seeds a not-yet-created root user with
		// the new password directly.
		if p.rootPassword.Authenticate(c.Context(), baseURL, creds.password) != nil {
			return fmt.Errorf("rotate root password: %w", err)
		}
	}
	creds.appliedPassword = creds.password
	return applyCredentialsSecret(c, creds)
}

// credentialsSecretChanged limits the Secret watch to the root credentials, so
// edits to the password trigger a rotation.
func credentialsSecretChanged() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return strings.HasSuffix(obj.GetName(), credentialsSecretSuffix)
	})
}
