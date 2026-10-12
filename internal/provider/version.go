package provider

import (
	"fmt"
	"sort"

	"golang.org/x/mod/semver"

	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// validateNoDowngrade rejects moving any component to an older Milvus
// release. Milvus migrates its metadata forward on upgrade and an older binary
// cannot be trusted to read it back.
func validateNoDowngrade(c *controller.Context) error {
	existing := &milvusapi.Milvus{}
	if err := c.Get(existing, c.Name()); err != nil || existing.Spec.Mode == "" {
		return nil
	}
	desired, err := BuildMilvusSpec(c)
	if err != nil {
		return err
	}
	current := componentVersions(existing.Spec.Com)
	requested := componentVersions(desired.Com)
	names := make([]string, 0, len(requested))
	for name := range requested {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		from, to := current[name], requested[name]
		if !semver.IsValid("v"+from) || !semver.IsValid("v"+to) {
			continue
		}
		if semver.Compare("v"+to, "v"+from) < 0 {
			return fmt.Errorf("component %q cannot be downgraded from %s to %s; choose %s or newer", name, from, to, from)
		}
	}
	return nil
}

// componentVersions maps each deployed component to the Milvus version the
// operator runs it at; a component without its own version inherits the
// instance-wide one.
func componentVersions(com milvusapi.MilvusComponents) map[string]string {
	versions := map[string]string{}
	for name, spec := range milvusComponentSpecs(com) {
		versions[name] = spec.Version
		if spec.Version == "" {
			versions[name] = com.Version
		}
	}
	return versions
}

// applyImageUpdateMode picks how the operator rolls out new images. Its
// dependency-ordered rolling upgrade moves each component only once the ones
// it depends on run the instance-wide image, so it can finish only when every
// component shares that image; components on different images are updated
// all at once instead, or the upgrade would stall forever.
func applyImageUpdateMode(com *milvusapi.MilvusComponents) {
	shared, ok := sharedComponentImage(*com)
	if !ok {
		com.ImageUpdateMode = milvusapi.ImageUpdateModeAll
		return
	}
	com.Image, com.Version = shared.image, shared.version
	com.ImageUpdateMode = milvusapi.ImageUpdateModeRollingUpgrade
}

// sharedComponentImage returns the image and version every component runs,
// or false when they differ.
func sharedComponentImage(com milvusapi.MilvusComponents) (milvusImage, bool) {
	shared := milvusImage{image: com.Image, version: com.Version}
	first := true
	for _, spec := range milvusComponentSpecs(com) {
		image := milvusImage{image: spec.Image, version: spec.Version}
		if first {
			shared, first = image, false
			continue
		}
		if image != shared {
			return milvusImage{}, false
		}
	}
	return shared, true
}

// milvusComponentSpecs returns the spec of every component present in com,
// keyed by Instance component name.
func milvusComponentSpecs(com milvusapi.MilvusComponents) map[string]milvusapi.ComponentSpec {
	specs := map[string]milvusapi.ComponentSpec{}
	if com.Standalone != nil {
		specs[common.ComponentStandalone] = com.Standalone.ComponentSpec
	}
	if com.Proxy != nil {
		specs[common.ComponentProxy] = com.Proxy.ComponentSpec
	}
	if com.MixCoord != nil {
		specs[common.ComponentMixCoord] = com.MixCoord.ComponentSpec
	}
	if com.DataNode != nil {
		specs[common.ComponentDataNode] = com.DataNode.ComponentSpec
	}
	if com.QueryNode != nil {
		specs[common.ComponentQueryNode] = com.QueryNode.ComponentSpec
	}
	if com.StreamingNode != nil {
		specs[common.ComponentStreaming] = com.StreamingNode.ComponentSpec
	}
	return specs
}
