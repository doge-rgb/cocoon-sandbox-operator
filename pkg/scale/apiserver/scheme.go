// Copyright 2026 The CocoonStack Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package apiserver assembles the aggregated apiserver that serves
// sandboxes.agents.x-k8s.io by scatter-gathering node inventory, the
// metrics.k8s.io pattern: no per-sandbox object is stored in etcd. It installs a
// custom rest.Storage (backed by a scale.SandboxStore) into a
// genericapiserver.GenericAPIServer rather than the etcd-backed generic
// registry.
package apiserver

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	sandboxv1alpha1 "github.com/doge-rgb/cocoon-sandbox-operator/api/v1alpha1"
	sandboxv1beta1 "github.com/doge-rgb/cocoon-sandbox-operator/api/v1beta1"
)

var (
	// Scheme knows the served sandboxes types and how to convert them.
	Scheme = runtime.NewScheme()
	// Codecs is the serializer for the aggregated group.
	Codecs = serializer.NewCodecFactory(Scheme)
	// ParameterCodec decodes list/get query parameters.
	ParameterCodec = runtime.NewParameterCodec(Scheme)
)

func init() {
	utilruntime.Must(sandboxv1beta1.AddToScheme(Scheme))
	// Register the served types under the internal version too, as an identity
	// version: sandboxes is a virtual, read-only resource with no distinct
	// storage schema, so the external v1beta1 type is also its own internal
	// type. This lets the request pipeline round-trip without hand-written
	// conversions while keeping v1beta1 the served, prioritized version.
	internalGV := schema.GroupVersion{Group: sandboxv1beta1.GroupVersion.Group, Version: runtime.APIVersionInternal}
	Scheme.AddKnownTypes(internalGV, &sandboxv1beta1.Sandbox{}, &sandboxv1beta1.SandboxList{})
	// The action subresources' request/response bodies round-trip through the
	// same pipeline, so they need the identity internal version too.
	Scheme.AddKnownTypes(internalGV,
		&sandboxv1beta1.SandboxPauseOptions{},
		&sandboxv1beta1.SandboxResumeOptions{},
		&sandboxv1beta1.SandboxForkOptions{},
		&sandboxv1beta1.SandboxForkResult{},
		&sandboxv1beta1.SandboxSnapshotOptions{},
		&sandboxv1beta1.SandboxSnapshotResult{},
	)
	// v1alpha1 is served alongside v1beta1 because that is the version the
	// upstream agent-sandbox clients speak — the official Go SDK builds an
	// AgentsV1alpha1 client and lists sandboxes through it, so a group that
	// serves only v1beta1 answers every SDK call with a 404 for the group.
	utilruntime.Must(sandboxv1alpha1.AddToScheme(Scheme))
	utilruntime.Must(registerAlphaConversions(Scheme))
	metav1.AddToGroupVersion(Scheme, sandboxv1alpha1.GroupVersion)
	metav1.AddToGroupVersion(Scheme, sandboxv1beta1.GroupVersion)
	// The common request/response meta types (ListOptions, GetOptions, Status,
	// WatchEvent, ...) live at the "v1" options version the request pipeline
	// decodes against (APIGroupInfo.OptionsExternalVersion defaults to v1).
	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Version: "v1"})
	// v1beta1 first: it is the storage-shaped version the store speaks and the
	// one the internal type is identical to.
	utilruntime.Must(Scheme.SetVersionPriority(sandboxv1beta1.GroupVersion, sandboxv1alpha1.GroupVersion))
}

// registerAlphaConversions teaches the scheme to move between v1alpha1 and the
// internal (v1beta1-shaped) types. The per-object conversions already exist as
// the controller-runtime hub methods the CRD conversion webhook uses; these
// adapters just expose them to the apiserver's request pipeline, which converts
// through the scheme rather than through conversion.Convertible.
func registerAlphaConversions(scheme *runtime.Scheme) error {
	if err := scheme.AddConversionFunc((*sandboxv1alpha1.Sandbox)(nil), (*sandboxv1beta1.Sandbox)(nil),
		func(a, b any, _ conversion.Scope) error {
			return a.(*sandboxv1alpha1.Sandbox).ConvertTo(b.(*sandboxv1beta1.Sandbox))
		}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*sandboxv1beta1.Sandbox)(nil), (*sandboxv1alpha1.Sandbox)(nil),
		func(a, b any, _ conversion.Scope) error {
			return b.(*sandboxv1alpha1.Sandbox).ConvertFrom(a.(*sandboxv1beta1.Sandbox))
		}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*sandboxv1alpha1.SandboxList)(nil), (*sandboxv1beta1.SandboxList)(nil),
		func(a, b any, _ conversion.Scope) error {
			src, dst := a.(*sandboxv1alpha1.SandboxList), b.(*sandboxv1beta1.SandboxList)
			dst.ListMeta = src.ListMeta
			dst.Items = make([]sandboxv1beta1.Sandbox, len(src.Items))
			for i := range src.Items {
				if err := src.Items[i].ConvertTo(&dst.Items[i]); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
		return err
	}
	return scheme.AddConversionFunc((*sandboxv1beta1.SandboxList)(nil), (*sandboxv1alpha1.SandboxList)(nil),
		func(a, b any, _ conversion.Scope) error {
			src, dst := a.(*sandboxv1beta1.SandboxList), b.(*sandboxv1alpha1.SandboxList)
			dst.ListMeta = src.ListMeta
			dst.Items = make([]sandboxv1alpha1.Sandbox, len(src.Items))
			for i := range src.Items {
				if err := dst.Items[i].ConvertFrom(&src.Items[i]); err != nil {
					return err
				}
			}
			return nil
		})
}
