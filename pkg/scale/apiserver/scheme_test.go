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

package apiserver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	sandboxv1alpha1 "github.com/doge-rgb/cocoon-sandbox-operator/api/v1alpha1"
	sandboxv1beta1 "github.com/doge-rgb/cocoon-sandbox-operator/api/v1beta1"
)

// TestSchemeServesTheVersionUpstreamClientsSpeak guards the version surface.
// The official agent-sandbox Go SDK builds an AgentsV1alpha1 client and lists
// sandboxes through it, so serving only v1beta1 makes every SDK call 404 on the
// group — with no error the caller can act on, just a wait that never ends.
func TestSchemeServesTheVersionUpstreamClientsSpeak(t *testing.T) {
	beta := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "sb", UID: "u-1"},
		Status: sandboxv1beta1.SandboxStatus{
			NodeName: "n1",
			Conditions: []metav1.Condition{{
				Type:   string(sandboxv1beta1.SandboxConditionReady),
				Status: metav1.ConditionTrue,
				Reason: "Running",
			}},
		},
	}

	out, err := Scheme.ConvertToVersion(beta, sandboxv1alpha1.GroupVersion)
	require.NoError(t, err, "the served v1beta1 object must convert to what the SDK asks for")
	alpha, ok := out.(*sandboxv1alpha1.Sandbox)
	require.True(t, ok)
	assert.Equal(t, "sb", alpha.Name)
	assert.Equal(t, types.UID("u-1"), alpha.UID, "identity must survive the conversion")

	// Readiness is the one field the SDK blocks on.
	require.NotEmpty(t, alpha.Status.Conditions)
	assert.Equal(t, metav1.ConditionTrue, alpha.Status.Conditions[0].Status)

	// And it blocks on a List, not a Get.
	list, err := Scheme.ConvertToVersion(
		&sandboxv1beta1.SandboxList{Items: []sandboxv1beta1.Sandbox{*beta}}, sandboxv1alpha1.GroupVersion)
	require.NoError(t, err)
	assert.Len(t, list.(*sandboxv1alpha1.SandboxList).Items, 1)
}
