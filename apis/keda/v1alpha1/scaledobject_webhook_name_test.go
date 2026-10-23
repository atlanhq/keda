/*
Copyright 2026 The KEDA Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func soNamed(name string) *ScaledObject {
	return &ScaledObject{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"}}
}

// temporal-worker-controller names ScaledObjects up to 63 chars, so the derived
// keda-hpa-<name> routinely exceeds 63. The operator truncates that label value
// (controllers/keda/hpa.go), so the webhook must admit it rather than freeze the SO.
func TestVerifyNameAdmitsLongDerivedHPAName(t *testing.T) {
	name := strings.Repeat("a", maxK8sLabelValueLength) // keda-hpa-<63> = 72 chars
	warnings, err := verifyName(soNamed(name), "create", false)
	assert.NoError(t, err)
	assert.Len(t, warnings, 1)
}

func TestVerifyNameRejectsScaledObjectNameOverLabelLimit(t *testing.T) {
	_, err := verifyName(soNamed(strings.Repeat("a", maxK8sLabelValueLength+1)), "create", false)
	assert.Error(t, err)
}

func TestVerifyNameNoWarningWhenDerivedHPANameFits(t *testing.T) {
	name := strings.Repeat("a", maxK8sLabelValueLength-len("keda-hpa-"))
	warnings, err := verifyName(soNamed(name), "update", false)
	assert.NoError(t, err)
	assert.Empty(t, warnings)
}
