/*
Copyright 2022.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// IntegrationTestScenarioSpec defines the desired state of IntegrationScenario
type IntegrationTestScenarioSpec struct {
	// Application that's associated with the IntegrationTestScenario
	// +kubebuilder:validation:Pattern=^[a-z0-9]([-a-z0-9]*[a-z0-9])?$
	// +required
	Application string `json:"application"`
	// Release Tekton Pipeline to execute
	// +required
	Pipeline string `json:"pipeline"`
	// Tekton Bundle where to find the pipeline
	// +required
	Bundle string `json:"bundle"`
	// If the pipeline is optional and can be ignored by gating
	Optional string `json:"optional"`
	// Params to pass to the pipeline
	Params []PipelineParameter `json:"params,omitempty"`
	// Environment that will be utilized by the test pipeline
	Environment TestEnvironment `json:"environment,omitempty"`
	// Contexts where this IntegrationTestScenario can be applied
	Contexts []TestContext `json:"contexts,omitempty"`
}

// IntegrationTestScenarioStatus defines the observed state of IntegrationTestScenario
type IntegrationTestScenarioStatus struct {
}

// PipelineParameter contains the name and values of a Tekton Pipeline parameter
type PipelineParameter struct {
	Name  string   `json:"name"`
	Value []string `json:"value"`
}

// TestEnvironment contains the name and values of a Test environment
type TestEnvironment struct {
	Name   string   `json:"name"`
	Type   string   `json:"type"`
	Params []string `json:"params"`
}

// TestContext contains the name and values of a Test context
type TestContext struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Application",type=string,JSONPath=`.spec.application`

// IntegrationTestScenario is the Schema for the integrationtestscenarios API
type IntegrationTestScenario struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   IntegrationTestScenarioSpec   `json:"spec,omitempty"`
	Status IntegrationTestScenarioStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// IntegrationTestScenarioList contains a list of IntegrationTestScenario
type IntegrationTestScenarioList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []IntegrationTestScenario `json:"items"`
}

func init() {
	SchemeBuilder.Register(&IntegrationTestScenario{}, &IntegrationTestScenarioList{})
}
