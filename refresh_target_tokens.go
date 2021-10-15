// Copyright 2020 Bull r.A.S. Atos Technologies - Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"strings"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/locations"
	"github.com/ystia/yorc/v4/log"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/tosca"
)

const targetRequirement = "target"

// RefreshTargetTokens holds propertied of a component refreshing tokens of a target
type RefreshTargetTokens struct {
	KV           *api.KV
	Cfg          config.Configuration
	DeploymentID string
	NodeName     string
	TaskID       string
	User         string
	Operation    prov.Operation
}

// ExecuteAsync is not supported here
func (r *RefreshTargetTokens) ExecuteAsync(ctx context.Context) (*prov.Action, time.Duration, error) {
	return nil, 0, errors.Errorf("Unsupported asynchronous operation %s", r.Operation.Name)
}

// Execute executes a synchronous operation
func (r *RefreshTargetTokens) Execute(ctx context.Context) error {

	var err error
	switch strings.ToLower(r.Operation.Name) {
	case "standard.start":
		var locationProps config.DynamicMap
		var targetNodeName string
		locationProps, targetNodeName, err = r.getLocationTargetFromRequirement(ctx, targetRequirement)
		if err != nil {
			err = errors.Wrapf(err, "Failed to get target associated to %s", r.NodeName)
		} else {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, r.DeploymentID).Registerf(
				"Refreshing access token for target of %s: %s", r.NodeName, targetNodeName)
			log.Printf("RefreshTargetTokens component asks to refresh token for deployment %s\n", r.DeploymentID)
			aaiClient := getAAIClient(r.DeploymentID, locationProps)
			var accessToken string
			accessToken, _, err = aaiClient.RefreshToken(ctx)
			if err != nil {
				break
			}
			err = deployments.SetAttributeForAllInstances(ctx, r.DeploymentID, targetNodeName,
				"access_token", accessToken)
			if err != nil {
				return err
			}

			// For compute instances, the OpenStack token has also to be refreshed
			var isComputeInstance bool
			isComputeInstance, err = deployments.IsNodeDerivedFrom(ctx, r.DeploymentID, targetNodeName, "yorc.nodes.openstack.Compute")
			if err != nil {
				break
			}

			if isComputeInstance {
				err = r.refreshComputeInstanceToken(ctx, targetNodeName)
				if err != nil {
					err = errors.Wrapf(err, "Failed to refresh compute instance token for %s", targetNodeName)
				}
			}

		}
	case "install", "uninstall", "standard.create", "standard.stop", "standard.delete":
		// Nothing to do here
	case tosca.RunnableSubmitOperationName, tosca.RunnableCancelOperationName:
		err = errors.Errorf("Unsupported operation %s", r.Operation.Name)
	default:
		err = errors.Errorf("Unsupported operation %s", r.Operation.Name)
	}

	return err
}

// ResolveExecution resolves inputs before the execution of an operation
func (v *RefreshTargetTokens) ResolveExecution(ctx context.Context) error {
	return nil
}

func (r *RefreshTargetTokens) getLocationTargetFromRequirement(ctx context.Context, requirementName string) (config.DynamicMap, string, error) {
	// First get the associated compute node
	targetNodeName, err := deployments.GetTargetNodeForRequirementByName(ctx,
		r.DeploymentID, r.NodeName, requirementName)
	if err != nil {
		return nil, targetNodeName, err
	}

	locationMgr, err := locations.GetManager(r.Cfg)
	if err != nil {
		return nil, targetNodeName, err
	}

	locationProps, err := locationMgr.GetPropertiesForFirstLocationOfType(damInfrastructureType)

	return locationProps, targetNodeName, err
}

func (r *RefreshTargetTokens) refreshComputeInstanceToken(ctx context.Context, targetNodeName string) error {
	nodeTemplate, err := getStoredNodeTemplate(ctx, r.DeploymentID, targetNodeName)
	if err != nil {
		return err
	}
	locationName, ok := nodeTemplate.Metadata[tosca.MetadataLocationNameKey]
	if !ok {
		return errors.Errorf("Found no location defined in metadata %s for %s", tosca.MetadataLocationNameKey, targetNodeName)
	}
	heappeURL, ok := nodeTemplate.Metadata[metadataCloudCredentialsURLKey]
	if !ok {
		return errors.Errorf("Found no HEAppE URL defined in metadata %s for %s", metadataCloudCredentialsURLKey, targetNodeName)
	}

	// Get a Cloud Service client and token
	cloudClient, cloudToken, err := getCloudClientToken(ctx, r.Cfg, locationName, heappeURL, "yorc", r.DeploymentID)
	if err != nil {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, r.DeploymentID).Registerf(
			"Failed to get token for cloud instance %s, error %s", locationName, err.Error())
		return err
	}

	// Update token for the node
	nodeTemplate.Metadata["token"] = cloudToken
	credsID, credsSecret := cloudClient.GetCreds()
	nodeTemplate.Metadata["token"] = cloudToken
	nodeTemplate.Metadata["application_credential_id"] = credsID
	nodeTemplate.Metadata["application_credential_secret"] = credsSecret

	// Location is now changed for this node template, storing it
	err = storeNodeTemplate(ctx, r.DeploymentID, targetNodeName, nodeTemplate)

	// Update the associated Floating IP Node token
	var floatingIPNodeName string
	for _, nodeReq := range nodeTemplate.Requirements {
		for _, reqAssignment := range nodeReq {
			if reqAssignment.Capability == fipConnectivityCapability {
				floatingIPNodeName = reqAssignment.Node
				break
			}
		}
	}
	if floatingIPNodeName == "" {
		// No associated floating IP pool to change, locations changes are done now
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, r.DeploymentID).Registerf(
			"No floating IP associated to compute instance %s in deployment %s", targetNodeName, r.DeploymentID)
		return err
	}

	fipNodeTemplate, err := getStoredNodeTemplate(ctx, r.DeploymentID, floatingIPNodeName)
	if err != nil {
		return err
	}
	if fipNodeTemplate.Metadata == nil {
		fipNodeTemplate.Metadata = make(map[string]string)
	}
	fipNodeTemplate.Metadata["token"] = cloudToken
	fipNodeTemplate.Metadata["application_credential_id"] = credsID
	fipNodeTemplate.Metadata["application_credential_secret"] = credsSecret

	// Location is now changed for this node template, storing it
	err = storeNodeTemplate(ctx, r.DeploymentID, floatingIPNodeName, fipNodeTemplate)
	if err != nil {
		return err
	}

	return err
}
