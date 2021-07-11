// Copyright 2020 Bull S.A.S. Atos Technologies - Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France.
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

// Cloud interface

package cloud

import (
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"path"
	"strings"

	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack"
	"github.com/gophercloud/gophercloud/openstack/compute/v2/extensions/keypairs"
	"github.com/gophercloud/gophercloud/pagination"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
)

const (
	// RequestStatusOK is the stauts of a successful request
	RequestStatusOK = "ok"
)

// GetApplicationCredentialsFunc is a type of function provided by the caller to get application credentials
type GetApplicationCredentialsFunc func() (credsID, credsSecret string, err error)

// Client is the client interface to the Cloud infrastructure
type Client interface {
	// Add a key pair to connect to hosts through ssh
	AddKeyPair(keyPairFilePath string) error
	// Get an OpenStack token
	GetToken() string
	// Get Application credentials
	GetCreds() (string, string)
}

// GetClient returns a DDI client for a given location
func GetClient(authURL string, getCredentialsFunc GetApplicationCredentialsFunc) (Client, error) {

	var client cloudClient

	credsID, credsSecret, err := getCredentialsFunc()
	if err != nil {
		return &client, errors.Wrapf(err, "Failed to get Application Credentials")
	}
	opts := gophercloud.AuthOptions{
		IdentityEndpoint:            authURL,
		ApplicationCredentialName:   "yorcCreds",
		ApplicationCredentialID:     credsID,
		ApplicationCredentialSecret: credsSecret,
	}

	client.provider, err = openstack.AuthenticatedClient(opts)
	if err != nil {
		return &client, errors.Wrapf(err, "Failed to get OpenStack client for %s", authURL)
	}

	client.compute, err = openstack.NewComputeV2(client.provider, gophercloud.EndpointOpts{})
	if err != nil {
		err = errors.Wrapf(err, "Failed to get OpenStack compute client provided for %s", authURL)
	}

	client.credsID = credsID
	client.credsSecret = credsSecret
	return &client, err
}

type cloudClient struct {
	provider    *gophercloud.ProviderClient
	compute     *gophercloud.ServiceClient
	credsID     string
	credsSecret string
}

// AddKeyPair adds a key pair used by the orchestrator
func (c *cloudClient) AddKeyPair(keyPairFilePath string) error {

	keyName := path.Base(keyPairFilePath)
	pos := strings.LastIndexByte(keyName, '.')
	if pos != -1 {
		keyName = keyName[:pos]
	}
	keyPairFound := false
	err := keypairs.List(c.compute).EachPage(func(page pagination.Page) (bool, error) {
		kPairs, err := keypairs.ExtractKeyPairs(page)
		if err != nil {
			return false, err
		}
		for _, kPair := range kPairs {
			if kPair.Name == keyName {
				keyPairFound = true
				break
			}
		}
		return !keyPairFound, nil
	})

	if err != nil {
		return errors.Wrapf(err, "Failed to list keypairs")
	}
	if keyPairFound {
		// Nothing to do, the key is already there
		return nil
	}

	// Get the public key value to import it
	pubKeyStr, err := getPublicKey(keyPairFilePath)
	if err != nil {
		return err
	}

	createOpts := keypairs.CreateOpts{
		Name:      keyName,
		PublicKey: pubKeyStr,
	}

	_, err = keypairs.Create(c.compute, createOpts).Extract()
	if err != nil {
		return errors.Wrapf(err, "Failed to import public key %s: %q", keyName, pubKeyStr)
	}
	return err
}

// Get an OpenStack token
func (c *cloudClient) GetToken() string {
	return c.provider.Token()
}

func (c *cloudClient) GetCreds() (string, string) {
	return c.credsID, c.credsSecret
}

func getPublicKey(keyPairFilePath string) (string, error) {

	var pubKeyStr string
	pemKey, err := ioutil.ReadFile(keyPairFilePath)
	if err != nil {
		return pubKeyStr, errors.Wrapf(err, "Failed to read key pair file %s", keyPairFilePath)
	}

	block, _ := pem.Decode([]byte(pemKey))
	if block == nil {
		return pubKeyStr, errors.Wrapf(err, "Failed to read key pair file %s", keyPairFilePath)
	}
	var pubKey ssh.PublicKey
	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err == nil {
		pubKey, err = ssh.NewPublicKey(&privateKey.PublicKey)
		if err != nil {
			return pubKeyStr, errors.Wrapf(err, "Failed to get public key from %s", keyPairFilePath)
		}
	}
	if err != nil {
		// Failed to parse PKCS1 private key, trying to parse EC private key
		privateKey, err := x509.ParseECPrivateKey(block.Bytes)
		if err != nil {
			return pubKeyStr, errors.Wrapf(err, "After attempt to parse PKCS1 private key, failed to parse EC private key %s", keyPairFilePath)
		}
		pubKey, err = ssh.NewPublicKey(&privateKey.PublicKey)
		if err != nil {
			return pubKeyStr, errors.Wrapf(err, "Failed to get public key for %s", keyPairFilePath)
		}
	}

	pubKeyStr = string(ssh.MarshalAuthorizedKey(pubKey))
	return pubKeyStr, nil

}
