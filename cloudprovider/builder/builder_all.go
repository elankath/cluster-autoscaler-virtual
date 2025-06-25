/*
Copyright 2018 The Kubernetes Authors.

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

package builder

import (
	"fmt"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/virtual"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/client-go/informers"
)

// AvailableCloudProviders supported by the cloud provider builder.
var AvailableCloudProviders = []string{
	cloudprovider.VirtualProviderName,
}

// DefaultCloudProvider is Virtual.
const DefaultCloudProvider = cloudprovider.VirtualProviderName

func buildCloudProvider(opts config.AutoscalingOptions,
	do cloudprovider.NodeGroupDiscoveryOptions,
	rl *cloudprovider.ResourceLimiter,
	informerFactory informers.SharedInformerFactory) cloudprovider.CloudProvider {
	switch opts.CloudProviderName {
	case cloudprovider.VirtualProviderName:
		return virtual.BuildVirtual(opts, do, rl)
	default:
		panic(fmt.Sprintf("unsupported cloud provider %s", opts.CloudProviderName))
	}
	return nil
}
