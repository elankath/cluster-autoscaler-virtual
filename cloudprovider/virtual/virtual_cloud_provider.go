package virtual

import (
	"cmp"
	"context"
	"fmt"
	gsc "github.com/elankath/gardener-scaling-common"
	"github.com/elankath/gardener-scaling-common/clientutil"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	kubeletapis "k8s.io/kubelet/pkg/apis"
	"k8s.io/utils/ptr"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type VirtualNodeGroup struct {
	gsc.NodeGroupInfo
	nonNamespacedName        string
	nodeTemplate             gsc.NodeTemplate
	instances                map[string]cloudprovider.Instance
	clientSet                *kubernetes.Clientset
	templateNode             *corev1.Node
	templateNodeCreationTime time.Time
}

var _ cloudprovider.CloudProvider = (*VirtualCloudProvider)(nil)
var _ cloudprovider.NodeGroup = (*VirtualNodeGroup)(nil)
var virtualNodeListCache = cache.NewExpiring()
var virtualNodesKey = "virtual-nodes"
var virtualNodesExpiry = 50 * time.Second

const GPULabel = "virtual/gpu"

type VirtualCloudProvider struct {
	launchTime             time.Time
	config                 *gsc.AutoscalerConfig
	configPath             string
	configLastModifiedTime time.Time
	resourceLimiter        *cloudprovider.ResourceLimiter
	clientSet              *kubernetes.Clientset
	virtualNodeGroups      map[string]*VirtualNodeGroup
	refreshCount           int
}

func BuildVirtual(opts config.AutoscalingOptions, do cloudprovider.NodeGroupDiscoveryOptions, rl *cloudprovider.ResourceLimiter) cloudprovider.CloudProvider {

	if opts.CloudProviderName != "virtual" {
		return nil
	}

	kubeConfigPath := opts.KubeClientOpts.KubeConfigPath

	config, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	if err != nil {
		klog.Fatalf("cannot build config from kubeConfigPath: %s, error: %s", kubeConfigPath, err.Error())
	}

	config.Burst = opts.KubeClientOpts.KubeClientBurst
	config.QPS = opts.KubeClientOpts.KubeClientQPS
	//config.QPS = 25
	//config.Burst = 20
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("failed to create the client: %s", err.Error())
	}

	//TODO replace with configmap
	virtualAutoscalerConfigPath := os.Getenv("VIRTUAL_AUTOSCALER_CONFIG")
	if virtualAutoscalerConfigPath == "" {
		virtualAutoscalerConfigPath = "/tmp/vas-config.json"
		klog.Warningf("VIRTUAL_AUTOSCALER_CONFIG not set. Assuming %s", virtualAutoscalerConfigPath)
	}
	cloudProvider, err := InitializeFromVirtualConfig(virtualAutoscalerConfigPath, clientSet, rl)
	if err != nil {
		klog.Fatalf("cannot initialize virtual autoscaler from virtual autoscaler config path: %s", err)
		return nil
	}
	return cloudProvider

}

func AsSyncMap(mp map[string]*VirtualNodeGroup) (sMap sync.Map) {
	for k, v := range mp {
		sMap.Store(k, v)
	}
	return
}

func InitializeFromVirtualConfig(virtualAutoscalerConfigPath string, clientSet *kubernetes.Clientset, rl *cloudprovider.ResourceLimiter) (*VirtualCloudProvider, error) {
	return &VirtualCloudProvider{
		launchTime: time.Now(),
		config: &gsc.AutoscalerConfig{
			NodeTemplates: make(map[string]gsc.NodeTemplate),
			NodeGroups:    make(map[string]gsc.NodeGroupInfo),
		},
		configPath:      virtualAutoscalerConfigPath,
		resourceLimiter: rl,
		clientSet:       clientSet,
	}, nil
}

func buildVirtualNodeGroups(clientSet *kubernetes.Clientset, clusterInfo *gsc.AutoscalerConfig) (map[string]*VirtualNodeGroup, error) {
	virtualNodeGroups := make(map[string]*VirtualNodeGroup)
	for name, ng := range clusterInfo.NodeGroups {
		names := strings.Split(name, ".")
		if len(names) <= 1 {
			return nil, fmt.Errorf("cannot split nodegroup name by '.' seperator for %s", name)
		}
		virtualNodeGroup := VirtualNodeGroup{
			NodeGroupInfo:     ng,
			nonNamespacedName: names[1],
			nodeTemplate:      gsc.NodeTemplate{},
			instances:         make(map[string]cloudprovider.Instance),
			clientSet:         clientSet,
		}
		//populateNodeTemplateTaints(nodeTemplates,mcdData)
		virtualNodeGroups[name] = &virtualNodeGroup
	}
	err := populateNodeTemplates(virtualNodeGroups, clusterInfo.NodeTemplates)
	if err != nil {
		klog.Fatalf("cannot construct the virtual cloud provider: %s", err.Error())
	}
	return virtualNodeGroups, nil
}

func ResourceListFromMap(input map[string]any) (corev1.ResourceList, error) {
	resourceList := corev1.ResourceList{}

	for key, value := range input {
		// Convert the value to a string
		strValue, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("value for key %s is not a string", key)
		}

		// Parse the string value into a Quantity
		quantity, err := resource.ParseQuantity(strValue)
		if err != nil {
			return nil, fmt.Errorf("error parsing quantity for key %s: %v", key, err)
		}
		quantity, err = gsc.NormalizeQuantity(quantity)
		if err != nil {
			return nil, fmt.Errorf("cannot normalize quantity %q: %w", quantity, err)
		}
		// Assign the quantity to the ResourceList
		resourceList[corev1.ResourceName(key)] = quantity
	}

	return resourceList, nil
}

func getVirtualNodeTemplateFromMCC(mcc map[string]any) (nt gsc.NodeTemplate, err error) {
	nodeTemplate := mcc["nodeTemplate"].(map[string]any)
	//providerSpec := mcc["providerSpec"].(map[string]any)
	metadata := mcc["metadata"].(map[string]any)
	capacity, err := ResourceListFromMap(nodeTemplate["capacity"].(map[string]any))
	if err != nil {
		return
	}
	//cpuVal := nodeTemplate["capacity"].(map[string]any)["cpu"].(string)
	//gpuVal := nodeTemplate["capacity"].(map[string]any)["gpu"].(string)
	//memoryVal := nodeTemplate["capacity"].(map[string]any)["memory"].(string)
	instanceType := nodeTemplate["instanceType"].(string)
	region := nodeTemplate["region"].(string)
	zone := nodeTemplate["zone"].(string)
	//tags := providerSpec["tags"].(map[string]any)
	name := metadata["name"].(string)

	//tagsStrMap := make(map[string]string)
	//for tagKey, tagVal := range tags {
	//	tagsStrMap[tagKey] = tagVal.(string)
	//}

	//cpu, err := resource.ParseQuantity(cpuVal)
	//if err != nil {
	//	return
	//}
	//gpu, err := resource.ParseQuantity(gpuVal)
	//if err != nil {
	//	return
	//}
	//memory, err := resource.ParseQuantity(memoryVal)
	//if err != nil {
	//	return
	//}

	nt = gsc.NodeTemplate{
		Name: name,
		//CPU:          cpu,
		//GPU:          gpu,
		//Memory:       memory,
		Capacity:     capacity,
		InstanceType: instanceType,
		Region:       region,
		Zone:         zone,
		//Tags:         tagsStrMap,
	}
	return
}

func getNodeTemplatesFromMCC(mccData map[string]any) (map[string]gsc.NodeTemplate, error) {
	mccList := mccData["items"].([]any)
	var nodeTemplates []gsc.NodeTemplate
	for _, mcc := range mccList {
		nodeTemplate, err := getVirtualNodeTemplateFromMCC(mcc.(map[string]any))
		if err != nil {
			return nil, fmt.Errorf("cannot build nodeTemplate: %w", err)
		}
		nodeTemplates = append(nodeTemplates, nodeTemplate)
	}
	namespace := mccList[0].(map[string]any)["metadata"].(map[string]any)["namespace"].(string)
	return lo.KeyBy(nodeTemplates, func(item gsc.NodeTemplate) string {
		name := item.Name
		idx := strings.LastIndex(name, "-")
		// mcc name - shoot--i585976--suyash-local-worker-1-z1-0af3f , we omit the hash from the mcc name to match it with the nodegroup name
		trimmedName := name[0:idx]
		return fmt.Sprintf("%s.%s", namespace, trimmedName)
	}), nil
}

func getNodeGroupFromMCD(mcd gsc.MachineDeploymentInfo) gsc.NodeGroupInfo {
	name := mcd.Name
	namespace := mcd.Namespace
	return gsc.NodeGroupInfo{
		Name:       fmt.Sprintf("%s.%s", namespace, name),
		PoolName:   mcd.PoolName,
		Zone:       mcd.Zone,
		TargetSize: mcd.Replicas,
		MinSize:    0,
		MaxSize:    0,
	}
}

func mapToNodeGroups(mcds []gsc.MachineDeploymentInfo) map[string]gsc.NodeGroupInfo {
	var nodeGroups []gsc.NodeGroupInfo
	for _, mcd := range mcds {
		nodeGroups = append(nodeGroups, getNodeGroupFromMCD(mcd))
	}
	return lo.KeyBy(nodeGroups, func(item gsc.NodeGroupInfo) string {
		return item.Name
	})
}

func parseCASettingsInfo(caDeploymentData map[string]any) (caSettings gsc.CASettingsInfo, err error) {
	caSettings.NodeGroupsMinMax = make(map[string]gsc.MinMax)
	containersVal, err := gsc.GetInnerMapValue(caDeploymentData, "spec", "template", "spec", "containers")
	if err != nil {
		return
	}
	containers := containersVal.([]any)
	if len(containers) == 0 {
		err = fmt.Errorf("len of containers is zero, no CA container found")
		return
	}
	caContainer := containers[0].(map[string]any)
	caCommands := caContainer["command"].([]any)
	for _, commandVal := range caCommands {
		command := commandVal.(string)
		vals := strings.Split(command, "=")
		if len(vals) <= 1 {
			continue
		}
		key := vals[0]
		val := vals[1]
		switch key {
		case "--max-graceful-termination-sec":
			caSettings.MaxGracefulTerminationSeconds, err = strconv.Atoi(val)
		case "--max-node-provision-time":
			caSettings.MaxNodeProvisionTime, err = time.ParseDuration(val)
		case "--scan-interval":
			caSettings.ScanInterval, err = time.ParseDuration(val)
		case "--max-empty-bulk-delete":
			caSettings.MaxEmptyBulkDelete, err = strconv.Atoi(val)
		case "--new-pod-scale-up-delay":
			caSettings.NewPodScaleUpDelay, err = time.ParseDuration(val)
		case "--nodes":
			var ngMinMax gsc.MinMax
			ngVals := strings.Split(val, ":")
			ngMinMax.Min, err = strconv.Atoi(ngVals[0])
			ngMinMax.Max, err = strconv.Atoi(ngVals[1])
			caSettings.NodeGroupsMinMax[ngVals[2]] = ngMinMax
		}
		if err != nil {
			return
		}
	}
	return
}

func readInitClusterInfo(clusterInfoPath string) (cI gsc.AutoscalerConfig, err error) {
	workerJsonFile := fmt.Sprintf("%s/shoot-worker.json", clusterInfoPath)
	data, err := os.ReadFile(workerJsonFile)
	if err != nil {
		klog.Errorf("cannot read the shoot json file: %s", err.Error())
		return
	}

	var workerDataMap map[string]any
	err = json.Unmarshal(data, &workerDataMap)
	if err != nil {
		klog.Errorf("cannot unmarshal the worker json: %s", err.Error())
		return
	}
	if err != nil {
		klog.Errorf("cannot parse the worker pools: %s", err.Error())
		return
	}

	machineClassesJsonPath := fmt.Sprintf("%s/machine-classes.json", clusterInfoPath)
	data, err = os.ReadFile(machineClassesJsonPath)
	if err != nil {
		klog.Errorf("cannot read the mcc json file: %s", err.Error())
		return
	}
	var mccData map[string]any
	err = json.Unmarshal(data, &mccData)
	if err != nil {
		klog.Errorf("cannot unmarshal the mcc json: %s", err.Error())
		return
	}
	cI.NodeTemplates, err = getNodeTemplatesFromMCC(mccData)
	if err != nil {
		klog.Errorf("cannot build the nodeTemplates: %s", err.Error())
	}

	//mcdJsonFile := fmt.Sprintf("%s/mcds.json", clusterInfoPath)
	//_, err = os.ReadFile(mcdJsonFile)
	//if err != nil {
	//	klog.Errorf("cannot read the mcd json file: %s", err.Error())
	//	return
	//}
	machineDeploymentsJsonPath := fmt.Sprintf("%s/machine-deployments.json", clusterInfoPath)
	machineDeployments, err := readMachineDeploymentInfos(machineDeploymentsJsonPath)
	if err != nil {
		klog.Errorf("readMachineDeploymentInfos error: %s", err.Error())
		return
	}

	populateNodeTemplatesFromMCD(machineDeployments, cI.NodeTemplates)
	cI.NodeGroups = mapToNodeGroups(machineDeployments)

	caDeploymentJsonFile := fmt.Sprintf("%s/ca-deployment.json", clusterInfoPath)
	data, err = os.ReadFile(caDeploymentJsonFile)
	if err != nil {
		klog.Errorf("cannot read the ca-deployment json file: %s", err.Error())
		return
	}
	var caDeploymentData map[string]any
	err = json.Unmarshal(data, &caDeploymentData)
	cI.CASettings, err = parseCASettingsInfo(caDeploymentData)
	if err != nil {
		klog.Errorf("cannot parse the ca settings from deployment json: %s", err.Error())
		return
	}
	err = cI.Init()
	return
}

func populateNodeTemplatesFromMCD(mcds []gsc.MachineDeploymentInfo, nodeTemplates map[string]gsc.NodeTemplate) {
	for _, mcd := range mcds {
		templateName := fmt.Sprintf("%s.%s", mcd.Namespace, mcd.Name)
		nodeTemplate := nodeTemplates[templateName]
		nodeTemplate.Labels = mcd.Labels
		nodeTemplate.Taints = mcd.Taints
		nodeTemplates[templateName] = nodeTemplate
	}
}

func populateNodeTemplates(nodeGroups map[string]*VirtualNodeGroup, nodeTemplates map[string]gsc.NodeTemplate) error {
	for name, template := range nodeTemplates {
		ng, ok := nodeGroups[name]
		if !ok {
			return fmt.Errorf("nodegroup name not found: %s", name)
		}
		ng.nodeTemplate = template
		nodeGroups[name] = ng
	}
	return nil
}

func (vcp *VirtualCloudProvider) Name() string {
	return cloudprovider.VirtualProviderName
}

func (vcp *VirtualCloudProvider) NodeGroups() []cloudprovider.NodeGroup {
	nodeGroups := make([]cloudprovider.NodeGroup, len(vcp.virtualNodeGroups))
	count := 0
	for _, v := range vcp.virtualNodeGroups {
		nodeGroups[count] = v
		count++
	}
	return nodeGroups
}

type poolKey struct {
	poolName string
	zone     string
}

func (vcp *VirtualCloudProvider) getNodeGroupsByPoolKey() map[poolKey]*VirtualNodeGroup {
	poolKeyMap := make(map[poolKey]*VirtualNodeGroup)
	for _, vng := range vcp.virtualNodeGroups {
		pk := poolKey{
			poolName: vng.PoolName,
			zone:     vng.Zone,
		}
		poolKeyMap[pk] = vng
	}
	return poolKeyMap
}

func getZonefromNodeLabels(nodeLabels map[string]string) (zone string, err error) {
	zone, ok := gsc.GetZone(nodeLabels)
	if ok {
		return
	}
	err = fmt.Errorf("no eligible zone label available for node %q", nodeLabels["kubernetes.io/hostname"])
	return
}
func (vcp *VirtualCloudProvider) NodeGroupForNode(node *corev1.Node) (cloudprovider.NodeGroup, error) {
	if len(vcp.config.NodeGroups) == 0 {
		klog.Warning("virtual autoscaler has not been initialized with nodes")
		return nil, nil
	}
	ctx := context.Background()
	poolKeyMap := vcp.getNodeGroupsByPoolKey()
	nodeInCluster, err := vcp.clientSet.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("cannot determine NodeGroup for node with name %q: %w", node.Name, err)
	}
	nodeLabels := make(map[string]string)
	if node.Labels != nil {
		maps.Copy(nodeLabels, nodeLabels)
	}
	if nodeInCluster.Labels != nil {
		maps.Copy(nodeLabels, nodeInCluster.Labels)
	}
	zone, err := getZonefromNodeLabels(nodeLabels)
	if err != nil {
		return nil, fmt.Errorf("cant find VirtualNodeGroup for node with with name %q: %w", node.Name, err)
	}
	nodePoolKey := poolKey{
		poolName: nodeLabels["worker.gardener.cloud/pool"],
		zone:     zone,
	}
	nodeGroup, ok := poolKeyMap[nodePoolKey]
	if !ok {
		return nil, fmt.Errorf("cant find VirtualNodeGroup for node with with name %q", node.Name)
	}
	return nodeGroup, nil
}

func (vcp *VirtualCloudProvider) HasInstance(node *corev1.Node) (bool, error) {
	return true, cloudprovider.ErrNotImplemented
}

func (vcp *VirtualCloudProvider) Pricing() (cloudprovider.PricingModel, errors.AutoscalerError) {
	return nil, cloudprovider.ErrNotImplemented
}

func (vcp *VirtualCloudProvider) GetAvailableMachineTypes() ([]string, error) {
	return []string{}, nil
}

func (vcp *VirtualCloudProvider) NewNodeGroup(machineType string, labels map[string]string, systemLabels map[string]string, taints []corev1.Taint, extraResources map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func (vcp *VirtualCloudProvider) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	return vcp.resourceLimiter, nil
}

func (vcp *VirtualCloudProvider) GPULabel() string {
	return GPULabel
}

func (vcp *VirtualCloudProvider) GetAvailableGPUTypes() map[string]struct{} {
	return nil
}

func (vcp *VirtualCloudProvider) GetNodeGpuConfig(node *corev1.Node) *cloudprovider.GpuConfig {
	return nil
}

func (vcp *VirtualCloudProvider) Cleanup() error {
	return nil
}

func checkAndGetFileLastModifiedTime(filePath string) (lastModifiedTime time.Time, err error) {
	file, err := os.Stat(filePath)
	if err != nil {
		return
	}
	lastModifiedTime = file.ModTime()
	return
}

func loadAutoscalerConfig(filePath string) (config gsc.AutoscalerConfig, err error) {
	bytes, err := os.ReadFile(filePath)
	if err != nil {
		return
	}

	err = json.Unmarshal(bytes, &config)
	if err != nil {
		return
	}

	return
}

func (vcp *VirtualCloudProvider) reloadVirtualNodeGroups() error {
	virtualNodeGroups, err := buildVirtualNodeGroups(vcp.clientSet, vcp.config)
	if err != nil {
		return err
	}
	vcp.virtualNodeGroups = virtualNodeGroups
	return nil
}

func adjustNode(clientSet *kubernetes.Clientset, nodeName string, nodeStatus corev1.NodeStatus) error {

	nd, err := clientSet.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("adjustNode cannot get node with name %q: %w", nd.Name, err)
	}
	nd.Spec.Taints = lo.Filter(nd.Spec.Taints, func(item corev1.Taint, index int) bool {
		return item.Key != "node.kubernetes.io/not-ready"
	})
	nd, err = clientSet.CoreV1().Nodes().Update(context.Background(), nd, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("adjustNode cannot update node with name %q: %w", nd.Name, err)
	}
	nd.Status = nodeStatus
	nd.Status.Phase = corev1.NodeRunning
	nd, err = clientSet.CoreV1().Nodes().UpdateStatus(context.Background(), nd, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("adjustNode cannot update the status of node with name %q: %w", nd.Name, err)
	}
	return nil
}

func (vcp *VirtualCloudProvider) refreshNodes() error {
	configNodeInfos := vcp.config.ExistingNodes
	ctx := context.Background()
	err := vcp.synchronizeNodes(ctx, configNodeInfos)
	if err != nil {
		return err
	}
	return nil
}

func (vcp *VirtualCloudProvider) synchronizeNodes(ctx context.Context, configNodeInfos []gsc.NodeInfo) error {
	configNodeInfosByName := lo.Associate(configNodeInfos, func(item gsc.NodeInfo) (string, struct{}) {
		return item.Name, struct{}{}
	})
	virtualNodes, err := clientutil.ListAllNodes(ctx, vcp.clientSet)
	if err != nil {
		return fmt.Errorf("cannot list the nodes in virtual cluster: %w", err)
	}
	virtualNodesByName := lo.KeyBy(virtualNodes, func(item corev1.Node) string {
		return item.Name
	})

	var deletedNodeNames []string

	deletedNodeNames, err = deleteVirtuallyScaledNodes(context.Background(), vcp.clientSet, virtualNodes)
	if err != nil {
		return err
	}
	for _, vn := range virtualNodes {
		_, ok := configNodeInfosByName[vn.Name]
		if ok {
			continue
		}
		err := vcp.clientSet.CoreV1().Nodes().Delete(ctx, vn.Name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("cannot delete the virtual node %q: %w", vn.Name, err)
		}
		//delete(virtualNodesByName, vn.Name)
		deletedNodeNames = append(deletedNodeNames, vn.Name)
		klog.V(2).Infof("%d | synchronizeNodes deleted the virtual node %q", vcp.refreshCount, vn.Name)
	}

	err = clearNodeNameFromPods(ctx, vcp.clientSet, deletedNodeNames...)
	if err != nil {
		return err
	}

	for _, vng := range vcp.virtualNodeGroups { // delete corresponding instances
		for _, nn := range deletedNodeNames {
			delete(vng.instances, nn)
		}
	}

	var nodeCreateOrUpdateFuncs []func() error

	for _, nodeInfo := range configNodeInfos {
		f := func() error {
			oldVNode, exists := virtualNodesByName[nodeInfo.Name]
			var sameLabels, sameTaints bool
			if exists {
				sameLabels = maps.Equal(oldVNode.Labels, nodeInfo.Labels)
				sameTaints = slices.EqualFunc(oldVNode.Spec.Taints, nodeInfo.Taints, gsc.IsEqualTaint)
			}
			if exists && sameLabels && sameTaints {
				return nil
			}
			node := corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:      nodeInfo.Name,
					Namespace: nodeInfo.Namespace,
					Labels:    nodeInfo.Labels,
				},
				Spec: corev1.NodeSpec{
					Taints: nodeInfo.Taints,
					//ProviderID: nodeInfo.ProviderID, //<-- this unfortunately causes node to be unregistered
					ProviderID: nodeInfo.Name,
				},
				Status: corev1.NodeStatus{
					Capacity:    nodeInfo.Capacity,
					Allocatable: nodeInfo.Allocatable,
				},
			}
			if node.Annotations == nil {
				node.Annotations = make(map[string]string)
			}
			node.Annotations["cluster-autoscaler.kubernetes.io/scale-down-disabled"] = "true"
			klog.V(4).Infof("%d | for node %q, initial node.Status.Allocatable (before ksr subtraction) is %q", vcp.refreshCount, node.Name, gsc.ResourcesAsString(node.Status.Allocatable))
			//node.Status.Allocatable = resutil.ComputeRevisedResources(node.Status.Allocatable, kubeSystemResources)
			klog.V(4).Infof("%d | for node %q, revised node.Status.Allocatable (after ksr subtraction) is %q", vcp.refreshCount, node.Name, gsc.ResourcesAsString(node.Status.Allocatable))
			nodeStatus := node.Status
			if !exists {
				_, err = vcp.clientSet.CoreV1().Nodes().Create(context.Background(), &node, metav1.CreateOptions{})
				if apierrors.IsAlreadyExists(err) {
					klog.Warningf("synchronizeNodes: node already exists. updating node %q, resources %q", node.Name, gsc.ResourcesAsString(node.Status.Allocatable))
					_, err = vcp.clientSet.CoreV1().Nodes().Update(context.Background(), &node, metav1.UpdateOptions{})
				}
				if err == nil {
					klog.V(2).Infof("%d | synchronizeNodes created node %q, resources %q", vcp.refreshCount, node.Name, gsc.ResourcesAsString(node.Status.Allocatable))
				}
			} else {
				_, err = vcp.clientSet.CoreV1().Nodes().Update(context.Background(), &node, metav1.UpdateOptions{})
				klog.V(2).Infof("%d | synchronizeNodes updated node %q, resources %q", vcp.refreshCount, node.Name, gsc.ResourcesAsString(node.Status.Allocatable))
			}
			if err != nil {
				return fmt.Errorf("synchronizeNodes cannot create/update node with name %q: %w", node.Name, err)
			}
			node.Status = nodeStatus
			node.Status.Conditions = cloudprovider.BuildReadyConditions()
			err = adjustNode(vcp.clientSet, node.Name, node.Status)
			if err != nil {
				return fmt.Errorf("synchronizeNodes cannot adjust the node with name %q: %w", node.Name, err)
			}
			//var nodeInCluster *corev1.Node
			//nodeInCluster, err = vcp.clientSet.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
			//if err != nil {
			//	klog.Errorf("synchronizeNodes cannot get the node named  name %q: %w", node.Name, err)
			//	return err
			//}
			//node.Spec.Taints = slices.DeleteFunc(node.Spec.Taints, func(taint corev1.Taint) bool {
			//	return taint.Key == "node.kubernetes.io/not-ready"
			//})
			//_, err = vcp.clientSet.CoreV1().Nodes().Update(ctx, nodeInCluster, metav1.UpdateOptions{})
			//if err != nil {
			//	klog.Errorf("synchronizeNodes cannot update the node named %q: %w", node.Name, err)
			//	return err
			//}
			return nil
		}
		nodeCreateOrUpdateFuncs = append(nodeCreateOrUpdateFuncs, f)
	}

	fnChunks := lo.Chunk(nodeCreateOrUpdateFuncs, 18)
	for _, chunk := range fnChunks {
		var g errgroup.Group
		for _, fn := range chunk {
			g.Go(fn)
		}
		if err := g.Wait(); err != nil {
			return err
		}
	}

	// Update VirtualNodeGroup.Instances
	virtualNodes, err = clientutil.ListAllNodes(ctx, vcp.clientSet)
	if err != nil {
		return fmt.Errorf("cannot list the nodes in virtual cluster: %w", err)
	}
	virtualNodeListCache.Set(virtualNodesKey, virtualNodes, virtualNodesExpiry)

	for _, node := range virtualNodes {
		ng, err := vcp.NodeGroupForNode(&node)
		if err != nil {
			return fmt.Errorf("synchronizeNodes can't find NodeGroup for node %q: %w", node.Name, err)
		}
		vng, ok := vcp.virtualNodeGroups[ng.Id()]
		if !ok {
			return fmt.Errorf("synchronizeNodes can't find VirtualNodeGroup with name: %w", ng.Id())
		}
		vng.instances[node.Name] = cloudprovider.Instance{
			Id: node.Name,
			Status: &cloudprovider.InstanceStatus{
				State:     cloudprovider.InstanceCreating,
				ErrorInfo: nil,
			},
		}
		klog.V(4).Infof("%d | synchronizeNodes added instance %q to VirtualNodeGroup %q", vcp.refreshCount, node.Name, vng.Name)
	}

	//time.AfterFunc(1*time.Second, func() { vcp.changeCreatingInstancesToRunning(ctx) })
	vcp.changeCreatingInstancesToRunning(ctx)
	return nil
}

func (vcp *VirtualCloudProvider) changeCreatingInstancesToRunning(ctx context.Context) {
	for _, v := range vcp.virtualNodeGroups {
		v.changeCreatingInstancesToRunning(ctx, strconv.Itoa(vcp.refreshCount))
	}
}

func (v *VirtualNodeGroup) changeCreatingInstancesToRunning(ctx context.Context, logPrefix string) {
	for nn, _ := range v.instances {
		if v.instances[nn].Status.State == cloudprovider.InstanceRunning {
			continue
		}
		v.instances[nn].Status.State = cloudprovider.InstanceRunning
	}
}

func (vcp *VirtualCloudProvider) Refresh() error {
	klog.V(2).Info("VirtualCloudProvider.Refresh invoked")
	lastModifiedTime, err := checkAndGetFileLastModifiedTime(vcp.configPath)
	if err != nil {
		return fmt.Errorf("cannot look up virtual autoscaler autoscalerConfig at path: %s, error: %s", vcp.configPath, err)
	}
	//if vcp.launchTime.After(lastModifiedTime) {
	//	klog.Warningf("Ignoring Old virtual autoscalerConfig  %q with time %q created before the CA launch time %q", vcp.configPath, lastModifiedTime, vcp.launchTime)
	//	return nil
	//}
	//if vcp.configLastModifiedTime.Equal(lastModifiedTime) {
	//	klog.V(2).Infof("Skipping load of virtual autoscalerConfig %q since lastModifiedTime %q is unchanged", vcp.configPath, lastModifiedTime)
	//	return nil
	//}
	klog.V(2).Infof("Triggering reload of virtual autoscalerConfig %q since lastModifiedTime %q is different from configLastModifiedTime %q", vcp.configPath, lastModifiedTime, vcp.configLastModifiedTime)
	autoscalerConfig, err := loadAutoscalerConfig(vcp.configPath)
	if err != nil {
		return fmt.Errorf("cannot load virtual autoscaling config from %q: %w", vcp.configPath, err)
	}
	oldLastModifiedTime := vcp.configLastModifiedTime
	vcp.config = &autoscalerConfig
	vcp.configLastModifiedTime = lastModifiedTime
	vcp.refreshCount++

	var msg string
	err = doRefresh(vcp, oldLastModifiedTime)
	if err != nil {
		msg = err.Error()
		//writeSignal(vcp.config.ErrorSignalPath, msg)
		return err
	}
	msg = fmt.Sprintf("succesfully refreshed. instanceCount=%d", vcp.getInstanceCount())
	writeSignal(vcp.config.SuccessSignalPath, msg)
	if vcp.config.Mode == gsc.AutoscalerReplayerPauseMode {
		klog.V(2).Infof("virtual autoscaler is in pause mode. instanceCount=%d", vcp.getInstanceCount())
		return fmt.Errorf("virtual autoscaler is in pause mode")
	}
	klog.V(2).Infof("virtual autoscaler is resuming")
	return nil
}

func (vcp *VirtualCloudProvider) getInstanceCount() (instanceCount int) {
	for _, vng := range vcp.virtualNodeGroups {
		instanceCount += len(vng.instances)
	}
	return
}

func writeSignal(signalPath string, msg string) {
	//err := os.WriteFile(signalPath, []byte(msg), 0644)
	//if err != nil {
	//	klog.Warningf("cannot write message %q to signalPath %q due to error: %s", msg, signalPath, err)
	//	return
	//}
	//klog.Infof("wrote message %q to signalPath %q", msg, signalPath)
}
func doRefresh(vcp *VirtualCloudProvider, oldLastModifiedTime time.Time) error {
	if vcp.configLastModifiedTime.Equal(oldLastModifiedTime) {
		klog.V(2).Infof("Skipping refresh of virtual autoscalerConfig %q since lastModifiedTime %q is unchanged", vcp.configPath, oldLastModifiedTime)
		return nil
	}
	if len(vcp.config.NodeGroups) == 0 {
		return fmt.Errorf("virtual autoscaler is not initialized")
	}
	err := vcp.reloadVirtualNodeGroups()
	if err != nil {
		return err
	}
	if len(vcp.config.NodeGroups) == 0 {
		return nil
	}
	err = vcp.refreshNodes()
	if err != nil {
		return err
	}
	klog.V(2).Infof("completed config reload of virtual cloud provider from path: %s", vcp.configPath)
	return nil

}

func deleteVirtuallyScaledNodes(ctx context.Context, clientSet *kubernetes.Clientset, nodes []corev1.Node) (deletedNodeNames []string, err error) {
	if nodes == nil {
		nodes, err = clientutil.ListAllNodes(ctx, clientSet)
	}
	if err != nil {
		err = fmt.Errorf("deleteVirtuallyScaledNodes cannot list all nodes: %w", err)
		return
	}
	for _, node := range nodes {
		_, ok := node.Labels[gsc.LabelVirtualScaled]
		if ok {
			err = clientSet.CoreV1().Nodes().Delete(ctx, node.Name, metav1.DeleteOptions{})
			if err != nil {
				err = fmt.Errorf("deleteVirtuallyScaledNodes cannot delete node %q: %w", node.Name, err)
				return
			}
			klog.V(2).Info("deleted virtual-scaled node %q", node.Name)
			deletedNodeNames = append(deletedNodeNames, node.Name)
		}
	}
	return
}

var _ cloudprovider.CloudProvider = (*VirtualCloudProvider)(nil)

func (v *VirtualNodeGroup) MaxSize() int {
	return v.NodeGroupInfo.MaxSize
}

func (v *VirtualNodeGroup) MinSize() int {
	return v.NodeGroupInfo.MinSize
}

func (v *VirtualNodeGroup) TargetSize() (int, error) {
	klog.V(3).Infof("TargetSize() of %q is currently %d", v.Name, len(v.instances))
	return len(v.instances), nil
}

func (v *VirtualNodeGroup) IncreaseSize(delta int) error {
	ctx := context.Background()
	//TODO add flags for simulating provider errors ex : ResourceExhaustion
	if len(v.instances) >= v.MaxSize() {
		klog.Warningf("IncreaseSize SPURIOUSLY called by CA core for %q with delta %d", v.Name, delta)
		return nil
	}
	// double check against Nodes of virtual cluster.
	klog.V(2).Infof("IncreaseSize called for %q with delta %d", v.Name, delta)
	if v.templateNode == nil || time.Now().Sub(v.templateNodeCreationTime) > time.Second*5 { //minor memoization
		templateNode, err := v.buildCoreNodeFromTemplate()
		if err != nil {
			return err
		}
		v.templateNode = &templateNode
		v.templateNodeCreationTime = time.Now()
	}
	for i := 0; i < delta; i++ {
		newNode := *v.templateNode
		newNode.Name = fmt.Sprintf("%s-%d", newNode.Name, i)
		newNode.Labels["kubernetes.io/hostname"] = newNode.Name
		newNode.Spec.ProviderID = newNode.Name
		v.instances[newNode.Name] = cloudprovider.Instance{
			Id: newNode.Name,
			Status: &cloudprovider.InstanceStatus{
				State:     cloudprovider.InstanceCreating,
				ErrorInfo: nil,
			},
		}
		nodeStatus := newNode.Status
		createdNode, err := v.clientSet.CoreV1().Nodes().Create(ctx, &newNode, metav1.CreateOptions{})
		if err != nil {
			return err
		}
		err = adjustNode(v.clientSet, newNode.Name, nodeStatus)
		if err != nil {
			return err
		}
		klog.Infof("IncreaseSize created a new node with name: %s", createdNode.Name)
	}
	time.AfterFunc(1*time.Second, func() { v.changeCreatingInstancesToRunning(ctx, fmt.Sprintf("NG.IncreaseSize")) })
	return nil
}

func (v *VirtualNodeGroup) AtomicIncreaseSize(delta int) error {
	return cloudprovider.ErrNotImplemented
}

func (v *VirtualNodeGroup) DeleteNodes(nodes []*corev1.Node) error {
	nodeNames := lo.Map(nodes, func(n *corev1.Node, index int) string {
		return n.Name
	})
	ctx := context.Background()
	for _, node := range nodes {
		klog.V(2).Infof("NG.DeleteNodes is deleting node with name %q", node.Name)
		err := v.clientSet.CoreV1().Nodes().Delete(ctx, node.Name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		nodeNames = append(nodeNames, node.Name)
		delete(v.instances, node.Name)
	}

	err := clearNodeNameFromPods(ctx, v.clientSet, nodeNames...)
	if err != nil {
		return err
	}
	return nil
}

func (v *VirtualNodeGroup) ForceDeleteNodes(nodes []*corev1.Node) error {
	return v.DeleteNodes(nodes)
}

func (v *VirtualNodeGroup) DecreaseTargetSize(delta int) error {
	klog.V(2).Infof("NG.DecreaseTargetSize called for ng %q and delta %d", v.Name, delta)
	ctx := context.Background()
	nodes, err := v.clientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	if delta > len(nodes.Items) {
		return fmt.Errorf("nodes to be deleted are greater than current number of nodes")
	}
	pods, err := v.clientSet.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	podsToNodesMap := lo.GroupBy(pods.Items, func(pod corev1.Pod) string {
		return pod.Spec.NodeName
	})
	var deleteNodes []*corev1.Node
	slices.SortFunc(nodes.Items, func(a, b corev1.Node) int {
		return cmp.Compare(len(podsToNodesMap[a.Name]), len(podsToNodesMap[b.Name]))
	})
	for i := 0; i < delta; i++ {
		deleteNodes = append(deleteNodes, &nodes.Items[i])
	}
	return v.DeleteNodes(deleteNodes)
}

func clearNodeNameFromPods(ctx context.Context, clientset *kubernetes.Clientset, deletedNodeNames ...string) error {
	pods, err := clientutil.ListAllPods(ctx, clientset)
	if err != nil {
		return err
	}

	deletedNodeNamesSet := sets.NewString(deletedNodeNames...)

	for _, pod := range pods {
		if deletedNodeNamesSet.Has(pod.Spec.NodeName) {
			err = clientset.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
				GracePeriodSeconds: ptr.To[int64](0),
			})
			if err != nil {
				return err
			}
			newPod := corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        pod.Name,
					Namespace:   pod.Namespace,
					Labels:      pod.Labels,
					Annotations: pod.Annotations,
				},
				Spec: pod.Spec,
			}
			newPod.Spec.NodeName = ""
			_, err = clientset.CoreV1().Pods(pod.Namespace).Create(ctx, &newPod, metav1.CreateOptions{})
			if err != nil {
				return err
			}
			klog.V(2).Infof("Cleared node name %q from pod %q", pod.Spec.NodeName, pod.Name)
		}
	}
	return nil
}

func (v *VirtualNodeGroup) Id() string {
	return v.Name
}

func (v *VirtualNodeGroup) Debug() string {
	return fmt.Sprintf("%s (%d:%d)", v.Id(), v.MinSize(), v.MaxSize())
}

func (v *VirtualNodeGroup) Nodes() ([]cloudprovider.Instance, error) {
	return lo.Values(v.instances), nil
}

func buildGenericLabels(template *gsc.NodeTemplate, nodeName string) map[string]string {
	result := make(map[string]string)
	// TODO: extract from MCM
	result[kubeletapis.LabelArch] = cloudprovider.DefaultArch
	result[corev1.LabelArchStable] = cloudprovider.DefaultArch

	result[kubeletapis.LabelOS] = cloudprovider.DefaultOS
	result[corev1.LabelOSStable] = cloudprovider.DefaultOS

	result[corev1.LabelInstanceType] = template.InstanceType
	result[corev1.LabelInstanceTypeStable] = template.InstanceType

	result[corev1.LabelZoneRegion] = template.Region
	result[corev1.LabelZoneRegionStable] = template.Region

	result[corev1.LabelZoneFailureDomain] = template.Zone
	result[corev1.LabelZoneFailureDomainStable] = template.Zone

	//TODO fix node name label to satisfy validation
	result[corev1.LabelHostname] = nodeName
	return result
}

func computeRevisedResourcesForNodeTemplate(nodeTemplate *gsc.NodeTemplate, sysComponentMaxResourceList corev1.ResourceList) {
	kubeReservedCPU := resource.MustParse("80m")
	kubeReservedMemory := resource.MustParse("1Gi")
	kubeReservedResources := corev1.ResourceList{corev1.ResourceCPU: kubeReservedCPU, corev1.ResourceMemory: kubeReservedMemory}
	nodeTemplate.Allocatable = computeRevisedAllocatable(nodeTemplate.Capacity, sysComponentMaxResourceList, kubeReservedResources)
	revisedPods := nodeTemplate.Allocatable.Pods()
	revisedPods.Set(110)
	nodeTemplate.Allocatable[corev1.ResourcePods] = *revisedPods
	nodeTemplate.Capacity[corev1.ResourcePods] = *revisedPods
}

func computeRevisedAllocatable(originalAllocatable corev1.ResourceList, systemComponentsResources corev1.ResourceList, kubeReservedResources corev1.ResourceList) corev1.ResourceList {
	revisedNodeAllocatable := originalAllocatable.DeepCopy()
	revisedMem := revisedNodeAllocatable.Memory()
	revisedMem.Sub(systemComponentsResources[corev1.ResourceMemory])

	revisedCPU := revisedNodeAllocatable.Cpu()
	revisedCPU.Sub(systemComponentsResources[corev1.ResourceCPU])

	if kubeReservedResources != nil {
		revisedMem.Sub(kubeReservedResources[corev1.ResourceMemory])
		revisedCPU.Sub(kubeReservedResources[corev1.ResourceCPU])
	}
	revisedNodeAllocatable[corev1.ResourceMemory] = *revisedMem
	revisedNodeAllocatable[corev1.ResourceCPU] = *revisedCPU
	return revisedNodeAllocatable
}

func (v *VirtualNodeGroup) buildCoreNodeFromTemplate() (corev1.Node, error) {
	//kubeSystemResources, err := GetKubeSystemPodsRequests(v.clientSet)
	//if err != nil {
	//	return corev1.Node{}, err
	//}

	node := corev1.Node{}
	nodeName := fmt.Sprintf("%s-%s", v.nonNamespacedName, rand.String(5))

	node.ObjectMeta = metav1.ObjectMeta{
		Name:     nodeName,
		SelfLink: fmt.Sprintf("/api/v1/nodes/%s", nodeName),
		Labels:   map[string]string{},
	}

	revisedNodeTemplate := v.nodeTemplate
	//computeRevisedResourcesForNodeTemplate(&revisedNodeTemplate, kubeSystemResources)
	node.Status = corev1.NodeStatus{
		Capacity: maps.Clone(v.nodeTemplate.Capacity),
	}
	node.Status.Capacity[corev1.ResourcePods] = resource.MustParse("110") //Fixme must take it dynamically from node object
	node.Status.Capacity[gpu.ResourceNvidiaGPU] = v.nodeTemplate.Capacity["gpu"]
	delete(node.Status.Capacity, "gpu")
	node.Status.Capacity["hugepages-1Gi"] = *resource.NewQuantity(0, resource.DecimalSI)
	node.Status.Capacity["hugepages-2Mi"] = *resource.NewQuantity(0, resource.DecimalSI)

	// get an existing node from this node group if one is available.
	//ctx := context.Background()
	//existingNode, err := getExistingNodeForNodeGroup(ctx, v.clientSet, v.PoolName)
	//if err != nil {
	//	return node, err
	//}
	//if existingNode != nil {
	//	klog.V(3).Infof("buildCoreNodeFromTemplate| For %q, found existing node %q, using its node.Status.Allocatable %q", node.Name, existingNode.Name, gsc.ResourcesAsString(existingNode.Status.Allocatable))
	//	node.Status.Allocatable = existingNode.Status.Allocatable
	//} else {
	node.Status.Allocatable = revisedNodeTemplate.Allocatable
	klog.V(3).Infof("buildCoreNodeFromTemplate| For %q, set node.Status.Allocatable from revisedNodeTemplate to %q", node.Name, gsc.ResourcesAsString(node.Status.Allocatable))
	//}

	node.Labels = cloudprovider.JoinStringMaps(node.Labels, buildGenericLabels(&v.nodeTemplate, nodeName))
	maps.Copy(node.Labels, v.nodeTemplate.Labels)
	delete(node.Labels, "kubernetes.io/role/node")
	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}
	node.Annotations["cluster-autoscaler.kubernetes.io/scale-down-disabled"] = "true"
	//delete(node.Labels, "kubernetes.io/cluster/shoot--i544000--record")
	for key, _ := range node.Labels {
		if strings.HasPrefix(key, "kubernetes.io/cluster") {
			delete(node.Labels, key)
		}
	}
	node.Labels[gsc.LabelVirtualScaled] = "true"
	//node.Labels[NodeGroupLabel] = v.nonNamespacedName

	//TODO populate taints from mcd
	node.Spec.Taints = v.nodeTemplate.Taints

	node.Status.Conditions = cloudprovider.BuildReadyConditions()
	return node, nil
}

func getVirtualClusterNodes(ctx context.Context, clientSet *kubernetes.Clientset) (nodes []corev1.Node, err error) {
	val, ok := virtualNodeListCache.Get(virtualNodesKey)
	if ok {
		nodes = val.([]corev1.Node)
	} else {
		nodes, err = clientutil.ListAllNodes(ctx, clientSet)
		virtualNodeListCache.Set(virtualNodesKey, nodes, virtualNodesExpiry)
	}
	return
}

func getExistingNodeForNodeGroup(ctx context.Context, clientSet *kubernetes.Clientset, poolName string) (matchingNode *corev1.Node, err error) {
	nodes, err := getVirtualClusterNodes(ctx, clientSet)
	if err != nil {
		return nil, err
	}
	for _, n := range nodes {
		nodeLabels := n.Labels
		np, ok := gsc.GetPoolName(nodeLabels)
		if !ok {
			continue
		}
		if poolName == np {
			klog.V(3).Infof("getExistingNodeForNodeGroup found existing node %q for poolName %q ", n.Name, poolName)
			matchingNode = &n
			return
		}
	}
	return
}

func GetKubeSystemPodsRequests(clientset *kubernetes.Clientset) (corev1.ResourceList, error) {
	podList, err := clientset.CoreV1().Pods("kube-system").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	podsByNode := lo.GroupBy(podList.Items, func(pod corev1.Pod) string {
		return pod.Spec.NodeName
	})

	nodeWithMostKubeSystemPods := ""
	numPods := 0
	for nodeName, nodePods := range podsByNode {
		if len(nodePods) > numPods {
			nodeWithMostKubeSystemPods = nodeName
			numPods = len(nodePods)
		}
	}

	nodeResource := sumResourceRequests(podsByNode[nodeWithMostKubeSystemPods])

	return nodeResource, nil
}

func sumResourceRequests(pods []corev1.Pod) corev1.ResourceList {
	var totalMemory resource.Quantity
	var totalCPU resource.Quantity
	var storage resource.Quantity
	var ephemeralStorage resource.Quantity
	for _, pod := range pods {
		for _, container := range pod.Spec.Containers {
			totalMemory.Add(NilOr(container.Resources.Requests.Memory(), resource.Quantity{}))
			totalCPU.Add(NilOr(container.Resources.Requests.Cpu(), resource.Quantity{}))
			storage.Add(NilOr(container.Resources.Requests.Storage(), resource.Quantity{}))
			ephemeralStorage.Add(NilOr(container.Resources.Requests.Storage(), resource.Quantity{}))
		}
	}
	return corev1.ResourceList{
		corev1.ResourceMemory:           totalMemory,
		corev1.ResourceCPU:              totalCPU,
		corev1.ResourceStorage:          storage,
		corev1.ResourceEphemeralStorage: ephemeralStorage,
	}
}

func NilOr[T any](val *T, defaultVal T) T {
	if val == nil {
		return defaultVal
	}
	return *val
}

func (v *VirtualNodeGroup) TemplateNodeInfo() (*framework.NodeInfo, error) {
	coreNode, err := v.buildCoreNodeFromTemplate()
	if err != nil {
		return nil, err
	}
	nodeInfo := framework.NewNodeInfo(&coreNode, nil, &framework.PodInfo{
		Pod: cloudprovider.BuildKubeProxy(v.Name),
	})
	return nodeInfo, nil
}

func (v *VirtualNodeGroup) Exist() bool {
	return true
}

func (v *VirtualNodeGroup) Create() (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrAlreadyExist
}

func (v *VirtualNodeGroup) Delete() error {
	return cloudprovider.ErrNotImplemented
}

func (v *VirtualNodeGroup) Autoprovisioned() bool {
	return false
}

func (v *VirtualNodeGroup) GetOptions(defaults config.NodeGroupAutoscalingOptions) (*config.NodeGroupAutoscalingOptions, error) {
	//TODO copy from mcm get options
	return &defaults, nil
}

func readMachineDeploymentInfos(mcdsJsonFile string) ([]gsc.MachineDeploymentInfo, error) {
	bytes, err := os.ReadFile(mcdsJsonFile)
	if err != nil {
		return nil, err
	}
	var mcdData unstructured.Unstructured
	err = json.Unmarshal(bytes, &mcdData)
	if err != nil {
		klog.Errorf("cannot unmarshal the mcd json: %s", err.Error())
		return nil, err
	}
	items := mcdData.UnstructuredContent()["items"].([]any)
	mcdInfos := make([]gsc.MachineDeploymentInfo, len(items))
	for i, item := range items {
		itemMap := item.(map[string]any)
		itemObj := unstructured.Unstructured{Object: itemMap}
		//medataDataMap, found, err := itemObjunstructured.NestedMap(itemMap, "metadata")
		name := itemObj.GetName()
		namespace := itemObj.GetNamespace()
		specMap, found, err := unstructured.NestedMap(itemObj.UnstructuredContent(), "spec")
		if !found {
			return nil, fmt.Errorf("cannot find 'spec' inside machine deployment json with idx %d", i)
		}
		if err != nil {
			return nil, fmt.Errorf("error loading spec map inside machine deployment json with idx %d: %w", i, err)
		}
		var replicas int
		replicasVal, ok := specMap["replicas"]
		if ok {
			replicas = int(replicasVal.(int64))
		}
		nodeTemplate, found, err := unstructured.NestedMap(specMap, "template", "spec", "nodeTemplate")
		if !found {
			return nil, fmt.Errorf("cannot find nested nodeTemplate map inside machine deployment json with idx %d", i)
		}
		if err != nil {
			return nil, fmt.Errorf("error loading nested nodeTemplate map inside machine deployment json with idx %d: %w", i, err)
		}
		labels, found, err := unstructured.NestedStringMap(nodeTemplate, "metadata", "labels")
		if !found {
			return nil, fmt.Errorf("cannot find nested labels inside nodeTemplate belonging to machine deployment json with idx %d", i)
		}
		if err != nil {
			return nil, fmt.Errorf("error loading nested labels inside nodeTemplate belonging to machine deployment json with idx %d: %w", i, err)
		}
		labelsVal, found, err := unstructured.NestedMap(nodeTemplate, "metadata", "labels")
		if !found {
			return nil, fmt.Errorf("cannot find nested labels inside nodeTemplate belonging to machine deployment json with idx %d", i)
		}
		if err != nil {
			return nil, fmt.Errorf("error loading nested labels inside nodeTemplate belonging to machine deployment json with idx %d: %w", i, err)
		}
		taintsVal, found, err := unstructured.NestedSlice(nodeTemplate, "spec", "taints")
		if err != nil {
			return nil, fmt.Errorf("error loading nested taints inside nodeTemplate.spec.taints belonging to machine deployment json with idx %d: %w", i, err)
		}

		var taints []corev1.Taint
		if found {
			for _, tv := range taintsVal {
				tvMap := tv.(map[string]any)
				taints = append(taints, corev1.Taint{
					Key:       tvMap["key"].(string),
					Value:     tvMap["value"].(string),
					Effect:    corev1.TaintEffect(tvMap["effect"].(string)),
					TimeAdded: nil,
				})
			}
		}
		klog.Infof("found taints inside  nodeTemplate belonging to machine deployment json with idx %d: %s", i, taintsVal)

		class, found, err := unstructured.NestedStringMap(specMap, "template", "spec", "class")
		if !found {
			return nil, fmt.Errorf("cannot find nested class inside nodeTemplate belonging to machine deployment json with idx %d", i)
		}
		mcdInfo := gsc.MachineDeploymentInfo{
			SnapshotMeta: gsc.SnapshotMeta{
				CreationTimestamp: itemObj.GetCreationTimestamp().Time,
				SnapshotTimestamp: time.Now(),
				Name:              name,
				Namespace:         namespace,
			},
			Replicas:         replicas,
			PoolName:         labels["worker.gardener.cloud/pool"],
			Zone:             gsc.GetZoneAnyMap(labelsVal),
			MaxSurge:         intstr.IntOrString{},
			MaxUnavailable:   intstr.IntOrString{},
			MachineClassName: class["name"],
			Labels:           labels,
			Taints:           taints,
			Hash:             "",
		}
		mcdInfos[i] = mcdInfo
	}
	return mcdInfos, nil
}
