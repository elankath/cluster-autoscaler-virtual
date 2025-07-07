package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	gsc "github.com/elankath/gardener-scaling-common"
	"github.com/elankath/gardener-scaling-common/clientutil"
	"github.com/spf13/pflag"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/virtual"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/v1/pod"
	"math"
	"os"
	"slices"
	"time"
)

var (
	ErrMissingOpt         = errors.New("missing option")
	log                   klog.Logger
	DefaultKubeConfigPath = "/tmp/kvcl.yaml"
)

const (
	ExitSuccess      int = iota
	ExitErrParseOpts     = 1
)

func main() {
	mainOpts, err := ParseProgramFlags(os.Args[1:])
	if err != nil {
		if errors.Is(err, pflag.ErrHelp) {
			return
		}
		_, _ = fmt.Fprintf(os.Stderr, "Err: %v\n", err)
		os.Exit(ExitErrParseOpts)
	}
	log = klog.NewKlogr()
	err = simulateScaling(context.Background(), mainOpts)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(2)
	}

}

func simulateScaling(ctx context.Context, mo *MainOpts) error {
	log.Info("Starting scale simulation with cluster snapshot", "clusterSnapshotPath", mo.ClusterSnapshotPath)
	cs, err := loadClusterSnapshot(mo.ClusterSnapshotPath)
	if err != nil {
		return err
	}
	client, err := createClient(mo.KubeConfigPath)
	if err != nil {
		return err
	}
	log.Info("Saving AutoscalerConfig with pause mode for virtual CA...")
	cs.AutoscalerConfig.Mode = gsc.AutoscalerReplayerPauseMode
	cs.AutoscalerConfig.SuccessSignalPath = "/tmp/success.signal"
	cs.AutoscalerConfig.ErrorSignalPath = "/tmp/error.signal"
	err = virtual.SaveAutoscalerConfig(virtual.DefaultVirtualAutoscalerConfigPath, cs.AutoscalerConfig)
	if err != nil {
		return err
	}
	err = virtual.WaitForVirtualCARefresh(ctx, log, len(cs.AutoscalerConfig.ExistingNodes), cs.AutoscalerConfig.SuccessSignalPath, cs.AutoscalerConfig.ErrorSignalPath)
	if err != nil {
		return err
	}
	err = syncVirtualCluster(ctx, client, cs)
	if err != nil {
		return err
	}
	log.Info("Waiting for virtual cluster to be stabilized...", "stabilizeInterval", mo.StabilizeInterval)
	<-time.After(mo.StabilizeInterval)
	usPodNames, err := getUnscheduledPodNames(ctx, client)
	if err != nil {
		return err
	}
	log.Info("Unscheduled pod names", "unscheduledPodNames", usPodNames)
	//  kubectl get pods -A -o json | jq -r '.items[] | select(.spec.nodeName == null) | [.metadata.namespace, .metadata.name] | @tsv'
	log.Info("Unpausing the virtual CA...")
	err = virtual.UnPauseCA(ctx)
	<-time.After(mo.StabilizeInterval)
	if err != nil {
		return err
	}
	return nil
}

func waitTillPodStabilized(ctx context.Context, client *kubernetes.Clientset, stabilizeInterval time.Duration) error {
	pods, err := clientutil.ListAllPods(ctx, client)
	if err != nil {
		return err
	}
	mark := time.Now()
	checkInterval := time.Second * 5
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(checkInterval):
			if time.Since(mark) > stabilizeInterval {
				return nil
			}
			for _, p := range pods {
				_, cond := pod.GetPodCondition(&p.Status, corev1.PodScheduled)
				if cond != nil && cond.Status == corev1.ConditionTrue {
					continue
				}
			}

		}
	}
}

func loadClusterSnapshot(filePath string) (cs gsc.ClusterSnapshot, err error) {
	bytes, err := os.ReadFile(filePath)
	if err != nil {
		return
	}

	err = json.Unmarshal(bytes, &cs)
	if err != nil {
		return
	}

	return
}
func syncVirtualCluster(ctx context.Context, client *kubernetes.Clientset, cs gsc.ClusterSnapshot) (err error) {
	nsSet := cs.GetPodNamspaces()
	err = createNamespaces(ctx, client, nsSet.UnsortedList()...)
	for _, pClass := range cs.PriorityClasses {
		_, err = client.SchedulingV1().PriorityClasses().Create(ctx, &pClass.PriorityClass, metav1.CreateOptions{})
		if err != nil {
			if apierrors.IsAlreadyExists(err) {
				klog.Infof("priorityclass %s already exists", pClass.Name)
				continue
			}
			return fmt.Errorf("syncVirtualCluster cannot create the priority class %s: %w", pClass.Name, err)
		}
		log.Info("syncVirtualCluster successfully created the priority class", "pc.Name", pClass.Name)
	}
	err = deployScheduledPods(ctx, client, cs.Pods)
	if err != nil {
		return err
	}
	err = deployUnScheduledPods(ctx, client, cs.Pods)
	if err != nil {
		return err
	}
	return nil
}

// MainOpts is a struct that encapsulates target fields for CLI options parsing.
type MainOpts struct {
	KubeConfigPath       string
	ClusterSnapshotPath  string
	StabilizeIntervalStr string
	StabilizeInterval    time.Duration
}

func ParseProgramFlags(args []string) (*MainOpts, error) {
	flagSet, mainOpts := SetupFlagsToOpts()
	err := flagSet.Parse(args)
	if err != nil {
		return nil, err
	}
	err = ValidateMainOpts(mainOpts)
	if err != nil {
		return nil, err
	}
	return mainOpts, nil
}
func SetupFlagsToOpts() (*pflag.FlagSet, *MainOpts) {
	var mainOpts MainOpts
	flagSet := pflag.NewFlagSet("scalesim", pflag.ContinueOnError)

	mainOpts.KubeConfigPath = os.Getenv(clientcmd.RecommendedConfigPathEnvVar)
	if mainOpts.KubeConfigPath == "" {
		mainOpts.KubeConfigPath = DefaultKubeConfigPath
	}
	flagSet.StringVarP(&mainOpts.KubeConfigPath, clientcmd.RecommendedConfigPathFlag, "k", mainOpts.KubeConfigPath, "path of virtual cluster kubeconfig - fallback to KUBECONFIG env-var or "+DefaultKubeConfigPath)
	flagSet.StringVarP(&mainOpts.ClusterSnapshotPath, "snapshot-path", "s", "", "cluster snapshot path")
	flagSet.StringVarP(&mainOpts.StabilizeIntervalStr, "stabilize-interval", "S", "30s", "stabilize interval")
	//flagSet.IntVarP(&mainOpts.Port, "port", "P", api.DefaultPort, "listen port for REST API")
	//flagSet.IntVarP(&mainOpts.WatchQueueSize, "watch-queue-size", "s", api.DefaultWatchQueueSize, "max number of events to queue per watcher")
	//flagSet.DurationVarP(&mainOpts.WatchTimeout, "watch-timeout", "t", api.DefaultWatchTimeout, "watch timeout after which connection is closed and watch removed")
	//flagSet.BoolVarP(&mainOpts.ProfilingEnabled, "pprof", "p", false, "enable pprof profiling")

	klogFlagSet := flag.NewFlagSet("klog", flag.ContinueOnError)
	klog.InitFlags(klogFlagSet)
	// Merge klog flags into pflag
	flagSet.AddGoFlagSet(klogFlagSet)

	return flagSet, &mainOpts
}

func ValidateMainOpts(opts *MainOpts) error {
	if opts.KubeConfigPath == "" {
		return fmt.Errorf("%w: --kubeconfig/-k flag is required", ErrMissingOpt)
	}
	if opts.ClusterSnapshotPath == "" {
		return fmt.Errorf("%w: --snapshot-path/-s is required", ErrMissingOpt)
	}
	stabilizeInterval, err := time.ParseDuration(opts.StabilizeIntervalStr)
	if err != nil {
		return err
	}
	opts.StabilizeInterval = stabilizeInterval
	return nil
}
func deployScheduledPods(ctx context.Context, client *kubernetes.Clientset, pods []gsc.PodInfo) error {
	pods = slices.Clone(pods)
	pods = slices.DeleteFunc(pods, func(info gsc.PodInfo) bool {
		return info.NodeName == "" || info.Spec.NodeName == ""
	})
	slices.SortFunc(pods, func(a, b gsc.PodInfo) int {
		return a.CreationTimestamp.Compare(b.CreationTimestamp)
	})
	log.Info("Deploying scheduled pods...", "numPods", len(pods))
	return deployPods(ctx, client, pods)
}
func deployUnScheduledPods(ctx context.Context, client *kubernetes.Clientset, pods []gsc.PodInfo) error {
	pods = slices.Clone(pods)
	pods = slices.DeleteFunc(pods, func(info gsc.PodInfo) bool {
		return info.NodeName != "" || info.Spec.NodeName != ""
	})
	for _, p := range pods {
		if p.NodeName == "" && p.Spec.NodeName != "" {
			return fmt.Errorf("for %q p.NodeName empty but pode.Spec.NodeName non-empty", p.Name)
		}
		if p.NodeName != "" && p.Spec.NodeName == "" {
			return fmt.Errorf("for %q p.NodeName not empty but pode.Spec.NodeName empty", p.Name)
		}
	}
	slices.SortFunc(pods, func(a, b gsc.PodInfo) int {
		return a.CreationTimestamp.Compare(b.CreationTimestamp)
	})
	log.Info("Deploying unscheduled pods...", "numPods", len(pods))
	return deployPods(ctx, client, pods)
}
func deployPods(ctx context.Context, client *kubernetes.Clientset, pods []gsc.PodInfo) error {
	slices.SortFunc(pods, func(a, b gsc.PodInfo) int {
		if a.NodeName == "" || a.Spec.NodeName == "" {
			return math.MaxInt
		}
		if b.NodeName == "" || b.Spec.NodeName == "" {
			return math.MaxInt
		}
		return a.CreationTimestamp.Compare(b.CreationTimestamp)
	})
	for _, pinfo := range pods {
		pod := getCorePodFromPodInfo(pinfo)
		err := doDeployPod(ctx, client, pod)
		if err != nil {
			return err
		}
	}
	return nil
}

func doDeployPod(ctx context.Context, clientSet *kubernetes.Clientset, pod corev1.Pod) error {
	// TODO ensure you don't deploy pods that are already present in the cluster
	podNew, err := clientSet.CoreV1().Pods(pod.Namespace).Create(ctx, &pod, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("doDeployPod cannot create the pod  %q: %w", pod.Name, err)
	}
	if podNew.Spec.NodeName != "" {
		podNew.Status.Phase = corev1.PodRunning
		_, err = clientSet.CoreV1().Pods(pod.Namespace).UpdateStatus(ctx, podNew, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("doDeployPod cannot change the pod Phase to Running for %s: %w", pod.Name, err)
		}
	}
	log.Info("doDeployPod finished.", "pod.Name", pod.Name, "pod.Namespace", pod.Namespace, "pod.NodeName", pod.Spec.NodeName)
	return nil
}

func getCorePodFromPodInfo(podInfo gsc.PodInfo) corev1.Pod {
	pod := corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Labels:    podInfo.Labels,
			Name:      podInfo.Name,
			Namespace: podInfo.Namespace,
			UID:       types.UID(podInfo.UID),
		},
		Spec: podInfo.Spec,
	}
	pod.Spec.NodeName = podInfo.NodeName
	pod.Status.NominatedNodeName = podInfo.NominatedNodeName
	return pod
}

func createClient(kubeConfigPath string) (client *kubernetes.Clientset, err error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	if err != nil {
		return
	}
	config.QPS = 200
	config.Burst = 100
	client, err = kubernetes.NewForConfig(config)
	if err != nil {
		return
	}
	return
}

func createNamespaces(ctx context.Context, clientSet *kubernetes.Clientset, nss ...string) error {
	for _, ns := range nss {
		if ns == "default" {
			continue
		}
		namespace := corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: ns,
			},
		}
		_, err := clientSet.CoreV1().Namespaces().Create(ctx, &namespace, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("cannot create the namespace %q in virtual cluster: %w", ns, err)
		}
		log.Info("created namespace", "namespace", ns)
	}
	return nil
}

func getUnscheduledPodNames(ctx context.Context, clientSet *kubernetes.Clientset) (podNames []string, err error) {
	pods, err := clientutil.ListAllPods(ctx, clientSet)
	if err != nil {
		return
	}
	for _, pod := range pods {
		if pod.Spec.NodeName == "" {
			podNames = append(podNames, pod.Name)
		}
	}
	return
}
