package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	unstructured "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	dynamicclient "k8s.io/client-go/dynamic"
	kubeclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog"
	"sigs.k8s.io/yaml"

	opv1 "github.com/openshift/api/operator/v1"
	"github.com/openshift/library-go/pkg/controller/controllercmd"
	"github.com/openshift/library-go/pkg/operator/csi/csicontrollerset"
	goc "github.com/openshift/library-go/pkg/operator/genericoperatorclient"
	"github.com/openshift/library-go/pkg/operator/v1helpers"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// Operand and operator run in the same namespace
	defaultNamespace = "openshift-cluster-csi-drivers"
	operatorName     = "kubevirt-csi-driver-operator"
	operandName      = "kubevirt-csi-driver"
	instanceName     = "csi.kubevirt.io"
)

// installConfig is used for reading cluster's install-config YAML
type kubevirt struct {
	StorageClass string
}
type platform struct {
	Kubevirt kubevirt
}
type installConfig struct {
	Platform platform
}

func RunOperator(ctx context.Context, controllerConfig *controllercmd.ControllerContext) error {
	// Create clientsets and informers
	kubeClient := kubeclient.NewForConfigOrDie(rest.AddUserAgent(controllerConfig.KubeConfig, operatorName))
	dynamicClient := dynamicclient.NewForConfigOrDie(rest.AddUserAgent(controllerConfig.KubeConfig, operatorName))
	kubeInformersForNamespaces := v1helpers.NewKubeInformersForNamespaces(kubeClient, defaultNamespace, "")

	// Create driver config YAML
	err := createDriverConfig(ctx, kubeClient)
	if err != nil {
		panic(err)
	}

	// Create storage class
	err = createStorageClass(ctx, kubeClient, dynamicClient)
	if err != nil {
		panic(err)
	}

	// Create GenericOperatorclient. This is used by the library-go controllers created down below
	gvr := opv1.SchemeGroupVersion.WithResource("clustercsidrivers")
	operatorClient, dynamicInformers, err := goc.NewClusterScopedOperatorClientWithConfigName(controllerConfig.KubeConfig, gvr, instanceName)
	if err != nil {
		return err
	}

	csiControllerSet := csicontrollerset.NewCSIControllerSet(
		operatorClient,
		controllerConfig.EventRecorder,
	).WithLogLevelController().WithManagementStateController(
		operandName,
		false,
	).WithStaticResourcesController(
		"KubevirtDriverStaticResources",
		kubeClient,
		kubeInformersForNamespaces,
		asset,
		[]string{
			"configmap.yaml",
			"csi-driver.yaml",
			"node-sa.yaml",
			"node-cr.yaml",
			"node-binding.yaml",
			"controller-sa.yaml",
			"controller-cr.yaml",
			"controller-binding.yaml",
		},
	).
		WithCredentialsRequestController(
			"KubevirtDriverCredentialsRequestController",
			defaultNamespace,
			assetPanic,
			"credentials-request.yaml",
			dynamicClient,
		).
		WithCSIDriverController(
			"KubevirtDriverController",
			instanceName,
			operandName,
			defaultNamespace,
			assetPanic,
			kubeClient,
			kubeInformersForNamespaces.InformersFor(defaultNamespace),
			csicontrollerset.WithControllerService("controller.yaml"),
			csicontrollerset.WithNodeService("node.yaml"),
		)

	if err != nil {
		return err
	}

	klog.Info("Starting the informers")
	go kubeInformersForNamespaces.Start(ctx.Done())
	go dynamicInformers.Start(ctx.Done())

	klog.Info("Starting controllerset")
	go csiControllerSet.Run(ctx, 1)

	<-ctx.Done()

	return fmt.Errorf("stopped")
}

func asset(name string) ([]byte, error) {
	return ioutil.ReadFile("assets/" + name) // Folder assets must be placed in the process's working directory
}

func assetPanic(name string) []byte {
	bytes, err := asset(name)
	if err != nil {
		panic("Fetching asset " + name + " failed. Error: " + err.Error())
	}

	return bytes
}

func createDriverConfig(ctx context.Context, kubeClient *kubeclient.Clientset) error {
	configMap, err := kubeClient.CoreV1().ConfigMaps("openshift-config").Get(ctx, "cloud-provider-config", metav1.GetOptions{})
	if err != nil {
		return err
	}

	jsonConfig, ok := configMap.Data["config"]
	if !ok {
		return fmt.Errorf("Field config in ConfigMap openshift-config/cloud-provider-config is missing")
	}

	var config map[string]string
	err = json.Unmarshal([]byte(jsonConfig), &config)
	if err != nil {
		return err
	}

	namespace, ok := config["namespace"]
	if !ok {
		return fmt.Errorf("Missing namespace in JSON string. Check field config in ConfigMap openshift-config/cloud-provider-config")
	}

	infraID, ok := config["infraID"]
	if !ok {
		return fmt.Errorf("Missing infraID in JSON string. Check field config in ConfigMap openshift-config/cloud-provider-config")
	}

	driverConfig := &corev1.ConfigMap{}

	driverConfig.APIVersion = corev1.SchemeGroupVersion.String()
	driverConfig.Kind = "ConfigMap"
	driverConfig.Name = "driver-config"
	driverConfig.Namespace = defaultNamespace
	driverConfig.Data = map[string]string{
		"infraClusterNamespace": namespace,
		"infraClusterLabels":    fmt.Sprintf("tenantcluster-%s-machine.openshift.io=owned", infraID),
	}

	bytes, err := yaml.Marshal(driverConfig)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile("assets/configmap.yaml", bytes, 0777)
	if err != nil {
		return err
	}

	return nil
}

func createStorageClass(ctx context.Context, kubeClient *kubeclient.Clientset, dynamicClient dynamicclient.Interface) error {
	const storageClassName = "kubevirt-csi-driver"

	// Check whether StorageClass already exists
	_, err := kubeClient.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		klog.Error("Failed reading StorageClass " + storageClassName)
		return err
	}

	if err == nil {
		klog.Info("StorageClass " + storageClassName + " already exists")
		return nil
	}

	// StorageClass does not exist. Create it.
	infraStorageClassName := ""

	// We need to figure out which storage class to use in infra cluster (where kubevirt is installed).
	// Try reading from kube-system/ConfigMap/cluster-config-v1
	// Try reading from openshift-machine-api/MachineSets. Use first in the list.
	// If not found then log warning and return

	infraStorageClassName, err = extractStorageClassFromClusterConfig(ctx, kubeClient)
	if err != nil {
		return err
	}

	if infraStorageClassName == "" {
		infraStorageClassName, err = extractStorageClassFromMachineSet(ctx, dynamicClient)
		if err != nil {
			return err
		}
	}

	// If storage class name not found then return
	if infraStorageClassName == "" {
		klog.Warning("Can not create StorageClass for driver. Infra cluster StorageClass name not found. Please create StorageClass manually. Refer to driver's documentation https://github.com/kubevirt/csi-driver")
		return nil
	}

	klog.Info("StorageClass " + storageClassName + " will be created")

	storageClass := &storagev1.StorageClass{}

	storageClass.APIVersion = storagev1.SchemeGroupVersion.String()
	storageClass.Kind = "StorageClass"
	storageClass.Name = storageClassName
	storageClass.Provisioner = instanceName
	storageClass.Parameters = map[string]string{"infraStorageClassName": infraStorageClassName}

	_, err = kubeClient.StorageV1().StorageClasses().Create(ctx, storageClass, metav1.CreateOptions{})
	if err != nil {
		klog.Error("Failed creating StorageClass. Error: " + err.Error())
		return err
	}

	return nil
}

func extractStorageClassFromClusterConfig(ctx context.Context, kubeClient *kubeclient.Clientset) (string, error) {
	// Get ConfigMap
	configMap, err := kubeClient.CoreV1().ConfigMaps("kube-system").Get(ctx, "cluster-config-v1", metav1.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return "", err
	}

	// No ConfigMap
	if err != nil {
		return "", nil
	}

	// ConfigMap exists
	yamlInstallConfig, ok := configMap.Data["install-config"]

	if !ok {
		return "", fmt.Errorf("install-config field is missing from ConfigMap kube-system/cluster-config-v1")
	}

	installConfig := installConfig{}
	err = yaml.Unmarshal([]byte(yamlInstallConfig), &installConfig)
	if err != nil {
		return "", err
	}

	storageClassName := installConfig.Platform.Kubevirt.StorageClass

	if storageClassName != "" {
		klog.Infof("Found infra cluster storage class name '" + storageClassName + "' in ConfigMap kube-system/cluster-config-v1")
	}

	return storageClassName, nil
}

func extractStorageClassFromMachineSet(ctx context.Context, client dynamicclient.Interface) (string, error) {
	resource := schema.GroupVersionResource{
		Group:    "machine.openshift.io",
		Version:  "v1beta1",
		Resource: "machinesets",
	}

	list, err := client.Resource(resource).Namespace("openshift-machine-api").List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
	}

	if list == nil || list.Items == nil || len(list.Items) == 0 {
		return "", nil
	}

	storageClassName, _, err := unstructured.NestedString(list.Items[0].Object, "spec", "template", "spec", "providerSpec", "value", "storageClassName")
	if err != nil {
		return "", err
	}

	if storageClassName != "" {
		klog.Infof("Found infra cluster storage class name '" + storageClassName + "' in MachineSet openshift-machine-api/" + list.Items[0].GetName())
	}

	return storageClassName, nil
}
