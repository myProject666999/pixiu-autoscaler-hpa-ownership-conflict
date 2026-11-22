/*
Copyright 2021 The Pixiu Authors.

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

package controller

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	utilpointer "k8s.io/utils/pointer"
)

var (
	KeyFunc = cache.DeletionHandlingMetaNamespaceKeyFunc
)

type ControllerClientBuilder interface {
	Config(name string) (*restclient.Config, error)
	ConfigOrDie(name string) *restclient.Config
	Client(name string) (clientset.Interface, error)
	ClientOrDie(name string) clientset.Interface
}

// SimpleControllerClientBuilder returns a fixed client with different user agents
type SimpleControllerClientBuilder struct {
	// ClientConfig is a skeleton config to clone and use as the basis for each controller client
	ClientConfig *restclient.Config
}

func (b SimpleControllerClientBuilder) Config(name string) (*restclient.Config, error) {
	clientConfig := *b.ClientConfig
	return restclient.AddUserAgent(&clientConfig, name), nil
}

func (b SimpleControllerClientBuilder) ConfigOrDie(name string) *restclient.Config {
	clientConfig, err := b.Config(name)
	if err != nil {
		klog.Fatal(err)
	}
	return clientConfig
}

func (b SimpleControllerClientBuilder) Client(name string) (clientset.Interface, error) {
	clientConfig, err := b.Config(name)
	if err != nil {
		return nil, err
	}
	return clientset.NewForConfig(clientConfig)
}

func (b SimpleControllerClientBuilder) ClientOrDie(name string) clientset.Interface {
	client, err := b.Client(name)
	if err != nil {
		klog.Fatal(err)
	}
	return client
}

func CreateHPAFromDeployment(d *appsv1.Deployment) (*autoscalingv2.HorizontalPodAutoscaler, error) {
	annotations := d.GetAnnotations()
	name := d.GetName()
	namespace := d.GetNamespace()
	uid := d.GetUID()
	apiVersion := AppsAPIVersion
	kind := Deployment

	minReplicas, err := extractReplicas(annotations, MinReplicas)
	if err != nil {
		return nil, fmt.Errorf("extract minReplicas from annotations failed: %v", err)
	}
	maxReplicas, err := extractReplicas(annotations, MaxReplicas)
	if err != nil {
		return nil, fmt.Errorf("extract maxReplicas from annotations failed: %v", err)
	}

	metrics, err := parseMetricSpecs(annotations)
	if err != nil {
		return nil, fmt.Errorf("parse metric specs from annotations failed: %v", err)
	}

	controller := true
	blockOwnerDeletion := true
	// Inject ownerReference label
	ownerReference := metav1.OwnerReference{
		APIVersion:         apiVersion,
		Kind:               kind,
		Name:               name,
		UID:                uid,
		Controller:         &controller,
		BlockOwnerDeletion: &blockOwnerDeletion,
	}

	spec := autoscalingv2.HorizontalPodAutoscalerSpec{
		MinReplicas: utilpointer.Int32Ptr(minReplicas),
		MaxReplicas: maxReplicas,
		ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
			APIVersion: apiVersion,
			Kind:       kind,
			Name:       name,
		},
		Metrics: metrics,
	}

	return &autoscalingv2.HorizontalPodAutoscaler{
		TypeMeta: metav1.TypeMeta{
			Kind:       HorizontalPodAutoscaler,
			APIVersion: AutoscalingAPIVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      HPANameForDeployment(d),
			Namespace: namespace,
			OwnerReferences: []metav1.OwnerReference{
				ownerReference,
			},
			Labels: d.Spec.Selector.MatchLabels,
		},
		Spec: spec,
	}, nil
}

// HPANameForDeployment 返回 Deployment 对应的期望 HPA 名称（按 Deployment 名确定性生成）。
func HPANameForDeployment(d *appsv1.Deployment) string {
	return d.GetName() + "-" + computeHash(d.GetName())
}

// ManagedHPAFields 公开本控制器负责管理的 HPA 字段清单。
// 同步时只允许覆盖这些字段；用户自行添加的 Spec.Behavior、
// 无关的注解与标签必须原样保留，不允许整体对象替换。
var ManagedHPAFields = []string{
	"spec.minReplicas",
	"spec.maxReplicas",
	"spec.metrics",
	"spec.scaleTargetRef",
	"metadata.labels[" + PrometheusCustomMetric + "]",
}

// ApplyManagedHPAFields 将 desired 中由控制器管理的字段（见 ManagedHPAFields）
// 合并到 target 上，target 的其余字段（含 Spec.Behavior、用户注解与标签）保持不变。
// customMetric 表示该 Deployment 是否启用 prometheus 自定义指标（对应受管标签）。
// 返回是否有受管字段发生变化。
func ApplyManagedHPAFields(target, desired *autoscalingv2.HorizontalPodAutoscaler, customMetric bool) bool {
	changed := false

	if !reflect.DeepEqual(target.Spec.MinReplicas, desired.Spec.MinReplicas) {
		target.Spec.MinReplicas = desired.Spec.MinReplicas
		changed = true
	}
	if target.Spec.MaxReplicas != desired.Spec.MaxReplicas {
		target.Spec.MaxReplicas = desired.Spec.MaxReplicas
		changed = true
	}
	if !reflect.DeepEqual(target.Spec.Metrics, desired.Spec.Metrics) {
		target.Spec.Metrics = desired.Spec.Metrics
		changed = true
	}
	if !reflect.DeepEqual(target.Spec.ScaleTargetRef, desired.Spec.ScaleTargetRef) {
		target.Spec.ScaleTargetRef = desired.Spec.ScaleTargetRef
		changed = true
	}

	// 受管标签：prometheus 自定义指标标记
	_, hasLabel := target.Labels[PrometheusCustomMetric]
	if customMetric && !hasLabel {
		if target.Labels == nil {
			target.Labels = make(map[string]string)
		}
		target.Labels[PrometheusCustomMetric] = "true"
		changed = true
	}
	if !customMetric && hasLabel {
		delete(target.Labels, PrometheusCustomMetric)
		changed = true
	}

	return changed
}

func computeHash(objectToWrite string) string {
	hasher := md5.New()
	hasher.Write([]byte(objectToWrite))
	hashedData := hex.EncodeToString(hasher.Sum(nil))

	return hashedData[:9]
}

// Parse and get metric type (valid is cpu and memory) and target
func getMetricTarget(metricName string) (string, string, error) {
	metricTypeSlice := strings.Split(metricName, PixiuDot)
	if len(metricTypeSlice) < 2 {
		return "", "", fmt.Errorf("invalid metric item %s", metricName)
	}
	metricType := metricTypeSlice[0]
	if metricType != cpu && metricType != memory && metricType != prometheus {
		return "", "", fmt.Errorf("unsupprted metric resource name: %s", metricType)
	}

	metricTargetSlice := strings.Split(metricName, PixiuSeparator)
	if len(metricTargetSlice) < 2 {
		return "", "", fmt.Errorf("invalid metric item %s", metricName)
	}

	return metricType, metricTargetSlice[len(metricTargetSlice)-1], nil
}

func parseMetricSpecs(annotations map[string]string) ([]autoscalingv2.MetricSpec, error) {
	metricSpecs := make([]autoscalingv2.MetricSpec, 0)

	for metricName, metricValue := range annotations {
		// let it go if annotation item are not the target
		if !strings.Contains(metricName, PixiuDot+PixiuRootPrefix) {
			continue
		}
		metricType, target, err := getMetricTarget(metricName)
		if err != nil {
			return nil, err
		}

		metricSpec, err := parseMetricSpec(target, metricType, metricValue, annotations)
		if err != nil {
			return nil, err
		}
		metricSpecs = append(metricSpecs, metricSpec)
	}

	if len(metricSpecs) == 0 {
		return nil, fmt.Errorf("could't parse metric specs, the numbers is zero")
	}

	return metricSpecs, nil
}

func parseMetricSpec(target string, metricType string, metricValue string, annotations map[string]string) (autoscalingv2.MetricSpec, error) {
	if metricType == prometheus {
		return parseMetricSpecForPrometheus(target, metricType, metricValue, annotations)
	}

	return parseMetricSpecFor(target, metricType, metricValue, annotations)
}

func parseMetricSpecFor(target string, metricType string, metricValue string, annotations map[string]string) (autoscalingv2.MetricSpec, error) {
	metricSpec := autoscalingv2.MetricSpec{
		Type: autoscalingv2.ResourceMetricSourceType,
	}

	switch target {
	case targetAverageUtilization:
		averageUtilization, err := extractAverageUtilization(metricValue)
		if err != nil {
			return autoscalingv2.MetricSpec{}, err
		}
		metricSpec.Resource = &autoscalingv2.ResourceMetricSource{
			Target: autoscalingv2.MetricTarget{
				Type: autoscalingv2.UtilizationMetricType, AverageUtilization: utilpointer.Int32Ptr(averageUtilization),
			},
		}
	case targetAverageValue:
		averageValue, err := resource.ParseQuantity(metricValue)
		if err != nil {
			return autoscalingv2.MetricSpec{}, err
		}
		metricSpec.Resource = &autoscalingv2.ResourceMetricSource{
			Target: autoscalingv2.MetricTarget{
				Type: autoscalingv2.AverageValueMetricType, AverageValue: &averageValue,
			},
		}
	}

	switch metricType {
	case cpu:
		metricSpec.Resource.Name = v1.ResourceCPU
	case memory:
		metricSpec.Resource.Name = v1.ResourceMemory
	}

	return metricSpec, nil
}

func parseMetricSpecForPrometheus(target string, metricType string, metricValue string, annotations map[string]string) (autoscalingv2.MetricSpec, error) {
	name, ok := annotations[PrometheusCustomMetric]
	if !ok {
		return autoscalingv2.MetricSpec{}, fmt.Errorf("failed to get targetCustomMetric from annotations")
	}
	averageValue, err := resource.ParseQuantity(metricValue)
	if err != nil {
		return autoscalingv2.MetricSpec{}, nil
	}

	metricSpec := autoscalingv2.MetricSpec{
		Type: autoscalingv2.ExternalMetricSourceType,
		External: &autoscalingv2.ExternalMetricSource{
			Metric: autoscalingv2.MetricIdentifier{
				Name: name,
			},
			Target: autoscalingv2.MetricTarget{
				AverageValue: &averageValue,
			},
		},
	}

	switch target {
	case targetAverageUtilization:
		metricSpec.External.Target.Type = autoscalingv2.UtilizationMetricType
	case targetAverageValue:
		metricSpec.External.Target.Type = autoscalingv2.AverageValueMetricType
	}

	return metricSpec, nil
}

// IsOwnerReference 是通用工具函数：判断 uid 是否出现在 ownerReferences 中。
// 注意：同步路径的所有权判定不依赖该函数，统一以 controller ownerReference
// 的 UID/Kind/Name 完全匹配为准。
func IsOwnerReference(uid types.UID, ownerReferences []metav1.OwnerReference) bool {
	var isOwnerRef bool
	for _, ownerReference := range ownerReferences {
		if uid == ownerReference.UID {
			isOwnerRef = true
			break
		}
	}
	return isOwnerRef
}

// ManageByPixiuController 仅用于诊断/观测：报告 HPA 是否曾被本控制器写入过。
// 注意：managedFields 中的 manager 名称不构成接管授权，不得据此修改或删除
// 任何对象；所有权判定只能依据 controller ownerReference 的 UID/Kind/Name。
func ManageByPixiuController(hpa *autoscalingv2.HorizontalPodAutoscaler) bool {
	for _, managedField := range hpa.ManagedFields {
		if managedField.APIVersion == AutoscalingAPIVersion &&
			managedField.Manager == PixiuManager {
			return true
		}
	}

	return false
}

func extractReplicas(annotations map[string]string, replicasType string) (int32, error) {
	var (
		Replicas string
		exists   bool
	)
	switch replicasType {
	case MinReplicas:
		Replicas, exists = annotations[MinReplicas]
		if !exists {
			return 1, nil // Default minReplicas is 1
		}
	case MaxReplicas:
		Replicas, exists = annotations[MaxReplicas]
		if !exists {
			return 6, nil // Default maxReplicas is 6
		}
	}

	targetReplicas, err := strconv.ParseInt(Replicas, 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(targetReplicas), err
}

func extractAverageUtilization(averageUtilization string) (int32, error) {
	value64, err := strconv.ParseInt(averageUtilization, 10, 32)
	if err != nil {
		return 0, err
	}
	if value64 <= 0 && value64 > 100 {
		return 0, fmt.Errorf("averageUtilization should be range 1 between 100")
	}

	return int32(value64), nil
}

// Empty is public since it is used by some internal API objects for conversions between external
// string arrays and internal sets, and conversion logic requires public types today.
type Empty struct{}

func NewItems() map[string]Empty {
	items := make(map[string]Empty)
	for _, k := range []string{cpuAverageUtilization, memoryAverageUtilization, prometheusAverageUtilization, cpuAverageValue, memoryAverageValue, prometheusAverageValue} {
		items[k] = Empty{}
	}

	return items
}
