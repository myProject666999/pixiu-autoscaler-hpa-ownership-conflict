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

package autoscaler

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	"k8s.io/api/core/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	autoscalinginformers "k8s.io/client-go/informers/autoscaling/v2"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	appslisters "k8s.io/client-go/listers/apps/v1"
	autoscalinglisters "k8s.io/client-go/listers/autoscaling/v2"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/component-base/metrics/prometheus/ratelimiter"
	"k8s.io/klog/v2"

	"github.com/caoyingjunz/pixiu-autoscaler/pkg/controller"
)

const (
	maxRetries = 15

	// maxUpdateConflictRetries 更新托管 HPA 时乐观锁冲突的最大重试次数。
	// 每次冲突都重新读取、重新判断归属并合并受管字段，不盲目重放旧对象。
	maxUpdateConflictRetries = 3
)

// 事件原因
const (
	reasonFailedNewestHPA = "FailedNewestHPA"
	reasonCreateHPA       = "CreateHPA"
	reasonFailedCreateHPA = "FailedCreateHPA"
	reasonUpdateHPA       = "UpdateHPA"
	reasonFailedUpdateHPA = "FailedUpdateHPA"
	reasonDeleteHPA       = "DeleteHPA"
	reasonFailedDeleteHPA = "FailedDeleteHPA"
	// reasonHPAConflict 存在非托管但冲突的 HPA（占用期望名称或指向同一伸缩目标）
	reasonHPAConflict = "HPAConflict"
	// reasonOrphanedHPA 同名 Deployment 重建后遗留的旧 UID HPA，等待垃圾回收
	reasonOrphanedHPA = "OrphanedHPA"
)

var (
	// orphanRequeueDelay 是等待旧 UID HPA 被垃圾回收时的重新入队间隔。
	// 该等待走独立的延时入队，不消耗 maxRetries 的配置失败重试计数。
	orphanRequeueDelay = 10 * time.Second
	// eventDedupInterval 是相同冲突事件的最小重复间隔，避免同一冲突反复刷事件。
	eventDedupInterval = 10 * time.Minute
)

// AutoscalerController is responsible for synchronizing HPA objects stored
// in the system.
type AutoscalerController struct {
	client        clientset.Interface
	eventRecorder record.EventRecorder

	syncHandler       func(dKey string) error
	enqueueDeployment func(deployment *appsv1.Deployment)

	syncConfigMapHandler func(dKey string) error
	enqueueConfigMap     func(cm *corev1.ConfigMap)

	// dLister can list/get deployments from the shared informer's store
	dLister appslisters.DeploymentLister
	// hpaLister is able to list/get HPAs from the shared informer's cache
	hpaLister autoscalinglisters.HorizontalPodAutoscalerLister
	// cmLister is able to list/get Configmaps from the shared informer's cache
	cmLister corelisters.ConfigMapLister

	// dListerSynced returns true if the Deployment store has been synced at least once.
	dListerSynced cache.InformerSynced
	// hpaListerSynced returns true if the HPA store has been synced at least once.
	hpaListerSynced cache.InformerSynced
	// cmListerSynced returns true if the configmap store has been synced at least once.
	cmListerSynced cache.InformerSynced

	// AutoscalerController that need to be synced
	queue workqueue.RateLimitingInterface

	cmQueue workqueue.RateLimitingInterface

	// Store and returns a reference to an empty store.
	items map[string]controller.Empty

	// eventLock 保护 lastEventAt。
	eventLock sync.Mutex
	// lastEventAt 记录同类事件上次发出的时间，用于冲突事件去重。
	lastEventAt map[string]time.Time
}

// NewAutoscalerController creates a new AutoscalerController.
func NewAutoscalerController(
	dInformer appsinformers.DeploymentInformer,
	hpaInformer autoscalinginformers.HorizontalPodAutoscalerInformer,
	cmInformer coreinformers.ConfigMapInformer,
	client clientset.Interface) (*AutoscalerController, error) {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: client.CoreV1().Events("")})

	if client != nil && client.CoreV1().RESTClient().GetRateLimiter() != nil {
		if err := ratelimiter.RegisterMetricAndTrackRateLimiterUsage("pixiu_autoscaler", client.CoreV1().RESTClient().GetRateLimiter()); err != nil {
			return nil, err
		}
	}

	ac := &AutoscalerController{
		client:        client,
		eventRecorder: eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "pixiu-autoscaler"}),
		queue:         workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "pixiu"),
		cmQueue:       workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "pixiu"),
		items:         controller.NewItems(),
	}

	// Deployment
	dInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ac.addDeployment,
		UpdateFunc: ac.updateDeployment,
		DeleteFunc: ac.deleteDeployment,
	})

	// HorizontalPodAutoscaler
	hpaInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ac.addHPA,
		UpdateFunc: ac.updateHPA,
		DeleteFunc: ac.deleteHPA,
	})

	// ConfigMap
	cmInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ac.addConfigMap,
		UpdateFunc: ac.updateConfigMap,
		DeleteFunc: ac.deleteConfigMap,
	})

	ac.dLister = dInformer.Lister()
	ac.hpaLister = hpaInformer.Lister()
	ac.cmLister = cmInformer.Lister()

	// syncAutoscalers
	ac.syncHandler = ac.syncAutoscalers
	ac.enqueueDeployment = ac.enqueue

	// syncConfigMaps
	ac.syncConfigMapHandler = ac.syncConfigMaps
	ac.enqueueConfigMap = ac.enqueueCM

	ac.dListerSynced = dInformer.Informer().HasSynced
	ac.hpaListerSynced = hpaInformer.Informer().HasSynced
	ac.cmListerSynced = cmInformer.Informer().HasSynced

	return ac, nil
}

// Run begins watching and syncing.
func (ac *AutoscalerController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer ac.queue.ShutDown()

	klog.Infof("Starting Pixiu Autoscaler Controller")
	defer klog.Infof("Shutting down Pixiu Autoscaler Controller")

	// Wait for all involved caches to be synced, before processing items from the queue is started
	if !cache.WaitForNamedCacheSync("pixiu-autoscaler-controller", stopCh, ac.dListerSynced, ac.hpaListerSynced, ac.cmListerSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(ac.worker, time.Second, stopCh)
	}
	for i := 0; i < workers; i++ {
		go wait.Until(ac.configMapWorker, time.Second, stopCh)
	}

	<-stopCh
}

// IsCustomMetricHPA 判断 deployment 是否维护自定位指标的 HPA
func (ac *AutoscalerController) IsCustomMetricHPA(d *appsv1.Deployment) bool {
	if !ac.IsDeploymentControlHPA(d) {
		return false
	}

	annotations := d.GetAnnotations()
	_, ok := annotations[controller.PrometheusCustomMetric]
	return ok
}

// IsDeploymentControlHPA 判断 deployment 是否维护 HPA
func (ac *AutoscalerController) IsDeploymentControlHPA(d *appsv1.Deployment) bool {
	annotations := d.GetAnnotations()
	if annotations == nil {
		return false
	}

	for annotation := range annotations {
		_, found := ac.items[annotation]
		if found {
			return true
		}
	}

	return false
}

func (ac *AutoscalerController) syncConfigMaps(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		klog.ErrorS(err, "Failed to split meta namespace cache key", "cacheKey", key)
		return err
	}
	if name != controller.DesireConfigMapName {
		return nil
	}

	hpaList, err := ac.hpaLister.List(labels.Set{controller.PrometheusCustomMetric: "true"}.AsSelector())
	if err != nil {
		return err
	}
	var externalRules []controller.ExternalRule
	for _, h := range hpaList {
		// 无需检查，hpa API已做个检验，此处为遵守coding规范
		metrics := h.Spec.Metrics
		if len(metrics) == 0 {
			return fmt.Errorf("no metric found in hpa(%s)", h.Name)
		}
		metric := metrics[0]
		if metric.External == nil {
			return fmt.Errorf("no external metric found in hpa(%s)", h.Name)
		}

		externalRules = append(externalRules, controller.ExternalRule{
			MetricsQuery: "<<.Series>>",
			Name: controller.RuleName{
				As:      "",
				Matches: "",
			},
			Resources: controller.ResourceMap{
				Overrides: map[string]controller.ResourceOverride{
					"namespace": {Resource: "namespace"},
				},
			},
			SeriesQuery: metric.External.Metric.Name,
		})
	}

	configMap, err := ac.cmLister.ConfigMaps(namespace).Get(name)
	if errors.IsNotFound(err) {
		klog.V(2).InfoS("configmap has been deleted", "configmap", klog.KRef(namespace, name))
		return nil
	}
	if err != nil {
		return err
	}
	// 深拷贝，避免缓存被修改
	cm := configMap.DeepCopy()
	if cm.DeletionTimestamp != nil {
		return nil
	}

	var cfg controller.PrometheusAdapterConfig
	if err = yaml.Unmarshal([]byte(cm.Data["config.yaml"]), &cfg); err != nil {
		klog.Errorf("Failed to unmarshal adapter configmap: %v", err)
		return err
	}

	// 退出，如果 externalRules 配置未发生变化则直接退出
	if reflect.DeepEqual(cfg.ExternalRules, externalRules) {
		return nil
	}

	cfg.ExternalRules = externalRules
	newConfig, err := yaml.Marshal(&cfg)
	if err != nil {
		klog.Errorf("Failed to marshal adapter config: %v", err)
		return err
	}

	cm.Data["config.yaml"] = string(newConfig)
	_, err = ac.client.CoreV1().ConfigMaps(cm.Namespace).Update(context.TODO(), cm, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	return ac.notifyAdapter()
}

func (ac *AutoscalerController) notifyAdapter() error {
	// TODO: 抽成变量
	ns := "pixiu-system"

	deployment, err := ac.client.AppsV1().Deployments(ns).Get(context.TODO(), controller.DesireConfigMapName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get prometheus-adapter: %v", err)
	}
	patchPayloadTemplate :=
		`[{
        "op": "%s",
        "path": "/spec/template/metadata/annotations",
        "value": %s
    }]`
	op := "replace"
	tplAnnotations := deployment.Spec.Template.Annotations
	if len(tplAnnotations) == 0 {
		tplAnnotations = map[string]string{}
		op = "add"
	}

	tplAnnotations["deployment.pixiu.io/restartAt"] = metav1.Now().Format("2006-01-02T15:04:05Z")
	raw, err := json.Marshal(tplAnnotations)
	if err != nil {
		return err
	}
	patchPayload := fmt.Sprintf(patchPayloadTemplate, op, raw)
	if _, err = ac.client.AppsV1().Deployments(ns).Patch(context.Background(), controller.DesireConfigMapName, types.JSONPatchType, []byte(patchPayload), metav1.PatchOptions{}); err != nil {
		klog.Errorf("failed to patch deployment: %v", err)
		return err
	}

	return err
}

// syncAutoscaler will sync the autoscaler with the given key.
// This function is not meant to be invoked concurrently with the same key.
func (ac *AutoscalerController) syncAutoscalers(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		klog.ErrorS(err, "Failed to split meta namespace cache key", "cacheKey", key)
		return err
	}

	startTime := time.Now()
	klog.V(4).InfoS("Started syncing pixiu autoscaler", "pixiu-autoscaler", "startTime", startTime)
	defer func() {
		klog.V(4).InfoS("Finished syncing pixiu autoscaler", "pixiu-autoscaler", "duration", time.Since(startTime))
	}()

	deployment, err := ac.dLister.Deployments(namespace).Get(name)
	if errors.IsNotFound(err) {
		klog.V(2).InfoS("Deployment has been deleted", "deployment", klog.KRef(namespace, name))
		return nil
	}
	if err != nil {
		return err
	}

	// 深拷贝，避免缓存被修改
	d := deployment.DeepCopy()
	if d.DeletionTimestamp != nil {
		return nil
	}

	hpaList, err := ac.hpaLister.HorizontalPodAutoscalers(namespace).List(labels.Everything())
	if err != nil {
		return err
	}
	c := classifyHPAs(d, controller.HPANameForDeployment(d), hpaList)

	// 同名 Deployment 删除重建后，旧 UID 的 HPA 可能尚未被垃圾回收。
	// 不立即接管、不删除、也不新建（期望名称可能仍被旧对象占用），
	// 以独立的等待状态短期重新入队，不消耗配置失败的重试计数。
	if ac.IsDeploymentControlHPA(d) && len(c.managed) == 0 && len(c.orphans) > 0 {
		ac.emitWarningEventDeduped(key, reasonOrphanedHPA, d,
			fmt.Sprintf("Waiting for garbage collection of HPA(s) %s owned by the previous incarnation of Deployment %s/%s",
				hpaNames(c.orphans), namespace, name))
		ac.queue.AddAfter(key, orphanRequeueDelay)
		return nil
	}

	return ac.sync(d, c)
}

// hpaClassification 按与 Deployment 的关系对命名空间内的 HPA 分类，
// 用于明确管理边界：只有 managed 属于当前授权范围。
type hpaClassification struct {
	// managed 由当前 Deployment 托管（controllerRef 的 UID/Kind/Name 完全匹配）
	managed []*autoscalingv2.HorizontalPodAutoscaler
	// orphans 属于同名旧 Deployment（controllerRef 的 Kind/Name 相同但 UID 不同），
	// 等待垃圾回收，不接管、不修改、不删除
	orphans []*autoscalingv2.HorizontalPodAutoscaler
	// conflicts 非托管但与本 Deployment 冲突：占用了期望 HPA 名称，
	// 或 ScaleTargetRef 的 apiVersion/kind/name 指向同一目标
	conflicts []*autoscalingv2.HorizontalPodAutoscaler
}

// classifyHPAs 将命名空间内的 HPA 按归属分类。所有权判定的唯一依据是
// controller ownerReference 的 UID/Kind/Name，不使用 managedFields 或标签猜测。
func classifyHPAs(d *appsv1.Deployment, expectedName string, hpaList []*autoscalingv2.HorizontalPodAutoscaler) hpaClassification {
	var c hpaClassification
	for _, hpa := range hpaList {
		if isManagedByDeployment(hpa, d) {
			c.managed = append(c.managed, hpa)
			continue
		}
		if ref := metav1.GetControllerOf(hpa); ref != nil &&
			ref.Kind == controller.Deployment && ref.Name == d.Name {
			// UID 不同：同名 Deployment 删除重建后遗留的旧 HPA
			c.orphans = append(c.orphans, hpa)
			continue
		}
		if hpa.Name == expectedName || targetsDeployment(hpa, d) {
			c.conflicts = append(c.conflicts, hpa)
		}
	}
	return c
}

// isManagedByDeployment 判断 HPA 是否由该 Deployment 托管。
// 这是唯一所有权判定依据：controller ownerReference 的 UID、Kind、名称完全匹配。
func isManagedByDeployment(hpa *autoscalingv2.HorizontalPodAutoscaler, d *appsv1.Deployment) bool {
	ref := metav1.GetControllerOf(hpa)
	return ref != nil &&
		ref.UID == d.UID &&
		ref.Kind == controller.Deployment &&
		ref.Name == d.Name
}

// targetsDeployment 判断 HPA 的 ScaleTargetRef 是否指向该 Deployment。
func targetsDeployment(hpa *autoscalingv2.HorizontalPodAutoscaler, d *appsv1.Deployment) bool {
	ref := hpa.Spec.ScaleTargetRef
	return ref.APIVersion == controller.AppsAPIVersion &&
		ref.Kind == controller.Deployment &&
		ref.Name == d.Name
}

func hpaNames(hpas []*autoscalingv2.HorizontalPodAutoscaler) string {
	names := make([]string, 0, len(hpas))
	for _, hpa := range hpas {
		names = append(names, hpa.Name)
	}
	return "[" + strings.Join(names, " ") + "]"
}

// emitWarningEventDeduped 发出 Warning 事件；相同 (key, reason, message) 的事件
// 在 eventDedupInterval 内只发一次，避免同一冲突在每次同步时反复刷事件。
func (ac *AutoscalerController) emitWarningEventDeduped(key, reason string, obj runtime.Object, message string) {
	ac.eventLock.Lock()
	defer ac.eventLock.Unlock()
	if ac.lastEventAt == nil {
		ac.lastEventAt = make(map[string]time.Time)
	}
	eventKey := key + "|" + reason + "|" + message
	if last, ok := ac.lastEventAt[eventKey]; ok && time.Since(last) < eventDedupInterval {
		return
	}
	ac.lastEventAt[eventKey] = time.Now()
	ac.eventRecorder.Event(obj, v1.EventTypeWarning, reason, message)
}

// sortManagedHPAs 对托管 HPA 确定性排序：期望名称优先，其次创建时间早的优先，
// 仍相同再按名称字典序。排序后第一个为保留对象，其余删除。
func sortManagedHPAs(hpas []*autoscalingv2.HorizontalPodAutoscaler, expectedName string) {
	sort.SliceStable(hpas, func(i, j int) bool {
		iExpected := hpas[i].Name == expectedName
		jExpected := hpas[j].Name == expectedName
		if iExpected != jExpected {
			return iExpected
		}
		ti, tj := hpas[i].CreationTimestamp, hpas[j].CreationTimestamp
		if !ti.Equal(&tj) {
			return ti.Before(&tj)
		}
		return hpas[i].Name < hpas[j].Name
	})
}

func (ac *AutoscalerController) sync(d *appsv1.Deployment, c hpaClassification) error {
	// 1. deployment 存在，但是 hpa 注释不存在 => 移除已存在的 hpa。
	//    仅删除当前授权范围内（本 Deployment UID 托管）的 HPA，非托管对象始终保留。
	if !ac.IsDeploymentControlHPA(d) {
		return ac.deleteHPAsInBatch(d, c.managed)
	}

	newHPA, err := controller.CreateHPAFromDeployment(d)
	if err != nil {
		ac.eventRecorder.Eventf(d, v1.EventTypeWarning, reasonFailedNewestHPA, fmt.Sprintf("Failed extract newest HPA %s/%s", d.GetNamespace(), d.GetName()))
		return err
	}
	customMetric := ac.IsCustomMetricHPA(d)
	if customMetric {
		newHPA.Labels[controller.PrometheusCustomMetric] = "true"
	}

	if len(c.managed) == 0 {
		// 存在冲突的非托管 HPA：不接管、不修改、不删除，也不创建，
		// 避免出现第二个伸缩控制器。记录名称冲突后继续。
		if len(c.conflicts) > 0 {
			ac.emitWarningEventDeduped(d.Namespace+"/"+d.Name, reasonHPAConflict, d,
				fmt.Sprintf("HPA(s) %s conflict with Deployment %s/%s (same name or same scale target); skip creating HPA %s to avoid duplicate autoscalers",
					hpaNames(c.conflicts), d.Namespace, d.Name, newHPA.Name))
			return nil
		}
		// 新建
		_, err = ac.client.AutoscalingV2().HorizontalPodAutoscalers(newHPA.Namespace).Create(context.TODO(), newHPA, metav1.CreateOptions{})
		if err != nil {
			ac.eventRecorder.Eventf(newHPA, v1.EventTypeWarning, reasonFailedCreateHPA, fmt.Sprintf("Failed to create HPA %s/%s: %v", newHPA.Namespace, newHPA.Name, err))
			return err
		}
		ac.eventRecorder.Eventf(newHPA, v1.EventTypeNormal, reasonCreateHPA, fmt.Sprintf("Create HPA %s/%s success", newHPA.Namespace, newHPA.Name))
	} else {
		// 多个托管 HPA：确定性保留一个（期望名称优先，其次创建时间，再按名称），其余删除
		sortManagedHPAs(c.managed, newHPA.Name)
		kept := c.managed[0]
		if err := ac.deleteHPAsInBatch(d, c.managed[1:]); err != nil {
			return err
		}

		// 仅更新约定由控制器管理的字段（见 controller.ManagedHPAFields），
		// 用户的 Spec.Behavior、无关注解与标签原样保留
		updated, err := ac.updateManagedHPA(d, kept, newHPA, customMetric)
		if err != nil {
			ac.eventRecorder.Eventf(newHPA, v1.EventTypeWarning, reasonFailedUpdateHPA, fmt.Sprintf("Failed to update HPA %s/%s: %v", newHPA.Namespace, newHPA.Name, err))
			klog.Errorf("Failed to update HPA %s/%s %v", newHPA.Namespace, newHPA.Name, err)
			return err
		}
		if !updated {
			klog.V(2).Infof("HPA: %s/%s is not changed", newHPA.Namespace, newHPA.Name)
			return nil
		}
		ac.eventRecorder.Eventf(newHPA, v1.EventTypeNormal, reasonUpdateHPA, fmt.Sprintf("Update HPA %s/%s success", newHPA.Namespace, newHPA.Name))
	}

	return ac.Notify(d)
}

// updateManagedHPA 仅将控制器管理的字段写入 kept 所指的 HPA。
// 更新携带正确的 resourceVersion；遇到冲突时重新读取、重新判断归属，
// 并在有界次数内合并重试，不盲目重放旧对象。返回是否真正发生了更新。
func (ac *AutoscalerController) updateManagedHPA(d *appsv1.Deployment, kept, desired *autoscalingv2.HorizontalPodAutoscaler, customMetric bool) (bool, error) {
	current := kept
	for attempt := 0; attempt < maxUpdateConflictRetries; attempt++ {
		merged := current.DeepCopy()
		if !controller.ApplyManagedHPAFields(merged, desired, customMetric) {
			return false, nil
		}
		if _, err := ac.client.AutoscalingV2().HorizontalPodAutoscalers(merged.Namespace).Update(context.TODO(), merged, metav1.UpdateOptions{}); err != nil {
			if errors.IsNotFound(err) {
				// 已被删除，下次同步按需重建
				return false, nil
			}
			if !errors.IsConflict(err) {
				return false, err
			}
			// 冲突：重新读取最新对象并重新确认归属后再合并
			fresh, getErr := ac.client.AutoscalingV2().HorizontalPodAutoscalers(kept.Namespace).Get(context.TODO(), kept.Name, metav1.GetOptions{})
			if getErr != nil {
				return false, getErr
			}
			if !isManagedByDeployment(fresh, d) {
				return false, fmt.Errorf("HPA %s/%s is no longer managed by Deployment %s/%s, abort update",
					kept.Namespace, kept.Name, d.Namespace, d.Name)
			}
			current = fresh
			continue
		}
		return true, nil
	}
	return false, fmt.Errorf("failed to update HPA %s/%s: still conflicting after %d retries",
		kept.Namespace, kept.Name, maxUpdateConflictRetries)
}

func (ac *AutoscalerController) Notify(d *appsv1.Deployment) error {
	if !ac.IsCustomMetricHPA(d) {
		return nil
	}

	// TODO: 抽成变量
	ns := "pixiu-system"
	cm, err := ac.cmLister.ConfigMaps(ns).Get(controller.DesireConfigMapName)
	if err != nil {
		return err
	}
	patchPayloadTemplate :=
		`[{
        "op": "%s",
        "path": "/metadata/annotations",
        "value": %s
    }]`
	op := "replace"
	if len(cm.Annotations) == 0 {
		cm.Annotations = map[string]string{}
		op = "add"
	}

	cm.Annotations[controller.NotifyAt] = time.Now().Format("2006-01-02T15:04:05Z")
	raw, err := json.Marshal(cm.Annotations)
	if err != nil {
		return err
	}
	patchPayload := fmt.Sprintf(patchPayloadTemplate, op, raw)
	if _, err = ac.client.CoreV1().ConfigMaps(ns).Patch(context.Background(), controller.DesireConfigMapName, types.JSONPatchType, []byte(patchPayload), metav1.PatchOptions{}); err != nil {
		klog.Errorf("failed to patch configmap: %v", err)
		return err
	}

	return nil
}

// deleteHPAsInBatch 删除当前授权范围内（本 Deployment UID 托管）的 HPA。
// 删除前再次确认归属，非托管对象一律保留。
func (ac *AutoscalerController) deleteHPAsInBatch(d *appsv1.Deployment, hpaList []*autoscalingv2.HorizontalPodAutoscaler) error {
	if len(hpaList) == 0 {
		return nil
	}
	for _, hpa := range hpaList {
		if !isManagedByDeployment(hpa, d) {
			klog.Warningf("Skip deleting HPA %s/%s: not managed by Deployment %s/%s", hpa.Namespace, hpa.Name, d.Namespace, d.Name)
			continue
		}
		if err := ac.client.AutoscalingV2().HorizontalPodAutoscalers(hpa.Namespace).Delete(context.TODO(), hpa.Name, metav1.DeleteOptions{}); err != nil {
			if !errors.IsNotFound(err) {
				ac.eventRecorder.Eventf(hpa, v1.EventTypeWarning, reasonFailedDeleteHPA, fmt.Sprintf("Failed to delete HPA %s/%s", hpa.Namespace, hpa.Name))
				return err
			}
		}
		ac.eventRecorder.Eventf(hpa, v1.EventTypeNormal, reasonDeleteHPA, fmt.Sprintf("Delete HPA %s/%s", hpa.Namespace, hpa.Name))
	}

	return nil
}

// getHPAsForDeployment 返回由该 Deployment 托管的 HPA 列表，
// 通过 controllerRef 的 UID、Kind、名称识别。
func (ac *AutoscalerController) getHPAsForDeployment(d *appsv1.Deployment) ([]*autoscalingv2.HorizontalPodAutoscaler, error) {
	hpaList, err := ac.hpaLister.HorizontalPodAutoscalers(d.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var wanted []*autoscalingv2.HorizontalPodAutoscaler
	for _, hpa := range hpaList {
		if isManagedByDeployment(hpa, d) {
			wanted = append(wanted, hpa)
		}
	}

	return wanted, nil
}

func (ac *AutoscalerController) enqueue(deployment *appsv1.Deployment) {
	key, err := controller.KeyFunc(deployment)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", deployment, err))
		return
	}

	ac.queue.Add(key)
}

func (ac *AutoscalerController) enqueueCM(cm *corev1.ConfigMap) {
	key, err := controller.KeyFunc(cm)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", cm, err))
		return
	}

	ac.cmQueue.Add(key)
}

// worker runs a worker thread that just dequeues items, processes then, and marks them done.
func (ac *AutoscalerController) worker() {
	for ac.processNextWorkItem() {
	}
}

func (ac *AutoscalerController) configMapWorker() {
	for ac.processNextConfigMapWorkItem() {
	}
}

func (ac *AutoscalerController) processNextWorkItem() bool {
	key, quit := ac.queue.Get()
	if quit {
		return false
	}
	defer ac.queue.Done(key)

	err := ac.syncHandler(key.(string))
	ac.handleErr(err, key)
	return true
}

func (ac *AutoscalerController) processNextConfigMapWorkItem() bool {
	key, quit := ac.cmQueue.Get()
	if quit {
		return false
	}
	defer ac.cmQueue.Done(key)

	err := ac.syncConfigMapHandler(key.(string))
	ac.handleConfigMapErr(err, key)
	return true
}

func (ac *AutoscalerController) handleErr(err error, key interface{}) {
	if err == nil {
		ac.queue.Forget(key)
		return
	}

	if ac.queue.NumRequeues(key) < maxRetries {
		klog.V(0).Infof("Error syncing HPA %v: %v", key, err)
		ac.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	klog.V(0).Infof("Dropping HPA %q out of the queue: %v", key, err)
	ac.queue.Forget(key)
}

func (ac *AutoscalerController) handleConfigMapErr(err error, key interface{}) {
	if err == nil {
		ac.cmQueue.Forget(key)
		return
	}

	if ac.cmQueue.NumRequeues(key) < maxRetries {
		klog.V(0).Infof("Error syncing HPA %v: %v", key, err)
		ac.cmQueue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	klog.V(0).Infof("Dropping HPA %q out of the queue: %v", key, err)
	ac.cmQueue.Forget(key)
}

// This functions just wrap Handler Deployment Events for improve the readability of codes
func (ac *AutoscalerController) addDeployment(obj interface{}) {
	d := obj.(*appsv1.Deployment)
	klog.V(4).InfoS("Adding deployment", "deployment", klog.KObj(d))
	ac.enqueueDeployment(d)
}

func (ac *AutoscalerController) updateDeployment(old, cur interface{}) {
	oldD := old.(*appsv1.Deployment)
	curD := cur.(*appsv1.Deployment)

	// Two different versions of the same HPA will always have different ResourceVersions.
	if oldD.ResourceVersion == curD.ResourceVersion {
		return
	}
	// deployment 的注释未变化，则HPA不变
	if reflect.DeepEqual(oldD.Annotations, curD.Annotations) {
		return
	}
	klog.V(4).InfoS("Updating deployment", "deployment", klog.KObj(oldD))

	ac.enqueueDeployment(curD)
}

func (ac *AutoscalerController) deleteDeployment(obj interface{}) {
	d, ok := obj.(*appsv1.Deployment)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
			return
		}
		d, ok = tombstone.Obj.(*appsv1.Deployment)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a Deployment %#v", obj))
			return
		}
	}
	klog.V(4).InfoS("Deleting deployment", "deployment", klog.KObj(d))
	ac.enqueueDeployment(d)
}

func (ac *AutoscalerController) addHPA(obj interface{}) {
	hpa := obj.(*autoscalingv2.HorizontalPodAutoscaler)

	if hpa.DeletionTimestamp != nil {
		// 重启控制器会中断原先删除过程，存在删除标识则继续删除
		ac.deleteHPA(hpa)
		return
	}

	// 如果存在 OwnerReference， 则直接获取上级资源
	if controllerRef := metav1.GetControllerOf(hpa); controllerRef != nil {
		d := ac.resolveControllerRef(hpa.Namespace, controllerRef)
		if d == nil {
			return
		}
		klog.V(4).InfoS("HPA added", "hpa", klog.KObj(hpa))
		ac.enqueueDeployment(d)
		return
	}
}

// updateHPA figures out what HPA(s) is updated and wake them up. old and cur must be *autoscalingv2.HorizontalPodAutoscaler types.
func (ac *AutoscalerController) updateHPA(old, cur interface{}) {
	oldHPA := old.(*autoscalingv2.HorizontalPodAutoscaler)
	curHPA := cur.(*autoscalingv2.HorizontalPodAutoscaler)

	// Two different versions of the same HPA will always have different ResourceVersions.
	if oldHPA.ResourceVersion == curHPA.ResourceVersion {
		return
	}

	curControllerRef := metav1.GetControllerOf(curHPA)
	oldControllerRef := metav1.GetControllerOf(oldHPA)
	controllerRefChanged := !reflect.DeepEqual(curControllerRef, oldControllerRef)
	if controllerRefChanged && oldControllerRef != nil {
		// hpa 的 ControllerRef 发生了变化，同步老的 controller
		if d := ac.resolveControllerRef(oldHPA.Namespace, oldControllerRef); d != nil {
			ac.enqueueDeployment(d)
		}
	}

	if curControllerRef != nil {
		if d := ac.resolveControllerRef(curHPA.Namespace, curControllerRef); d != nil {
			ac.enqueueDeployment(d)
		}
	}
}

func (ac *AutoscalerController) deleteHPA(obj interface{}) {
	hpa, ok := obj.(*autoscalingv2.HorizontalPodAutoscaler)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
			return
		}
		hpa, ok = tombstone.Obj.(*autoscalingv2.HorizontalPodAutoscaler)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a HorizontalPodAutoscaler %#v", obj))
			return
		}
	}

	controllerRef := metav1.GetControllerOf(hpa)
	if controllerRef == nil {
		return
	}
	d := ac.resolveControllerRef(hpa.Namespace, controllerRef)
	if d == nil {
		return
	}
	klog.V(0).Infof("Deleting HPA %s/%s", hpa.Namespace, hpa.Name)
	ac.enqueueDeployment(d)
}

func (ac *AutoscalerController) addConfigMap(obj interface{}) {
	cm := obj.(*corev1.ConfigMap)
	klog.V(4).InfoS("Adding configmap", "configmap", klog.KObj(cm))
	ac.enqueueConfigMap(cm)
}

func (ac *AutoscalerController) updateConfigMap(old, cur interface{}) {
	oldCM := old.(*corev1.ConfigMap)
	curCM := cur.(*corev1.ConfigMap)

	if oldCM.ResourceVersion == curCM.ResourceVersion {
		return
	}
	//if reflect.DeepEqual(oldCM.Data[controller.ExternalRuleKey], curCM.Data[controller.ExternalRuleKey]) {
	//	return
	//}
	//klog.V(4).InfoS("Updating configmap", "configmap", klog.KObj(oldCM))

	ac.enqueueConfigMap(curCM)
}

func (ac *AutoscalerController) deleteConfigMap(obj interface{}) {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
			return
		}
		cm, ok = tombstone.Obj.(*corev1.ConfigMap)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a ConfigMap %#v", obj))
			return
		}
	}
	klog.V(4).InfoS("Adding configmap", "configmap", klog.KObj(cm))
	ac.enqueueConfigMap(cm)
}

func (ac *AutoscalerController) resolveControllerRef(namespace string, controllerRef *metav1.OwnerReference) *appsv1.Deployment {
	if controllerRef.Kind != controller.Deployment {
		return nil
	}
	d, err := ac.dLister.Deployments(namespace).Get(controllerRef.Name)
	if err != nil {
		return nil
	}
	if d.UID != controllerRef.UID {
		return nil
	}
	return d
}
