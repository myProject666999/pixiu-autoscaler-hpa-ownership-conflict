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
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	utilpointer "k8s.io/utils/pointer"

	"github.com/caoyingjunz/pixiu-autoscaler/pkg/controller"
)

const (
	testNamespace = "default"
	testDeploy    = "web"
)

type fixture struct {
	t        *testing.T
	client   *fake.Clientset
	ac       *AutoscalerController
	recorder *record.FakeRecorder
	dStore   cache.Store
	hpaStore cache.Store
}

func newFixture(t *testing.T, objects ...runtime.Object) *fixture {
	t.Helper()
	client := fake.NewSimpleClientset(objects...)
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	dInformer := informerFactory.Apps().V1().Deployments()
	hpaInformer := informerFactory.Autoscaling().V2().HorizontalPodAutoscalers()
	cmInformer := informerFactory.Core().V1().ConfigMaps()

	recorder := record.NewFakeRecorder(100)
	ac := &AutoscalerController{
		client:        client,
		eventRecorder: recorder,
		dLister:       dInformer.Lister(),
		hpaLister:     hpaInformer.Lister(),
		cmLister:      cmInformer.Lister(),
		queue:         workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test"),
		cmQueue:       workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test-cm"),
		items:         controller.NewItems(),
	}
	return &fixture{
		t:        t,
		client:   client,
		ac:       ac,
		recorder: recorder,
		dStore:   dInformer.Informer().GetStore(),
		hpaStore: hpaInformer.Informer().GetStore(),
	}
}

func hpaAnnotations(min, max string) map[string]string {
	return map[string]string{
		"cpu.hpa.caoyingjunz.io/targetAverageUtilization": "60",
		controller.MinReplicas:                            min,
		controller.MaxReplicas:                            max,
	}
}

func newDeployment(uid types.UID, annotations map[string]string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   testNamespace,
			Name:        testDeploy,
			UID:         uid,
			Annotations: annotations,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": testDeploy}},
		},
	}
}

func ownerRef(d *appsv1.Deployment, uid types.UID) metav1.OwnerReference {
	truth := true
	return metav1.OwnerReference{
		APIVersion:         controller.AppsAPIVersion,
		Kind:               controller.Deployment,
		Name:               d.Name,
		UID:                uid,
		Controller:         &truth,
		BlockOwnerDeletion: &truth,
	}
}

// newManagedHPA 构造一个归属 d（当前 UID）的托管 HPA。
// Spec 固定为 min=1/max=6/cpu=60（与 hpaAnnotations("1","6") 的期望一致），
// 与 d 的注解无关，因此可通过调整注解模拟“需要更新”的场景。
func newManagedHPA(d *appsv1.Deployment, name string, created time.Time) *autoscalingv2.HorizontalPodAutoscaler {
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         d.Namespace,
			Name:              name,
			ResourceVersion:   "100",
			CreationTimestamp: metav1.NewTime(created),
			Labels:            map[string]string{"app": d.Name},
			OwnerReferences:   []metav1.OwnerReference{ownerRef(d, d.UID)},
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			MinReplicas: utilpointer.Int32Ptr(1),
			MaxReplicas: 6,
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: controller.AppsAPIVersion,
				Kind:       controller.Deployment,
				Name:       d.Name,
			},
			Metrics: []autoscalingv2.MetricSpec{
				{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name: "cpu",
						Target: autoscalingv2.MetricTarget{
							Type:               autoscalingv2.UtilizationMetricType,
							AverageUtilization: utilpointer.Int32Ptr(60),
						},
					},
				},
			},
		},
	}
}

// newForeignHPA 构造一个非托管（无 ownerReference）的用户 HPA。
func newForeignHPA(name string, targetName string) *autoscalingv2.HorizontalPodAutoscaler {
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       testNamespace,
			Name:            name,
			ResourceVersion: "100",
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			MinReplicas: utilpointer.Int32Ptr(1),
			MaxReplicas: 4,
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: controller.AppsAPIVersion,
				Kind:       controller.Deployment,
				Name:       targetName,
			},
		},
	}
}

// hpaActions 返回 fake clientset 记录到的 HPA 写操作（create/update/delete）。
func (f *fixture) hpaWriteActions() []core.Action {
	var acts []core.Action
	for _, a := range f.client.Actions() {
		if a.GetResource().Resource != "horizontalpodautoscalers" {
			continue
		}
		switch a.GetVerb() {
		case "create", "update", "delete":
			acts = append(acts, a)
		}
	}
	return acts
}

func (f *fixture) drainEvents() []string {
	var events []string
	for {
		select {
		case e := <-f.recorder.Events:
			events = append(events, e)
		default:
			return events
		}
	}
}

func (f *fixture) runSync(d *appsv1.Deployment) error {
	return f.ac.syncAutoscalers(d.Namespace + "/" + d.Name)
}

func actionNames(acts []core.Action) []string {
	var names []string
	for _, a := range acts {
		switch act := a.(type) {
		case core.CreateAction:
			names = append(names, a.GetVerb()+":"+act.GetObject().(*autoscalingv2.HorizontalPodAutoscaler).Name)
		case core.UpdateAction:
			names = append(names, a.GetVerb()+":"+act.GetObject().(*autoscalingv2.HorizontalPodAutoscaler).Name)
		case core.DeleteAction:
			names = append(names, a.GetVerb()+":"+act.GetName())
		}
	}
	return names
}

// 无冲突创建：没有任何现存 HPA 时，按注解创建期望名称的 HPA。
func TestSyncCreatesHPAWhenNoneExists(t *testing.T) {
	f := newFixture(t)
	d := newDeployment("uid-1", hpaAnnotations("1", "6"))
	if err := f.dStore.Add(d); err != nil {
		t.Fatal(err)
	}

	if err := f.runSync(d); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	acts := f.hpaWriteActions()
	if len(acts) != 1 || acts[0].GetVerb() != "create" {
		t.Fatalf("expected exactly one create action, got %v", actionNames(acts))
	}
	created := acts[0].(core.CreateAction).GetObject().(*autoscalingv2.HorizontalPodAutoscaler)
	if want := controller.HPANameForDeployment(d); created.Name != want {
		t.Errorf("expected HPA name %q, got %q", want, created.Name)
	}
	ref := metav1.GetControllerOf(created)
	if ref == nil || ref.UID != d.UID || ref.Kind != controller.Deployment || ref.Name != d.Name {
		t.Errorf("created HPA has wrong controller reference: %+v", ref)
	}
	if created.Spec.MaxReplicas != 6 {
		t.Errorf("expected maxReplicas 6, got %d", created.Spec.MaxReplicas)
	}

	events := f.drainEvents()
	if len(events) != 1 || !strings.Contains(events[0], "Normal "+reasonCreateHPA) {
		t.Errorf("expected one CreateHPA event, got %v", events)
	}
}

// 同名外来对象：用户自建的 HPA 占用了期望名称，控制器不接管、不修改、不创建，
// 发出含冲突对象名的警告事件，且同一冲突不反复刷事件；用户删除后下次同步才创建。
func TestSyncSameNameForeignHPABlocksCreate(t *testing.T) {
	d := newDeployment("uid-1", hpaAnnotations("1", "6"))
	expectedName := controller.HPANameForDeployment(d)
	foreign := newForeignHPA(expectedName, testDeploy)

	// 外来对象只存在于 informer 缓存；控制器不应对其产生任何写操作
	f := newFixture(t)
	if err := f.dStore.Add(d); err != nil {
		t.Fatal(err)
	}
	if err := f.hpaStore.Add(foreign); err != nil {
		t.Fatal(err)
	}

	if err := f.runSync(d); err != nil {
		t.Fatalf("sync failed: %v", err)
	}
	if acts := f.hpaWriteActions(); len(acts) != 0 {
		t.Fatalf("expected no write actions on foreign HPA, got %v", actionNames(acts))
	}
	events := f.drainEvents()
	if len(events) != 1 || !strings.Contains(events[0], "Warning "+reasonHPAConflict) ||
		!strings.Contains(events[0], expectedName) {
		t.Fatalf("expected one HPAConflict event naming %q, got %v", expectedName, events)
	}

	// 同一冲突再次同步不反复刷事件
	if err := f.runSync(d); err != nil {
		t.Fatalf("sync failed: %v", err)
	}
	if events := f.drainEvents(); len(events) != 0 {
		t.Fatalf("expected conflict event to be deduplicated, got %v", events)
	}

	// 用户删除冲突对象后，下一次同步才创建
	if err := f.hpaStore.Delete(foreign); err != nil {
		t.Fatal(err)
	}
	if err := f.runSync(d); err != nil {
		t.Fatalf("sync failed: %v", err)
	}
	acts := f.hpaWriteActions()
	if len(acts) != 1 || acts[0].GetVerb() != "create" {
		t.Fatalf("expected create after foreign HPA removed, got %v", actionNames(acts))
	}
}

// 异名同目标：名字不同但 ScaleTargetRef 指向同一 Deployment 的非托管 HPA
// 必须被识别为冲突，避免新增第二个伸缩控制器。
func TestSyncDifferentNameSameTargetBlocksCreate(t *testing.T) {
	d := newDeployment("uid-1", hpaAnnotations("1", "6"))
	foreign := newForeignHPA("web-hpa", testDeploy)

	f := newFixture(t, foreign)
	if err := f.dStore.Add(d); err != nil {
		t.Fatal(err)
	}
	if err := f.hpaStore.Add(foreign); err != nil {
		t.Fatal(err)
	}

	if err := f.runSync(d); err != nil {
		t.Fatalf("sync failed: %v", err)
	}
	if acts := f.hpaWriteActions(); len(acts) != 0 {
		t.Fatalf("expected no write actions, got %v", actionNames(acts))
	}
	events := f.drainEvents()
	if len(events) != 1 || !strings.Contains(events[0], "Warning "+reasonHPAConflict) ||
		!strings.Contains(events[0], "web-hpa") {
		t.Fatalf("expected HPAConflict event naming web-hpa, got %v", events)
	}
}

// 托管 HPA 被用户加上 Behavior 后，上限从 6 改为 8：
// 只改变受管的上限等字段，Behavior、用户注解与标签原样保留。
func TestSyncUpdatePreservesBehaviorAndUserMetadata(t *testing.T) {
	f := newFixture(t)
	d := newDeployment("uid-1", hpaAnnotations("1", "8"))
	expectedName := controller.HPANameForDeployment(d)
	managed := newManagedHPA(d, expectedName, time.Now())
	managed.Spec.Behavior = &autoscalingv2.HorizontalPodAutoscalerBehavior{
		ScaleDown: &autoscalingv2.HPAScalingRules{
			StabilizationWindowSeconds: utilpointer.Int32Ptr(300),
		},
	}
	managed.Annotations = map[string]string{"user/key": "keep-me"}
	managed.Labels["user-label"] = "keep-me-too"

	if err := f.dStore.Add(d); err != nil {
		t.Fatal(err)
	}
	if err := f.hpaStore.Add(managed); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).Create(
		context.TODO(), managed.DeepCopy(), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.client.ClearActions()

	if err := f.runSync(d); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	acts := f.hpaWriteActions()
	if len(acts) != 1 || acts[0].GetVerb() != "update" {
		t.Fatalf("expected exactly one update action, got %v", actionNames(acts))
	}
	updated := acts[0].(core.UpdateAction).GetObject().(*autoscalingv2.HorizontalPodAutoscaler)
	if updated.Spec.MaxReplicas != 8 {
		t.Errorf("expected maxReplicas updated to 8, got %d", updated.Spec.MaxReplicas)
	}
	if updated.Spec.Behavior == nil ||
		updated.Spec.Behavior.ScaleDown == nil ||
		*updated.Spec.Behavior.ScaleDown.StabilizationWindowSeconds != 300 {
		t.Errorf("user-provided Spec.Behavior was not preserved: %+v", updated.Spec.Behavior)
	}
	if updated.Annotations["user/key"] != "keep-me" {
		t.Errorf("user annotation was erased: %v", updated.Annotations)
	}
	if updated.Labels["user-label"] != "keep-me-too" {
		t.Errorf("user label was erased: %v", updated.Labels)
	}
	if updated.ResourceVersion != "100" {
		t.Errorf("update must carry the resourceVersion of the kept object, got %q", updated.ResourceVersion)
	}

	events := f.drainEvents()
	if len(events) != 1 || !strings.Contains(events[0], "Normal "+reasonUpdateHPA) {
		t.Errorf("expected one UpdateHPA event, got %v", events)
	}
}

// 多个托管对象：确定性保留（期望名称优先，其次创建时间，再按名称），其余删除。
func TestSyncMultipleManagedHPAsKeepsDeterministic(t *testing.T) {
	f := newFixture(t)
	d := newDeployment("uid-1", hpaAnnotations("1", "6"))
	expectedName := controller.HPANameForDeployment(d)
	base := time.Now()

	// 期望名称的对象创建时间最晚，仍应被保留
	expected := newManagedHPA(d, expectedName, base.Add(2*time.Hour))
	older := newManagedHPA(d, "zzz-extra", base)
	oldest := newManagedHPA(d, "aaa-extra", base.Add(-time.Hour))

	for _, h := range []*autoscalingv2.HorizontalPodAutoscaler{oldest, older, expected} {
		if err := f.hpaStore.Add(h); err != nil {
			t.Fatal(err)
		}
		if _, err := f.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).Create(
			context.TODO(), h.DeepCopy(), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.dStore.Add(d); err != nil {
		t.Fatal(err)
	}
	f.client.ClearActions()

	if err := f.runSync(d); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	acts := f.hpaWriteActions()
	// 保留期望名称对象（Spec 已一致，无更新），按创建时间顺序删除 aaa-extra、zzz-extra
	want := []string{"delete:aaa-extra", "delete:zzz-extra"}
	if got := actionNames(acts); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("expected deletes %v, got %v", want, got)
	}

	// 无期望名称时：保留创建时间最早的
	f2 := newFixture(t)
	h1 := newManagedHPA(d, "bbb", base.Add(time.Hour))
	h2 := newManagedHPA(d, "aaa", base.Add(time.Hour)) // 同时刻按名称排序，aaa 保留
	for _, h := range []*autoscalingv2.HorizontalPodAutoscaler{h1, h2} {
		if err := f2.hpaStore.Add(h); err != nil {
			t.Fatal(err)
		}
		if _, err := f2.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).Create(
			context.TODO(), h.DeepCopy(), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := f2.dStore.Add(d); err != nil {
		t.Fatal(err)
	}
	f2.client.ClearActions()
	if err := f2.runSync(d); err != nil {
		t.Fatalf("sync failed: %v", err)
	}
	if got := actionNames(f2.hpaWriteActions()); fmt.Sprint(got) != fmt.Sprint([]string{"delete:bbb"}) {
		t.Fatalf("expected to keep aaa and delete bbb, got %v", got)
	}
}

// UID 重建：同名 Deployment 删除重建后，旧 HPA 指向不同 UID，不能立即接管，
// 以独立等待状态短期重新入队，不消耗配置失败的重试计数。
func TestSyncOrphanedHPAWaitsForGC(t *testing.T) {
	defer func(d time.Duration) { orphanRequeueDelay = d }(orphanRequeueDelay)
	orphanRequeueDelay = 0

	f := newFixture(t)
	d := newDeployment("uid-new", hpaAnnotations("1", "6"))
	expectedName := controller.HPANameForDeployment(d)
	orphan := newManagedHPA(d, expectedName, time.Now())
	orphan.OwnerReferences = []metav1.OwnerReference{ownerRef(d, "uid-old")}

	if err := f.dStore.Add(d); err != nil {
		t.Fatal(err)
	}
	if err := f.hpaStore.Add(orphan); err != nil {
		t.Fatal(err)
	}

	if err := f.runSync(d); err != nil {
		t.Fatalf("orphan wait must not return error, got: %v", err)
	}
	if acts := f.hpaWriteActions(); len(acts) != 0 {
		t.Fatalf("expected no writes while orphan exists, got %v", actionNames(acts))
	}
	key := testNamespace + "/" + testDeploy
	if f.ac.queue.Len() != 1 {
		t.Fatalf("expected key requeued for waiting, queue len %d", f.ac.queue.Len())
	}
	if n := f.ac.queue.NumRequeues(key); n != 0 {
		t.Fatalf("orphan wait must not consume failure retries, got %d", n)
	}
	events := f.drainEvents()
	if len(events) != 1 || !strings.Contains(events[0], "Warning "+reasonOrphanedHPA) ||
		!strings.Contains(events[0], expectedName) {
		t.Fatalf("expected OrphanedHPA event naming %q, got %v", expectedName, events)
	}

	// 旧对象已被 GC 后，下一次同步正常创建
	if err := f.hpaStore.Delete(orphan); err != nil {
		t.Fatal(err)
	}
	if err := f.runSync(d); err != nil {
		t.Fatalf("sync failed: %v", err)
	}
	if acts := f.hpaWriteActions(); len(acts) != 1 || acts[0].GetVerb() != "create" {
		t.Fatalf("expected create after orphan collected, got %v", actionNames(acts))
	}
}

// 注解移除：只删除当前授权范围内的托管 HPA，非托管对象始终保留。
func TestSyncAnnotationRemovalDeletesOnlyManaged(t *testing.T) {
	f := newFixture(t)
	d := newDeployment("uid-1", nil) // 无 HPA 注解
	managed := newManagedHPA(d, controller.HPANameForDeployment(d), time.Now())
	foreign := newForeignHPA("web-hpa", testDeploy)

	for _, h := range []*autoscalingv2.HorizontalPodAutoscaler{managed, foreign} {
		if err := f.hpaStore.Add(h); err != nil {
			t.Fatal(err)
		}
		if _, err := f.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).Create(
			context.TODO(), h.DeepCopy(), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.dStore.Add(d); err != nil {
		t.Fatal(err)
	}
	f.client.ClearActions()

	if err := f.runSync(d); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	want := []string{"delete:" + managed.Name}
	if got := actionNames(f.hpaWriteActions()); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("expected only managed HPA deleted %v, got %v", want, got)
	}
	if _, err := f.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).Get(
		context.TODO(), "web-hpa", metav1.GetOptions{}); err != nil {
		t.Fatalf("foreign HPA must be preserved, got err: %v", err)
	}
}

// 更新冲突：携带正确 resourceVersion，冲突后重新读取、重新判断归属、合并重试。
func TestSyncUpdateConflictReReadsAndRetries(t *testing.T) {
	f := newFixture(t)
	d := newDeployment("uid-1", hpaAnnotations("1", "8"))
	expectedName := controller.HPANameForDeployment(d)

	// 缓存中的是旧版本（rv=100），API 上已是更新版本（rv=200，含用户 Behavior）
	cached := newManagedHPA(d, expectedName, time.Now())
	cached.ResourceVersion = "100"
	fresh := cached.DeepCopy()
	fresh.ResourceVersion = "200"
	fresh.Spec.Behavior = &autoscalingv2.HorizontalPodAutoscalerBehavior{
		ScaleUp: &autoscalingv2.HPAScalingRules{
			StabilizationWindowSeconds: utilpointer.Int32Ptr(30),
		},
	}

	if err := f.dStore.Add(d); err != nil {
		t.Fatal(err)
	}
	if err := f.hpaStore.Add(cached); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).Create(
		context.TODO(), fresh, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.client.ClearActions()

	// 模拟真实 API 服务器：旧 resourceVersion 的更新返回 Conflict
	f.client.PrependReactor("update", "horizontalpodautoscalers", func(action core.Action) (bool, runtime.Object, error) {
		hpa := action.(core.UpdateAction).GetObject().(*autoscalingv2.HorizontalPodAutoscaler)
		if hpa.ResourceVersion == "100" {
			return true, nil, apierrors.NewConflict(
				schema.GroupResource{Resource: "horizontalpodautoscalers"}, hpa.Name,
				fmt.Errorf("the object has been modified; please apply your changes to the latest version"))
		}
		return false, nil, nil
	})

	if err := f.runSync(d); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	var updates, gets int
	for _, a := range f.client.Actions() {
		if a.GetResource().Resource != "horizontalpodautoscalers" {
			continue
		}
		switch a.GetVerb() {
		case "update":
			updates++
		case "get":
			gets++
		}
	}
	if updates != 2 || gets != 1 {
		t.Fatalf("expected 2 updates and 1 re-read after conflict, got %d updates, %d gets", updates, gets)
	}

	got, err := f.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).Get(context.TODO(), expectedName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Spec.MaxReplicas != 8 {
		t.Errorf("expected maxReplicas 8 after retry, got %d", got.Spec.MaxReplicas)
	}
	if got.Spec.Behavior == nil || got.Spec.Behavior.ScaleUp == nil {
		t.Errorf("Behavior from the fresh object must be preserved, got %+v", got.Spec.Behavior)
	}
}

// 更新冲突次数有界：持续冲突时返回错误，重试次数不超过上限。
func TestSyncUpdateConflictBounded(t *testing.T) {
	f := newFixture(t)
	d := newDeployment("uid-1", hpaAnnotations("1", "8"))
	expectedName := controller.HPANameForDeployment(d)
	managed := newManagedHPA(d, expectedName, time.Now())

	if err := f.dStore.Add(d); err != nil {
		t.Fatal(err)
	}
	if err := f.hpaStore.Add(managed); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).Create(
		context.TODO(), managed.DeepCopy(), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.client.ClearActions()

	f.client.PrependReactor("update", "horizontalpodautoscalers", func(action core.Action) (bool, runtime.Object, error) {
		hpa := action.(core.UpdateAction).GetObject().(*autoscalingv2.HorizontalPodAutoscaler)
		return true, nil, apierrors.NewConflict(
			schema.GroupResource{Resource: "horizontalpodautoscalers"}, hpa.Name, fmt.Errorf("conflict"))
	})

	if err := f.runSync(d); err == nil {
		t.Fatal("expected error after bounded conflict retries")
	}
	var updates int
	for _, a := range f.client.Actions() {
		if a.GetResource().Resource == "horizontalpodautoscalers" && a.GetVerb() == "update" {
			updates++
		}
	}
	if updates != maxUpdateConflictRetries {
		t.Fatalf("expected %d bounded update attempts, got %d", maxUpdateConflictRetries, updates)
	}
}

// 失败用例确认：模拟 API 服务器拒绝不携带 resourceVersion 的更新，
// 控制器发出的任何更新都必须携带 resourceVersion（旧流程整体替换、
// 无 resourceVersion 的写法在此会失败，不能断言其必然无条件覆盖）。
func TestSyncUpdateAlwaysCarriesResourceVersion(t *testing.T) {
	f := newFixture(t)
	d := newDeployment("uid-1", hpaAnnotations("1", "8"))
	expectedName := controller.HPANameForDeployment(d)
	managed := newManagedHPA(d, expectedName, time.Now())

	if err := f.dStore.Add(d); err != nil {
		t.Fatal(err)
	}
	if err := f.hpaStore.Add(managed); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).Create(
		context.TODO(), managed.DeepCopy(), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.client.ClearActions()

	f.client.PrependReactor("update", "horizontalpodautoscalers", func(action core.Action) (bool, runtime.Object, error) {
		hpa := action.(core.UpdateAction).GetObject().(*autoscalingv2.HorizontalPodAutoscaler)
		if hpa.ResourceVersion == "" {
			return true, nil, apierrors.NewInvalid(
				schema.GroupKind{Kind: "HorizontalPodAutoscaler"}, hpa.Name, nil)
		}
		return false, nil, nil
	})

	if err := f.runSync(d); err != nil {
		t.Fatalf("controller must never issue a resourceVersion-less update, got: %v", err)
	}
}

// 读取失败：冲突后重新读取失败时返回错误，且不能因读取失败就删除任何对象。
func TestSyncReadFailureDoesNotDelete(t *testing.T) {
	f := newFixture(t)
	d := newDeployment("uid-1", hpaAnnotations("1", "8"))
	expectedName := controller.HPANameForDeployment(d)
	managed := newManagedHPA(d, expectedName, time.Now())

	if err := f.dStore.Add(d); err != nil {
		t.Fatal(err)
	}
	if err := f.hpaStore.Add(managed); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.AutoscalingV2().HorizontalPodAutoscalers(testNamespace).Create(
		context.TODO(), managed.DeepCopy(), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.client.ClearActions()

	f.client.PrependReactor("update", "horizontalpodautoscalers", func(action core.Action) (bool, runtime.Object, error) {
		hpa := action.(core.UpdateAction).GetObject().(*autoscalingv2.HorizontalPodAutoscaler)
		return true, nil, apierrors.NewConflict(
			schema.GroupResource{Resource: "horizontalpodautoscalers"}, hpa.Name, fmt.Errorf("conflict"))
	})
	f.client.PrependReactor("get", "horizontalpodautoscalers", func(action core.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("connection refused")
	})

	if err := f.runSync(d); err == nil {
		t.Fatal("expected error when re-read fails")
	}
	for _, a := range f.client.Actions() {
		if a.GetVerb() == "delete" {
			t.Fatalf("read failure must not lead to deletion, got %v", actionNames([]core.Action{a}))
		}
	}
	// 直接查 tracker，绕过上述 get reactor，确认对象仍然存在
	if _, err := f.client.Tracker().Get(
		schema.GroupVersionResource{Group: "autoscaling", Version: "v2", Resource: "horizontalpodautoscalers"},
		testNamespace, expectedName); err != nil {
		t.Fatalf("managed HPA must survive read failure: %v", err)
	}
}

// getHPAsForDeployment 仅通过 controllerRef 的 UID、Kind、名称识别托管 HPA。
func TestGetHPAsForDeployment(t *testing.T) {
	f := newFixture(t)
	d := newDeployment("uid-1", hpaAnnotations("1", "6"))

	managed := newManagedHPA(d, "managed", time.Now())
	staleUID := newManagedHPA(d, "stale-uid", time.Now())
	staleUID.OwnerReferences = []metav1.OwnerReference{ownerRef(d, "uid-old")}
	foreign := newForeignHPA("foreign", testDeploy)
	noRef := newForeignHPA("no-ref", "other")

	for _, h := range []*autoscalingv2.HorizontalPodAutoscaler{managed, staleUID, foreign, noRef} {
		if err := f.hpaStore.Add(h); err != nil {
			t.Fatal(err)
		}
	}

	got, err := f.ac.getHPAsForDeployment(d)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Name != "managed" {
		names := make([]string, 0, len(got))
		for _, h := range got {
			names = append(names, h.Name)
		}
		t.Fatalf("expected only [managed], got %v", names)
	}
}
