package k8slease_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/bingcool/gofy-adapter/adapter/lease/k8slease"
	"github.com/bingcool/gofy/support/lease"
)

var leases = schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}

// fakeAPI is an in-memory Lease API with the one behaviour that matters here:
// Update only succeeds when the resourceVersion still matches.
// fakeAPI 是内存版 Lease API，只保留这里真正依赖的行为：resourceVersion 仍匹配时
// Update 才成功。
type fakeAPI struct {
	mu           sync.Mutex
	objects      map[string]*coordinationv1.Lease
	version      int
	conflictNext bool // make the next Update fail like a stale resourceVersion
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{objects: map[string]*coordinationv1.Lease{}}
}

func (f *fakeAPI) Get(_ context.Context, name string, _ metav1.GetOptions) (*coordinationv1.Lease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, ok := f.objects[name]
	if !ok {
		return nil, apierrors.NewNotFound(leases, name)
	}
	return obj.DeepCopy(), nil
}

func (f *fakeAPI) Create(_ context.Context, obj *coordinationv1.Lease, _ metav1.CreateOptions) (*coordinationv1.Lease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.objects[obj.Name]; ok {
		return nil, apierrors.NewAlreadyExists(leases, obj.Name)
	}
	f.version++
	stored := obj.DeepCopy()
	stored.ResourceVersion = strconv.Itoa(f.version)
	f.objects[stored.Name] = stored
	return stored.DeepCopy(), nil
}

func (f *fakeAPI) Update(_ context.Context, obj *coordinationv1.Lease, _ metav1.UpdateOptions) (*coordinationv1.Lease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur, ok := f.objects[obj.Name]
	if !ok {
		return nil, apierrors.NewNotFound(leases, obj.Name)
	}
	if f.conflictNext {
		f.conflictNext = false
		return nil, apierrors.NewConflict(leases, obj.Name, errors.New("resource version mismatch"))
	}
	if cur.ResourceVersion != obj.ResourceVersion {
		return nil, apierrors.NewConflict(leases, obj.Name, errors.New("resource version mismatch"))
	}
	f.version++
	stored := obj.DeepCopy()
	stored.ResourceVersion = strconv.Itoa(f.version)
	f.objects[stored.Name] = stored
	return stored.DeepCopy(), nil
}

// expire moves the stored renewTime into the past, the way a paused holder's
// lease runs out.
// expire 把已存储的 renewTime 拨到过去，模拟持有者停滞后租期耗完。
func (f *fakeAPI) expire(t *testing.T, name string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, ok := f.objects[name]
	if !ok {
		t.Fatalf("%q is not stored", name)
	}
	past := metav1.NewMicroTime(time.Now().Add(-time.Hour))
	obj.Spec.RenewTime = &past
}

func (f *fakeAPI) renewTime(name string) time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	if obj, ok := f.objects[name]; ok && obj.Spec.RenewTime != nil {
		return obj.Spec.RenewTime.Time
	}
	return time.Time{}
}

func (f *fakeAPI) holder(name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if obj, ok := f.objects[name]; ok && obj.Spec.HolderIdentity != nil {
		return *obj.Spec.HolderIdentity
	}
	return ""
}

func newLocker(t *testing.T, api *fakeAPI, identity string) *k8slease.Locker {
	t.Helper()
	l, err := k8slease.New(api, k8slease.Config{Namespace: "default", Identity: identity})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return l
}

const ttl = 10 * time.Second

func TestAcquireTakesAFreeLease(t *testing.T) {
	api := newFakeAPI()
	l := newLocker(t, api, "pod-a")

	held, err := l.Acquire(context.Background(), "settle-reconcile", ttl)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if held.Token() != 1 {
		t.Fatalf("token = %d, want 1", held.Token())
	}
	if got := api.holder("settle-reconcile"); got != "pod-a" {
		t.Fatalf("holder = %q, want pod-a", got)
	}
}

func TestAcquireIsExclusive(t *testing.T) {
	api := newFakeAPI()
	first := newLocker(t, api, "pod-a")
	second := newLocker(t, api, "pod-b")

	if _, err := first.Acquire(context.Background(), "settle-reconcile", ttl); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if _, err := second.Acquire(context.Background(), "settle-reconcile", ttl); !errors.Is(err, lease.ErrHeld) {
		t.Fatalf("second acquire: got %v, want ErrHeld", err)
	}
}

// TestTakeoverBumpsTheToken is the fencing guarantee: a new holder always gets a
// larger transition count than the one it replaced.
// TestTakeoverBumpsTheToken 是 fencing 的保证：新持有者的 transition 计数一定比它
// 接替的那个大。
func TestTakeoverBumpsTheToken(t *testing.T) {
	api := newFakeAPI()
	old := newLocker(t, api, "pod-a")
	replacement := newLocker(t, api, "pod-b")

	first, err := old.Acquire(context.Background(), "settle-reconcile", ttl)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	api.expire(t, "settle-reconcile")

	next, err := replacement.Acquire(context.Background(), "settle-reconcile", ttl)
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if next.Token() <= first.Token() {
		t.Fatalf("takeover token %d did not advance past %d", next.Token(), first.Token())
	}
	if err := first.Renew(context.Background()); !errors.Is(err, lease.ErrLeaseLost) {
		t.Fatalf("stale renew: got %v, want ErrLeaseLost", err)
	}
	if err := first.Release(context.Background()); err != nil {
		t.Fatalf("stale release: %v", err)
	}
	if got := api.holder("settle-reconcile"); got != "pod-b" {
		t.Fatalf("stale release cleared the new holder: %q", got)
	}
}

// TestSameIdentityReacquireKeepsTheToken covers a pod restart that reads its own
// unexpired lease: it must not invalidate tokens its in-flight work already
// wrote to the database.
// TestSameIdentityReacquireKeepsTheToken 覆盖同名 Pod 重启读到自己的未过期租约：
// 它不能让自己已经在途写入数据库的 token 失效。
// TestSameIdentityReacquireKeepsTheToken models a pod restarting: the process
// that held the lease is gone, a new Locker appears with the same identity, and
// the token must not move — its own in-flight writes may still be fencing on it.
// A second Locker is the point: re-acquiring through the *same* Locker is a
// concurrent acquire, which ErrHeld rejects (see the single-flight tests).
// TestSameIdentityReacquireKeepsTheToken 模拟 Pod 重启：持有租约的进程已经消失，同
// identity 的新 Locker 出现，token 不能变 —— 它自己还在途的写入可能仍以这个 token 做
// fencing。用新 Locker 是关键：同一个 Locker 再获取属于并发获取，会被 ErrHeld 拒绝
// （见单飞测试）。
func TestSameIdentityReacquireKeepsTheToken(t *testing.T) {
	api := newFakeAPI()
	before := newLocker(t, api, "pod-a")

	first, err := before.Acquire(context.Background(), "settle-reconcile", ttl)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	// The restarted process has an empty single-flight set.
	// 重启后的进程单飞表是空的。
	after := newLocker(t, api, "pod-a")
	again, err := after.Acquire(context.Background(), "settle-reconcile", ttl)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if again.Token() != first.Token() {
		t.Fatalf("token moved from %d to %d on re-acquire", first.Token(), again.Token())
	}
}

// TestConcurrentAcquireInOneProcessIsSingleFlight is the P2-04 acceptance test:
// without the in-process set, both calls would see holder == identity and both
// would "re-join" the same term, so two goroutines would believe they hold the
// same critical section.
// TestConcurrentAcquireInOneProcessIsSingleFlight 是 P2-04 的验收测试：没有进程内
// 单飞表，两次调用都会看到 holder == identity 并"重新加入"同一租期，两个 goroutine
// 因此都会认为自己持有同一个临界区。
func TestConcurrentAcquireInOneProcessIsSingleFlight(t *testing.T) {
	api := newFakeAPI()
	l := newLocker(t, api, "pod-a")

	held, err := l.Acquire(context.Background(), "settle-reconcile", ttl)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if _, err := l.Acquire(context.Background(), "settle-reconcile", ttl); !errors.Is(err, lease.ErrHeld) {
		t.Fatalf("second acquire in the same process = %v, want ErrHeld", err)
	}

	// Other keys are unaffected: single-flight is per key, not per locker.
	// 其它 key 不受影响：单飞是按 key 的，不是按 locker 的。
	if _, err := l.Acquire(context.Background(), "other-job", ttl); err != nil {
		t.Fatalf("acquire on another key: %v", err)
	}

	if err := held.Release(context.Background()); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := l.Acquire(context.Background(), "settle-reconcile", ttl); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
}

// TestRenewLossFreesTheInProcessReservation covers the recovery path: once the
// lease is gone there is nothing left to protect, so the key must become
// acquirable again in the same process.
// TestRenewLossFreesTheInProcessReservation 覆盖恢复路径：租约没了就没有需要保护的
// 东西，同一个进程必须能重新获取该 key。
func TestRenewLossFreesTheInProcessReservation(t *testing.T) {
	api := newFakeAPI()
	l := newLocker(t, api, "pod-a")

	held, err := l.Acquire(context.Background(), "settle-reconcile", ttl)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	// Another identity wins an Update behind our back.
	// 另一个 identity 在背后赢了一次 Update。
	api.takeover(t, "settle-reconcile", "pod-b")
	if err := held.Renew(context.Background()); !errors.Is(err, lease.ErrLeaseLost) {
		t.Fatalf("renew after takeover = %v, want ErrLeaseLost", err)
	}

	// pod-b hands the lease back. pod-a can only re-acquire if the lost lease
	// released its in-process reservation; otherwise this returns
	// "already held by this process".
	// pod-b 交还租约。只有当丢失的租约释放了进程内占用，pod-a 才能重新获取；否则这里
	// 会返回 "already held by this process"。
	api.clearHolder(t, "settle-reconcile")
	if _, err := l.Acquire(context.Background(), "settle-reconcile", ttl); err != nil {
		t.Fatalf("acquire after lost lease: %v", err)
	}
}

// takeover moves the stored lease to another identity and bumps the token, the
// way a competitor that won a real Update would.
// takeover 把已存储的租约换成另一个 identity 并递增 token，模拟真实竞争中赢下一次
// Update 的对手。
func (f *fakeAPI) takeover(t *testing.T, name, identity string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, ok := f.objects[name]
	if !ok {
		t.Fatalf("%q is not stored", name)
	}
	f.version++
	now := metav1.NewMicroTime(time.Now())
	token := int32(1)
	if obj.Spec.LeaseTransitions != nil {
		token = *obj.Spec.LeaseTransitions + 1
	}
	obj.Spec.HolderIdentity = &identity
	obj.Spec.LeaseTransitions = &token
	obj.Spec.RenewTime = &now
	obj.ResourceVersion = strconv.Itoa(f.version)
}

// clearHolder frees the object the way Release does.
// clearHolder 像 Release 那样把对象置为空闲。
func (f *fakeAPI) clearHolder(t *testing.T, name string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, ok := f.objects[name]
	if !ok {
		t.Fatalf("%q is not stored", name)
	}
	f.version++
	empty := ""
	one := int32(1)
	now := metav1.NewMicroTime(time.Now())
	obj.Spec.HolderIdentity = &empty
	obj.Spec.LeaseDurationSeconds = &one
	obj.Spec.RenewTime = &now
	obj.ResourceVersion = strconv.Itoa(f.version)
}
