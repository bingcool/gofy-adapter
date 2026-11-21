package k8slease_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"

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

// TestRenewConflictFreesTheInProcessReservation covers the resource-version
// conflict branch: the holder/token never changed, only the Update lost a race,
// but Renew reports LeaseLost and must still release the local reservation.
// Otherwise the same Locker is stuck in ErrHeld forever.
// TestRenewConflictFreesTheInProcessReservation 覆盖 resourceVersion 冲突分支：holder/token
// 没变，只是 Update 竞争失败，但 Renew 已经报告 LeaseLost，就必须释放本地占用；否则同一个
// Locker 会永久卡在 ErrHeld。
func TestRenewConflictFreesTheInProcessReservation(t *testing.T) {
	api := newFakeAPI()
	l := newLocker(t, api, "pod-a")

	held, err := l.Acquire(context.Background(), "settle-reconcile", ttl)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	// Force the next Update to conflict without changing holder or token.
	// 强制下一次 Update 冲突，但不改 holder/token。
	api.conflictNext = true
	if err := held.Renew(context.Background()); !errors.Is(err, lease.ErrLeaseLost) {
		t.Fatalf("renew on conflict = %v, want ErrLeaseLost", err)
	}

	// The conflict was one-shot. The same Locker must be able to re-acquire,
	// which only works if the failed Renew released its reservation.
	// 这次冲突是一次性的。同一个 Locker 必须能重新获取，而这只有在失败的 Renew 释放了
	// 进程内占用之后才可能。
	if _, err := l.Acquire(context.Background(), "settle-reconcile", ttl); err != nil {
		t.Fatalf("acquire after renew conflict: %v", err)
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

// deadlineAPI records the deadline passed to Get, which is the observable proof
// that the locker bounds its own API calls.
// deadlineAPI 记录传给 Get 的 deadline，这是 Locker 自己限制 API 调用时长的可观测量。
type deadlineAPI struct {
	*fakeAPI
	deadline time.Time
	sawIt    bool
}

func (d *deadlineAPI) Get(ctx context.Context, name string, opts metav1.GetOptions) (*coordinationv1.Lease, error) {
	d.deadline, d.sawIt = ctx.Deadline()
	return d.fakeAPI.Get(ctx, name, opts)
}

// gateAPI parks Get until the context is done once the gate is closed, the way
// an unreachable API server behaves.
// gateAPI 在闸门关闭后让 Get 一直等到 ctx 结束，模拟不可达的 API server。
type gateAPI struct {
	*fakeAPI
	blocked atomic.Bool
}

func (g *gateAPI) Get(ctx context.Context, name string, opts metav1.GetOptions) (*coordinationv1.Lease, error) {
	if g.blocked.Load() {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return g.fakeAPI.Get(ctx, name, opts)
}

// TestRequestTimeoutBoundsAPICalls pins E6's timeout half: every coordination
// call runs under the configured budget even though the caller passes a
// deadline-free context, and a hung API server therefore fails the operation
// instead of parking it forever.
// TestRequestTimeoutBoundsAPICalls 固定 E6 的超时一侧：即使调用方传入没有 deadline
// 的 ctx，每次 coordination 调用也受配置预算约束，挂死的 API server 因此让操作失败，
// 而不是把它永久挂起。
func TestRequestTimeoutBoundsAPICalls(t *testing.T) {
	api := &deadlineAPI{fakeAPI: newFakeAPI()}
	l, err := k8slease.New(api, k8slease.Config{Namespace: "default", Identity: "pod-a", RequestTimeout: 25 * time.Millisecond})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := l.Acquire(context.Background(), "settle-reconcile", ttl); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !api.sawIt {
		t.Fatal("Acquire called the API without a deadline")
	}
	if left := time.Until(api.deadline); left <= 0 || left > time.Second {
		t.Fatalf("deadline is %s away, want within the 25ms budget", left)
	}

	gated := &gateAPI{fakeAPI: newFakeAPI()}
	blockedLocker, err := k8slease.New(gated, k8slease.Config{Namespace: "default", Identity: "pod-a", RequestTimeout: 25 * time.Millisecond})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	blockedHeld, err := blockedLocker.Acquire(context.Background(), "settle-reconcile", ttl)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	gated.blocked.Store(true)

	start := time.Now()
	if err := blockedHeld.Renew(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Renew = %v, want the request timeout to fire", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Renew blocked for %s, the budget was not applied", elapsed)
	}
	start = time.Now()
	if err := blockedHeld.Release(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Release = %v, want the request timeout to fire", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Release blocked for %s, the budget was not applied", elapsed)
	}

	// The default budget must still be a real deadline, not "none".
	defaultLocker, err := k8slease.New(api, k8slease.Config{Namespace: "default", Identity: "pod-b"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := defaultLocker.Acquire(context.Background(), "settle-default", ttl); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if left := time.Until(api.deadline); left < 5*time.Second || left > 15*time.Second {
		t.Fatalf("default deadline is %s away, want about 10s", left)
	}
}

// TestAcquireRejectsTTLOverflowingLeaseSeconds covers the second half of E6:
// past ~68 years LeaseDurationSeconds overflows int32 inside durationSeconds
// and degrades to one second, so the TTL is rejected up front instead.
// TestAcquireRejectsTTLOverflowingLeaseSeconds 覆盖 E6 的另一半：超过约 68 年的 TTL
// 会在 durationSeconds 里溢出 int32 并退化成 1 秒，因此改为在入口就拒绝。
func TestAcquireRejectsTTLOverflowingLeaseSeconds(t *testing.T) {
	api := newFakeAPI()
	l := newLocker(t, api, "pod-a")
	oversized := time.Duration(math.MaxInt32+1) * time.Second
	if _, err := l.Acquire(context.Background(), "settle-reconcile", oversized); !errors.Is(err, lease.ErrInvalidTTL) {
		t.Fatalf("Acquire = %v, want ErrInvalidTTL for an overflowing TTL", err)
	}
	if len(api.objects) != 0 {
		t.Fatal("the overflowing TTL reached the API before being rejected")
	}
}

// TestNewRejectsNegativeRequestTimeout keeps the knob honest: a negative
// budget cannot mean anything, so construction fails instead of silently
// falling back to the default.
// TestNewRejectsNegativeRequestTimeout 让开关保持诚实：负预算没有含义，构造应当
// 失败，而不是悄悄回落成默认值。
func TestNewRejectsNegativeRequestTimeout(t *testing.T) {
	cfg := k8slease.Config{Namespace: "default", Identity: "pod-a", RequestTimeout: -time.Second}
	if _, err := k8slease.New(newFakeAPI(), cfg); err == nil {
		t.Fatal("New accepted a negative request timeout")
	}
	if _, err := k8slease.NewForConfig(&rest.Config{}, cfg); err == nil {
		t.Fatal("NewForConfig accepted a negative request timeout")
	}
}

// windowAPI widens the Get→Update window and records how many renewals sit in
// it at once, which is exactly what the per-handle mutex must keep at one.
// windowAPI 放大 Get→Update 窗口，并记录同时处于窗口内的续期数量；每句柄互斥锁
// 要保证的正是这个数量不超过一。
type windowAPI struct {
	*fakeAPI
	mu     sync.Mutex
	active int
	max    int
}

func (w *windowAPI) Get(ctx context.Context, name string, opts metav1.GetOptions) (*coordinationv1.Lease, error) {
	w.mu.Lock()
	w.active++
	if w.active > w.max {
		w.max = w.active
	}
	w.mu.Unlock()
	time.Sleep(2 * time.Millisecond)
	return w.fakeAPI.Get(ctx, name, opts)
}

func (w *windowAPI) Create(ctx context.Context, obj *coordinationv1.Lease, opts metav1.CreateOptions) (*coordinationv1.Lease, error) {
	defer w.end()
	return w.fakeAPI.Create(ctx, obj, opts)
}

func (w *windowAPI) Update(ctx context.Context, obj *coordinationv1.Lease, opts metav1.UpdateOptions) (*coordinationv1.Lease, error) {
	defer w.end()
	return w.fakeAPI.Update(ctx, obj, opts)
}

func (w *windowAPI) end() {
	w.mu.Lock()
	w.active--
	w.mu.Unlock()
}

func (w *windowAPI) peak() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.max
}

// TestConcurrentRenewOnOneHandleIsSerialized pins E1: without the per-handle
// mutex two renewals read the same resourceVersion, the second Update conflicts
// and Renew reports ErrLeaseLost while the process still holds the lease.
// TestConcurrentRenewOnOneHandleIsSerialized 固定 E1：没有每句柄互斥锁时，两次续期
// 读到同一个 resourceVersion，后一次 Update 冲突，Renew 在进程仍持有租约时误报
// ErrLeaseLost。
func TestConcurrentRenewOnOneHandleIsSerialized(t *testing.T) {
	api := &windowAPI{fakeAPI: newFakeAPI()}
	l, err := k8slease.New(api, k8slease.Config{Namespace: "default", Identity: "pod-a"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	held, err := l.Acquire(context.Background(), "settle-reconcile", ttl)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	const renewers = 8
	start := make(chan struct{})
	errs := make(chan error, renewers)
	for i := 0; i < renewers; i++ {
		go func() {
			<-start
			errs <- held.Renew(context.Background())
		}()
	}
	close(start)
	for i := 0; i < renewers; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent Renew: %v", err)
		}
	}
	if peak := api.peak(); peak != 1 {
		t.Fatalf("%d renewals overlapped the Get→Update window, want 1", peak)
	}
}

// TestCloseFreesReservationsAndRejectsAcquire pins E4: a heldLease dropped
// without Release used to leave its key reserved for the life of the Locker,
// and Close is the release valve. Handles taken before Close still work; only
// new acquisitions are refused.
// TestCloseFreesReservationsAndRejectsAcquire 固定 E4：被丢弃且未 Release 的
// heldLease 过去会让 key 在整个 Locker 生命周期内保持占用，Close 就是泄压阀。
// Close 之前取得的句柄仍可使用，只有新的获取会被拒绝。
func TestCloseFreesReservationsAndRejectsAcquire(t *testing.T) {
	api := newFakeAPI()
	l := newLocker(t, api, "pod-a")
	held, err := l.Acquire(context.Background(), "settle-reconcile", ttl)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	// The handle is still around, but the reservation must not outlive Close.
	// 句柄还在，但占用不能活过 Close。
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := l.Acquire(context.Background(), "settle-reconcile", ttl); !errors.Is(err, k8slease.ErrClosed) {
		t.Fatalf("Acquire after Close = %v, want ErrClosed", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	// Release keeps working after Close and still hands the object back.
	// Close 之后 Release 仍可用，也仍会把对象交还。
	if err := held.Release(context.Background()); err != nil {
		t.Fatalf("Release after Close: %v", err)
	}
	if got := api.holder("settle-reconcile"); got != "" {
		t.Fatalf("holder = %q after Release, want empty", got)
	}
}

// renewGateAPI parks the first Get after block is set, signalling when it is
// inside. That makes an API call observably in flight while Close runs.
// ignoreCtx keeps the call parked even after the request deadline, which
// exercises Close's own wait bound.
// renewGateAPI 在 block 置位后让第一次 Get 停住，并在进入时发信号，测试因此能
// 观察一次确实在途的 API 调用。ignoreCtx 让调用连请求超时也不解除阻塞，用它
// 检验 Close 自身的等待上限。
type renewGateAPI struct {
	*fakeAPI
	blocked   atomic.Bool
	ignoreCtx atomic.Bool
	entered   chan struct{}
	release   chan struct{}
	once      sync.Once
}

func newRenewGateAPI() *renewGateAPI {
	return &renewGateAPI{
		fakeAPI: newFakeAPI(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (g *renewGateAPI) Get(ctx context.Context, name string, opts metav1.GetOptions) (*coordinationv1.Lease, error) {
	if g.blocked.Load() {
		g.once.Do(func() { close(g.entered) })
		if g.ignoreCtx.Load() {
			<-g.release
		} else {
			select {
			case <-g.release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return g.fakeAPI.Get(ctx, name, opts)
}

// TestCloseWaitsForInFlightRenew pins P2-08: Close must not return while an
// API call that started before it is still running.
// TestCloseWaitsForInFlightRenew 固定 P2-08：仍在进行、早于 Close 的 API 调用
// 结束前，Close 不能返回。
func TestCloseWaitsForInFlightRenew(t *testing.T) {
	api := newRenewGateAPI()
	l, err := k8slease.New(api, k8slease.Config{Namespace: "default", Identity: "pod-a", RequestTimeout: time.Second})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	held, err := l.Acquire(context.Background(), "settle-reconcile", ttl)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	api.blocked.Store(true)
	renewDone := make(chan error, 1)
	go func() { renewDone <- held.Renew(context.Background()) }()
	<-api.entered

	closeDone := make(chan error, 1)
	go func() { closeDone <- l.Close() }()
	select {
	case <-closeDone:
		t.Fatal("Close returned while the Renew was still in flight")
	case <-time.After(30 * time.Millisecond):
	}

	close(api.release)
	if err := <-renewDone; err != nil {
		t.Fatalf("Renew: %v", err)
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return after the in-flight Renew finished")
	}
}

// TestCloseWaitIsBoundedByRequestTimeout pins the wait ceiling and its outcome:
// a Close issued while an operation ignores its own deadline returns in bounded
// time and reports ErrCloseTimeout instead of a silent success. A second Close
// after the operation drains returns nil.
// TestCloseWaitIsBoundedByRequestTimeout 固定等待上限及其结果：在途操作无视自身
// deadline 时，Close 在有界时间内返回，并报告 ErrCloseTimeout 而不是静默成功。
// 操作排空后再调用一次 Close 返回 nil。
func TestCloseWaitIsBoundedByRequestTimeout(t *testing.T) {
	api := newRenewGateAPI()
	const budget = 40 * time.Millisecond
	l, err := k8slease.New(api, k8slease.Config{Namespace: "default", Identity: "pod-a", RequestTimeout: budget})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	held, err := l.Acquire(context.Background(), "settle-reconcile", ttl)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	api.blocked.Store(true)
	api.ignoreCtx.Store(true)
	renewDone := make(chan error, 1)
	go func() { renewDone <- held.Renew(context.Background()) }()
	<-api.entered

	start := time.Now()
	if err := l.Close(); !errors.Is(err, k8slease.ErrCloseTimeout) {
		t.Fatalf("Close = %v, want ErrCloseTimeout", err)
	}
	elapsed := time.Since(start)
	if elapsed < budget/2 {
		t.Fatalf("Close returned after %s, did not wait for the in-flight operation", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Close blocked for %s despite the %s budget", elapsed, budget)
	}

	close(api.release)
	if err := <-renewDone; err != nil {
		t.Fatalf("Renew: %v", err)
	}

	// Once the operation has drained, a second Close reports success rather
	// than repeating the first Close's timeout.
	// 操作排空之后，第二次 Close 报告成功，而不是重复第一次的超时。
	if err := l.Close(); err != nil {
		t.Fatalf("second Close after drain: %v", err)
	}
}

// TestCloseRejectsAcquireFinishingAfterClose pins P2-05: an Acquire that was
// already in flight when Close began must fail with ErrClosed instead of
// delivering a handle, even though the Kubernetes object was acquired. The
// object is intentionally left to expire, like a handle dropped without
// Release; the control case (a handle acquired before Close) is covered by
// TestCloseFreesReservationsAndRejectsAcquire.
// TestCloseRejectsAcquireFinishingAfterClose 固定 P2-05：Close 开始时已在途的
// Acquire 必须以 ErrClosed 失败，而不是交付句柄，即使 Kubernetes 对象已经取得。
// 该对象刻意留到自然过期，等同未 Release 就丢弃的句柄；对照行（Close 之前取得的
// 句柄）由 TestCloseFreesReservationsAndRejectsAcquire 覆盖。
func TestCloseRejectsAcquireFinishingAfterClose(t *testing.T) {
	api := newRenewGateAPI()
	const budget = 40 * time.Millisecond
	l, err := k8slease.New(api, k8slease.Config{Namespace: "default", Identity: "pod-a", RequestTimeout: budget})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	api.blocked.Store(true)
	api.ignoreCtx.Store(true)
	type result struct {
		held lease.Lease
		err  error
	}
	acquired := make(chan result, 1)
	go func() {
		held, err := l.Acquire(context.Background(), "settle-reconcile", ttl)
		acquired <- result{held: held, err: err}
	}()
	<-api.entered

	if err := l.Close(); !errors.Is(err, k8slease.ErrCloseTimeout) {
		t.Fatalf("Close = %v, want ErrCloseTimeout while the Acquire is parked", err)
	}
	select {
	case r := <-acquired:
		t.Fatalf("Acquire returned before the API was released: %+v", r)
	default:
	}

	close(api.release)
	r := <-acquired
	if !errors.Is(r.err, k8slease.ErrClosed) {
		t.Fatalf("Acquire = %v, want ErrClosed", r.err)
	}
	if r.held != nil {
		t.Fatalf("Acquire delivered %v after Close", r.held)
	}
	if got := api.holder("settle-reconcile"); got != "pod-a" {
		t.Fatalf("holder = %q, want the object left behind for pod-a until expiry", got)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close after the refused acquisition drained: %v", err)
	}
}

// TestLifecycleOperationsRunConcurrently puts Close, Acquire, Renew and Release
// on one Locker at the same time. The test does not prescribe the
// interleaving; it requires every outcome to stay inside the documented set
// while -race checks the locking discipline itself.
// TestLifecycleOperationsRunConcurrently 让 Close、Acquire、Renew 与 Release 同时
// 操作一个 Locker。测试不规定交错顺序，只要求所有结果落在文档允许的集合内，并由
// -race 检查锁纪律本身。
func TestLifecycleOperationsRunConcurrently(t *testing.T) {
	const workers = 6
	api := newFakeAPI()
	l, err := k8slease.New(api, k8slease.Config{
		Namespace:      "default",
		Identity:       "pod-a",
		RequestTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	keys := make([]string, workers)
	handles := make([]lease.Lease, workers)
	for i := range keys {
		keys[i] = "race-" + strconv.Itoa(i)
		held, err := l.Acquire(context.Background(), keys[i], ttl)
		if err != nil {
			t.Fatalf("pre-acquire %s: %v", keys[i], err)
		}
		handles[i] = held
	}

	start := make(chan struct{})
	renewing := make(chan struct{}, workers)
	errs := make(chan error, workers*3+1)
	report := func(err error) { errs <- err }
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(3)
		go func(i int) {
			defer wg.Done()
			<-start
			renewing <- struct{}{}
			for j := 0; j < 50; j++ {
				// A lost lease is expected once the matching Release below wins
				// the race; anything else is a real failure.
				// 与会 Release 竞争后丢失租约是预期的，其他错误才算失败。
				if err := handles[i].Renew(context.Background()); err != nil && !errors.Is(err, lease.ErrLeaseLost) {
					report(fmt.Errorf("renew %s: %w", keys[i], err))
					return
				}
				runtime.Gosched()
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			<-start
			late := "late-" + strconv.Itoa(i)
			held, err := l.Acquire(context.Background(), late, ttl)
			switch {
			case err == nil:
				if held == nil {
					report(fmt.Errorf("late acquire %s returned no handle and no error", late))
					return
				}
				if err := held.Release(context.Background()); err != nil {
					report(fmt.Errorf("late release %s: %w", late, err))
				}
			case errors.Is(err, k8slease.ErrClosed):
				// The normal outcome once Close won the race.
				// Close 赢得竞争后的正常结果。
			default:
				report(fmt.Errorf("late acquire %s: %w", late, err))
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			<-start
			runtime.Gosched()
			if err := handles[i].Release(context.Background()); err != nil {
				report(fmt.Errorf("release %s: %w", keys[i], err))
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		// Wait until every renewer has entered its loop, so Close really does
		// overlap Renew instead of possibly running first in isolation.
		// 等所有续期方进入循环，确保 Close 与 Renew 真正重叠，而不是可能先单独跑完。
		for i := 0; i < workers; i++ {
			<-renewing
		}
		runtime.Gosched()
		if err := l.Close(); err != nil {
			report(fmt.Errorf("close: %w", err))
		}
	}()

	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestCloseDoesNotBlockReservationCleanup covers the deadlock constraint: an
// in-flight Renew that loses the lease needs mu to drop its reservation, so
// Close must not hold mu while waiting for it.
// TestCloseDoesNotBlockReservationCleanup 覆盖死锁约束：在途 Renew 丢失租约后
// 需要 mu 清理占用，Close 等待时因此不能持有 mu。
func TestCloseDoesNotBlockReservationCleanup(t *testing.T) {
	api := newRenewGateAPI()
	l, err := k8slease.New(api, k8slease.Config{Namespace: "default", Identity: "pod-a", RequestTimeout: time.Second})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	held, err := l.Acquire(context.Background(), "settle-reconcile", ttl)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	api.blocked.Store(true)
	renewDone := make(chan error, 1)
	go func() { renewDone <- held.Renew(context.Background()) }()
	<-api.entered

	closeDone := make(chan error, 1)
	go func() { closeDone <- l.Close() }()

	// Make the parked Renew discover another holder once it resumes; its
	// ErrLeaseLost path calls unreserve, which needs mu.
	// 让停住的 Renew 恢复后发现持有者已变；ErrLeaseLost 分支会调用需要 mu 的
	// unreserve。
	api.takeover(t, "settle-reconcile", "pod-b")
	close(api.release)
	if err := <-renewDone; !errors.Is(err, lease.ErrLeaseLost) {
		t.Fatalf("Renew = %v, want ErrLeaseLost", err)
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close deadlocked with the in-flight Renew's reservation cleanup")
	}
}
