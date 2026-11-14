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
func TestSameIdentityReacquireKeepsTheToken(t *testing.T) {
	api := newFakeAPI()
	l := newLocker(t, api, "pod-a")

	first, err := l.Acquire(context.Background(), "settle-reconcile", ttl)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	again, err := l.Acquire(context.Background(), "settle-reconcile", ttl)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if again.Token() != first.Token() {
		t.Fatalf("token moved from %d to %d on re-acquire", first.Token(), again.Token())
	}
}

func TestReleaseHandsTheLeaseBack(t *testing.T) {
	api := newFakeAPI()
	first := newLocker(t, api, "pod-a")
	second := newLocker(t, api, "pod-b")

	held, err := first.Acquire(context.Background(), "settle-reconcile", ttl)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := held.Release(context.Background()); err != nil {
		t.Fatalf("release: %v", err)
	}
	if got := api.holder("settle-reconcile"); got != "" {
		t.Fatalf("holder after release = %q, want empty", got)
	}

	next, err := second.Acquire(context.Background(), "settle-reconcile", ttl)
	if err != nil {
		t.Fatalf("takeover after release: %v", err)
	}
	if next.Token() <= held.Token() {
		t.Fatalf("token %d did not advance past %d", next.Token(), held.Token())
	}
}

func TestRenewKeepsTheLeaseAlive(t *testing.T) {
	api := newFakeAPI()
	l := newLocker(t, api, "pod-a")
	held, err := l.Acquire(context.Background(), "settle-reconcile", ttl)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	before := api.renewTime("settle-reconcile")
	if err := held.Renew(context.Background()); err != nil {
		t.Fatalf("renew: %v", err)
	}
	after := api.renewTime("settle-reconcile")
	if !after.After(before) {
		t.Fatalf("renewTime did not move forward: %v -> %v", before, after)
	}
}

// TestConflictIsReportedAsHeld covers the lost race: the object moved between
// read and write, so this instance must go back to polling rather than assume
// it won.
// TestConflictIsReportedAsHeld 覆盖竞争失败：读写之间对象变了，本实例必须回到轮询
// 而不是假定自己赢。
func TestConflictIsReportedAsHeld(t *testing.T) {
	api := newFakeAPI()
	first := newLocker(t, api, "pod-a")
	second := newLocker(t, api, "pod-b")

	if _, err := first.Acquire(context.Background(), "settle-reconcile", ttl); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	api.expire(t, "settle-reconcile")

	// The update the takeover issues must fail the way a stale resourceVersion
	// does in a real apiserver.
	api.mu.Lock()
	api.conflictNext = true
	api.mu.Unlock()

	if _, err := second.Acquire(context.Background(), "settle-reconcile", ttl); !errors.Is(err, lease.ErrHeld) {
		t.Fatalf("conflicting acquire: got %v, want ErrHeld", err)
	}
}

func TestKeyMustBeADNSName(t *testing.T) {
	api := newFakeAPI()
	l := newLocker(t, api, "pod-a")

	if _, err := l.Acquire(context.Background(), "settle:reconcile", ttl); !errors.Is(err, k8slease.ErrInvalidObjectName) {
		t.Fatalf("colon key: got %v, want ErrInvalidObjectName", err)
	}
	if _, err := l.Acquire(context.Background(), "Settle", ttl); !errors.Is(err, k8slease.ErrInvalidObjectName) {
		t.Fatalf("uppercase key: got %v, want ErrInvalidObjectName", err)
	}
}

func TestNewValidatesConfig(t *testing.T) {
	api := newFakeAPI()
	if _, err := k8slease.New(nil, k8slease.Config{Namespace: "default", Identity: "pod-a"}); err == nil {
		t.Fatal("New(nil) returned no error")
	}
	if _, err := k8slease.New(api, k8slease.Config{Identity: "pod-a"}); err == nil {
		t.Fatal("New accepted an empty namespace")
	}
	if _, err := k8slease.New(api, k8slease.Config{Namespace: "default"}); err == nil {
		t.Fatal("New accepted an empty identity")
	}
}
