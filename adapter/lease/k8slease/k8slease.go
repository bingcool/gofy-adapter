// Package k8slease implements lease.Locker with a coordination.k8s.io/v1 Lease
// object, which is the lock Kubernetes itself uses for leader election.
//
// It lives in the adapter module on purpose: client-go drags a whole API
// machinery into go.mod, and a project that locks with Redis should not pay for
// it. Import the typed coordination client rather than the full clientset, so
// only that API group is compiled in.
// 包 k8slease 用 coordination.k8s.io/v1 的 Lease 对象实现 lease.Locker，这也是
// Kubernetes 自身选举用的锁。
//
// 它放在适配层模块是刻意的：client-go 会把一整套 API machinery 拖进 go.mod，用
// Redis 加锁的项目不该为它买单。这里 import 的是 coordination 的类型化客户端而不是
// 完整 clientset，因此只编译这一个 API 组。
package k8slease

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"

	"github.com/bingcool/gofy/support/lease"
)

// Config names the lease object and the holder.
// Config 指定租约对象与持有者。
type Config struct {
	// Namespace holds the Lease object; required.
	// Namespace 是 Lease 对象所在的命名空间，必填。
	Namespace string
	// Identity is written to holderIdentity. It must be unique per process,
	// otherwise two pods would both believe they hold the same lease; the pod
	// name is the natural choice.
	// Identity 写入 holderIdentity。它必须每个进程唯一，否则两个 Pod 都会认为
	// 自己持有同一个租约；Pod 名是最自然的选择。
	Identity string
	// RequestTimeout bounds each API interaction (Acquire, Renew, Release). It
	// is also the ceiling Close waits for in-flight operations. Zero means
	// defaultRequestTimeout; a caller context without a deadline is the normal
	// case, so without this a black-holed API server would park the lease
	// operations forever. Negative values are rejected.
	// RequestTimeout 限制每次 API 交互（Acquire、Renew、Release）的时长，同时也是
	// Close 等待在途操作的上限。零值表示 defaultRequestTimeout。调用方的 ctx 通常
	// 没有 deadline，若没有这个限制，被黑洞的 API server 会让租约操作永久挂起。
	// 负值被拒绝。
	RequestTimeout time.Duration
}

// defaultRequestTimeout bounds one API interaction when Config.RequestTimeout
// is zero.
// defaultRequestTimeout 在 Config.RequestTimeout 为零时限制单次 API 交互的时长。
const defaultRequestTimeout = 10 * time.Second

// maxLeaseSeconds is the largest value LeaseDurationSeconds can hold. A longer
// TTL would overflow int32 in durationSeconds and silently become one second,
// turning a long lease into one that expires before its holder renews.
// maxLeaseSeconds 是 LeaseDurationSeconds 能容纳的最大值。更长的 TTL 会在
// durationSeconds 里溢出 int32，悄悄变成一秒，长租约因此在持有者续期之前就过期。
const maxLeaseSeconds = int64(math.MaxInt32)

// LeaseAPI is the subset of the coordination API this package uses. The typed
// client satisfies it, and tests can supply a fake.
// LeaseAPI 是本包用到的 coordination API 子集。类型化客户端满足它，测试也可以
// 提供假实现。
type LeaseAPI interface {
	Get(ctx context.Context, name string, opts metav1.GetOptions) (*coordinationv1.Lease, error)
	Create(ctx context.Context, lease *coordinationv1.Lease, opts metav1.CreateOptions) (*coordinationv1.Lease, error)
	Update(ctx context.Context, lease *coordinationv1.Lease, opts metav1.UpdateOptions) (*coordinationv1.Lease, error)
}

// ErrInvalidObjectName means the key cannot be used as a Kubernetes object
// name. Keys are not sanitized because "a:b" and "a-b" would collide silently.
// ErrInvalidObjectName 表示 key 不能作为 Kubernetes 对象名。这里不做字符替换：
// "a:b" 与 "a-b" 会被悄悄映射成同一个对象。
var ErrInvalidObjectName = errors.New("k8slease: key must be a DNS-1123 subdomain")

// ErrClosed means Close was called; the Locker accepts no new acquisitions.
// An acquisition still in flight when Close begins also fails with it instead
// of delivering a handle; if that attempt already wrote the Lease object, the
// object stays until its term expires, like a handle dropped without Release.
// Handles acquired before Close still use the API, but their Release only
// empties the reservation table Close already cleared.
// ErrClosed 表示 Close 已调用，Locker 不再接受新的获取。Close 开始时仍在途的获取
// 也以它失败，而不是交付句柄；若该次尝试已经写入 Lease 对象，对象会留到租期结束，
// 与未 Release 就丢弃的句柄相同。Close 之前取得的句柄仍可访问 API，但它们 Release
// 时清理的占用表已被 Close 清空。
var ErrClosed = errors.New("k8slease: locker is closed")

// ErrCloseTimeout means Close stopped waiting before every in-flight operation
// finished. The Locker is still closed: late operations finish against the
// API, an acquisition still in flight fails with ErrClosed, and a later Close
// call waits again for whatever remains.
// ErrCloseTimeout 表示 Close 在全部在途操作结束之前停止了等待。Locker 仍然已经关闭：
// 迟到的操作继续访问 API，仍在途的获取以 ErrClosed 失败，之后再调用 Close 会继续等待
// 剩余操作。
var ErrCloseTimeout = errors.New("k8slease: close timed out waiting for in-flight operations")

// dnsSubdomain is the name rule for Lease objects.
// dnsSubdomain 是 Lease 对象的命名规则。
var dnsSubdomain = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

// Locker acquires leases through the Kubernetes API.
//
// It is safe for concurrent use across keys. For one key it is deliberately
// single-flight within the process: see the held field.
// Locker 通过 Kubernetes API 获取租约。
//
// 跨 key 并发安全；对同一个 key，进程内有意的单飞语义见 held 字段。
type Locker struct {
	api       LeaseAPI
	namespace string
	identity  string
	timeout   time.Duration

	// held is the in-process single-flight set, keyed by lease key.
	//
	// Re-acquiring a lease this identity already holds returns the same term on
	// purpose: a pod that restarts must not invalidate the fencing tokens its
	// own in-flight writes are still using. But that rule is about a *previous
	// process*, not about concurrent callers in the current one. Without this
	// set, two goroutines calling Acquire with the same key would both see
	// holder == identity and both "re-join", so the recovery rule would quietly
	// become "any number of concurrent owners". Reserving the key keeps the
	// recovery semantics and makes the second caller get ErrHeld instead.
	// held 是进程内单飞表，按租约 key 索引。
	//
	// 同一 identity 重新获取自己已持有的租约时沿用同一租期是刻意的：重启的 Pod 不能
	// 让自己在途写入仍在使用的 fencing token 失效。但那条规则针对的是"上一个进程"，
	// 不是当前进程里的并发调用方。没有这张表，两个 goroutine 用同一个 key 调 Acquire
	// 都会看到 holder == identity 并都"重新加入"，恢复语义就悄悄变成了"任意多个并发
	// 持有者"。占用 key 既保住了恢复语义，也让第二个调用方拿到 ErrHeld。
	mu   sync.Mutex
	held map[string]struct{}

	// closed is guarded by mu. Close sets it once; every later Acquire fails
	// with ErrClosed instead of refilling a map nobody will drain.
	// closed 由 mu 保护。Close 置位后，后续 Acquire 一律返回 ErrClosed，而不是
	// 重新填充一个没人再清理的表。
	closed bool

	// inflight counts API operations registered before Close began. Close waits
	// on it, bounded by RequestTimeout, so an operation that predates Close is
	// normally finished before Close returns; a timeout is reported as
	// ErrCloseTimeout instead of pretending the drain completed. Add shares mu
	// with the closed check, so no operation can join Close's Wait; each
	// operation pairs its Add with Done.
	// inflight 记录 Close 开始前已登记的 API 操作数。Close 等待它，上限是
	// RequestTimeout，早于 Close 的操作因此通常在 Close 返回前结束；等待超时以
	// ErrCloseTimeout 报告，而不是假装已经排空。Add 与 closed 检查共用 mu，因此没有
	// 操作能在 Close 的 Wait 之后加入；每个操作的 Add 都与 Done 成对出现。
	inflight sync.WaitGroup

	// drainOnce guards drained. Close can time out and be called again, and
	// every call must observe the same waiter: a fresh goroutine per call would
	// leave one blocked on inflight.Wait for each call that outlives the
	// operation it waited for.
	// drainOnce 保护 drained。Close 可能超时后被再次调用，每次调用都必须看到同一个等待者：
	// 每次新建 goroutine 会让每个活得比被等操作更久的调用留下一个阻塞在 inflight.Wait 上。
	drainOnce sync.Once
	drained   chan struct{}
}

// NewForConfig builds a locker from a rest config, using the typed coordination
// client only.
// NewForConfig 基于 rest 配置构建 Locker，只使用 coordination 的类型化客户端。
func NewForConfig(cfg *rest.Config, c Config) (*Locker, error) {
	if cfg == nil {
		return nil, fmt.Errorf("k8slease: rest config is required")
	}
	if err := validate(c); err != nil {
		return nil, err
	}
	// The rest config timeout is the transport-level backstop for the same
	// budget. Copy first so the caller's config (often shared) is not mutated.
	// rest 配置里的超时是同一预算的传输层兜底。先 Copy，避免改到调用方（常常是
	// 共享）的配置。
	restCfg := rest.CopyConfig(cfg)
	if restCfg.Timeout <= 0 {
		restCfg.Timeout = requestTimeout(c)
	}
	client, err := coordclient.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("k8slease: coordination client: %w", err)
	}
	return &Locker{
		api:       client.Leases(c.Namespace),
		namespace: c.Namespace,
		identity:  c.Identity,
		timeout:   requestTimeout(c),
		held:      make(map[string]struct{}),
	}, nil
}

// New builds a locker around an existing Lease API, which is how tests and
// custom clients wire it.
// New 基于已有的 Lease API 构建 Locker，测试与自定义客户端由此接入。
func New(api LeaseAPI, c Config) (*Locker, error) {
	if api == nil {
		return nil, fmt.Errorf("k8slease: lease api is required")
	}
	if err := validate(c); err != nil {
		return nil, err
	}
	return &Locker{
		api:       api,
		namespace: c.Namespace,
		identity:  c.Identity,
		timeout:   requestTimeout(c),
		held:      make(map[string]struct{}),
	}, nil
}

func validate(c Config) error {
	if c.Namespace == "" {
		return fmt.Errorf("k8slease: namespace is required")
	}
	if c.Identity == "" {
		return fmt.Errorf("k8slease: identity is required")
	}
	if c.RequestTimeout < 0 {
		return fmt.Errorf("k8slease: request timeout must not be negative: %s", c.RequestTimeout)
	}
	return nil
}

// requestTimeout resolves the zero value to the default; validate has already
// rejected negative values.
// requestTimeout 把零值解析为默认值；负值已在 validate 中拒绝。
func requestTimeout(c Config) time.Duration {
	if c.RequestTimeout == 0 {
		return defaultRequestTimeout
	}
	return c.RequestTimeout
}

// Acquire takes the Lease object when it is free or its term has expired.
//
// The fencing token is spec.leaseTransitions, which the API server stores with
// the object and every takeover increments. Deleting the Lease resets it, so
// operators must not delete it while the protected resource still trusts old
// tokens.
// Acquire 在 Lease 空闲或租期已过时接管它。
//
// fencing token 取 spec.leaseTransitions：它随对象存储在 API server 上，每次接管
// 加一。删除 Lease 会让它归零，因此只要被保护的资源还认旧 token，运维就不能删
// 这个对象。
func (l *Locker) Acquire(ctx context.Context, key string, ttl time.Duration) (lease.Lease, error) {
	if err := lease.ValidateKey(key); err != nil {
		return nil, err
	}
	if err := lease.ValidateTTL(ttl); err != nil {
		return nil, err
	}
	if ttl > time.Duration(maxLeaseSeconds)*time.Second {
		return nil, fmt.Errorf("%w: %s exceeds the %d-second LeaseDurationSeconds limit", lease.ErrInvalidTTL, ttl, maxLeaseSeconds)
	}
	name, err := objectName(key)
	if err != nil {
		return nil, err
	}
	// Reserve before touching the API: the single-flight decision has to happen
	// before a second caller can observe the same holderIdentity and join it.
	// 先占用再访问 API：单飞决定必须发生在第二个调用方观察到同一个
	// holderIdentity 并"加入"之前。
	if err := l.reserve(key); err != nil {
		return nil, err
	}
	defer l.inflight.Done()
	// One budget covers the whole read-then-write interaction, so a slow API
	// server cannot stretch an Acquire past its caller's patience.
	// 一笔预算覆盖整个"先读后写"交互，慢速 API server 无法把 Acquire 拖过调用方的
	// 忍耐极限。
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	held, err := l.acquireReserved(ctx, key, name, ttl)
	if err != nil {
		l.unreserve(key)
		return nil, err
	}
	// Close may have begun while the API interaction was in flight. Its Wait
	// covers this operation, but the handle must not be delivered: a shutdown
	// that already saw Close return would otherwise still get a usable lease
	// and start new work with it. The Lease object is left alone, exactly like
	// a handle dropped without Release; Close never writes objects.
	// Close 可能在这笔 API 交互在途时开始。Close 的等待覆盖本操作，但句柄不能交付：
	// 否则已经看到 Close 返回的停机流程仍会拿到可用租约并据此开始新工作。Lease 对象
	// 保持原样，与未 Release 就丢弃的句柄完全相同；Close 从不写对象。
	if l.isClosed() {
		l.unreserve(key)
		return nil, fmt.Errorf("%w: acquisition of %q finished after Close", ErrClosed, key)
	}
	return held, nil
}

// isClosed reports whether Close has begun. Acquire's final check shares mu
// with Close, so delivering a handle has a definite order relative to Close.
// isClosed 报告 Close 是否已经开始。Acquire 的最终判定与 Close 共用 mu，交付句柄
// 因此相对 Close 有明确的先后顺序。
func (l *Locker) isClosed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closed
}

// reserve marks key as held by this process. It returns ErrHeld when the key is
// already held, and ErrClosed once the locker was closed.
// reserve 把 key 标记为本进程持有。key 已被持有时返回 ErrHeld，Locker 已关闭时
// 返回 ErrClosed。
func (l *Locker) reserve(key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if _, ok := l.held[key]; ok {
		return fmt.Errorf("%w: %s is already held by this process", lease.ErrHeld, key)
	}
	l.held[key] = struct{}{}
	l.inflight.Add(1)
	return nil
}

// unreserve releases the in-process reservation. It is called on every path
// where this process stops owning the key: an acquisition failure, Release, or
// a Renew that discovered another holder.
// unreserve 释放进程内占用。本进程不再拥有该 key 的每条路径都会调用它：获取失败、
// Release，或 Renew 发现已换持有者。
func (l *Locker) unreserve(key string) {
	l.mu.Lock()
	delete(l.held, key)
	l.mu.Unlock()
}

// beginInflight registers a Renew or Release as an in-flight API operation.
// It returns false once Close has begun: the operation still runs so handles
// taken before Close keep working, but Close no longer waits for it. Checking
// closed here keeps Add from racing with Close's Wait.
// beginInflight 把 Renew 或 Release 登记为在途 API 操作。Close 开始后返回 false：
// 操作照常执行，Close 前取得的句柄仍然可用，只是 Close 不再等待它。在这里检查
// closed，避免 Add 与 Close 的 Wait 竞争。
func (l *Locker) beginInflight() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return false
	}
	l.inflight.Add(1)
	return true
}

// endInflight completes an operation registered by beginInflight.
// endInflight 结束一次由 beginInflight 登记的操作。
func (l *Locker) endInflight(tracked bool) {
	if tracked {
		l.inflight.Done()
	}
}

// Close clears the in-process reservations and rejects further acquisitions.
// An acquisition still in flight when Close begins returns ErrClosed instead
// of a handle. Close waits for API operations already in flight to finish, up
// to RequestTimeout; when that wait runs out it returns ErrCloseTimeout, and
// a later Close call waits again for whatever remains.
//
// It does not write the Lease objects: doing so would race with handles that
// are still in use. A caller that wants to hand a lease back must Release its
// handle before Close; a handle dropped without Release keeps its Lease until
// the term expires, and Close only stops the stale reservation from blocking
// this Locker forever.
// Close 清空进程内占用并拒绝后续获取。Close 开始时仍在途的获取返回 ErrClosed，
// 而不是交付句柄。Close 等待已经在途的 API 操作结束，最多等待 RequestTimeout；
// 等待用尽时返回 ErrCloseTimeout，之后再调用 Close 会继续等待剩余操作。
//
// 它不写 Lease 对象：那样会与仍在使用的句柄竞争。想把租约交还的调用方必须在
// Close 之前 Release 自己的句柄；被丢弃且未 Release 的句柄会让 Lease 一直存在到
// 租期结束，Close 只是让残留占用不再永久阻塞本 Locker。
func (l *Locker) Close() error {
	l.mu.Lock()
	l.closed = true
	l.held = make(map[string]struct{})
	l.mu.Unlock()

	// Do not hold mu while waiting: an in-flight Renew or Release may still
	// need it to drop its reservation. The timeout bounds the wait so a Close
	// issued from inside an in-flight operation cannot hang on itself.
	// 等待时不能持有 mu：在途的 Renew / Release 可能还需要它清理占用。超时给等待
	// 设上限，从在途操作内部调用 Close 时不会把自己永久等死。
	timer := time.NewTimer(l.timeout)
	defer timer.Stop()
	select {
	case <-l.closeWaiter():
		return nil
	case <-timer.C:
		return fmt.Errorf("%w after %s", ErrCloseTimeout, l.timeout)
	}
}

// closeWaiter returns the channel closed when the in-flight set drains. The
// waiter goroutine and its channel are created once and shared by every Close
// call, including the ones that follow a timeout.
// closeWaiter 返回在途集合排空时关闭的 channel。等待 goroutine 与其 channel 只创建一次，
// 由所有 Close 调用共享，包括超时之后的那些调用。
func (l *Locker) closeWaiter() <-chan struct{} {
	l.drainOnce.Do(func() {
		l.drained = make(chan struct{})
		go func() {
			l.inflight.Wait()
			close(l.drained)
		}()
	})
	return l.drained
}

// acquireReserved performs the API interaction for a key this process already
// reserved.
// acquireReserved 对已经占用的 key 执行 API 交互。
func (l *Locker) acquireReserved(ctx context.Context, key, name string, ttl time.Duration) (lease.Lease, error) {
	now := metav1.NewMicroTime(time.Now())
	cur, err := l.api.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return l.create(ctx, key, name, ttl, now)
	}
	if err != nil {
		return nil, fmt.Errorf("k8slease: get %q: %w", name, err)
	}
	return l.takeOrJoin(ctx, key, ttl, now, cur)
}

func (l *Locker) create(ctx context.Context, key, name string, ttl time.Duration, now metav1.MicroTime) (lease.Lease, error) {
	seconds := durationSeconds(ttl)
	obj := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: l.namespace},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &l.identity,
			LeaseDurationSeconds: &seconds,
			AcquireTime:          &now,
			RenewTime:            &now,
			LeaseTransitions:     int32Ptr(1),
		},
	}
	_, err := l.api.Create(ctx, obj, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		// Another instance created it between our read and write.
		// 读与写之间被别人创建了。
		return nil, lease.ErrHeld
	}
	if err != nil {
		return nil, fmt.Errorf("k8slease: create %q: %w", name, err)
	}
	return &heldLease{locker: l, key: key, name: name, token: 1, ttl: ttl}, nil
}

func (l *Locker) takeOrJoin(ctx context.Context, key string, ttl time.Duration, now metav1.MicroTime, cur *coordinationv1.Lease) (lease.Lease, error) {
	holder := derefString(cur.Spec.HolderIdentity)
	token := int64(derefInt32(cur.Spec.LeaseTransitions))
	live := holder != "" && !expired(cur, now.Time)

	if live && holder != l.identity {
		return nil, lease.ErrHeld
	}
	if live {
		// Same holder re-acquiring: refresh the term and keep the token. A
		// process that restarts under the same pod name must not invalidate
		// tokens its own in-flight work already handed to the database.
		// 同一持有者再次获取：刷新租期并沿用原 token。同名 Pod 重启后不能让自己
		// 已经在途的写入所用的 token 失效。
		return l.update(ctx, key, cur, token, now, ttl, holder)
	}
	return l.update(ctx, key, cur, token+1, now, ttl, l.identity)
}

func (l *Locker) update(ctx context.Context, key string, cur *coordinationv1.Lease, token int64, now metav1.MicroTime, ttl time.Duration, holder string) (lease.Lease, error) {
	seconds := durationSeconds(ttl)
	next := cur.DeepCopy()
	next.Spec.HolderIdentity = &holder
	next.Spec.LeaseDurationSeconds = &seconds
	next.Spec.RenewTime = &now
	if holder == l.identity && (cur.Spec.HolderIdentity == nil || *cur.Spec.HolderIdentity != l.identity) {
		next.Spec.AcquireTime = &now
	}
	t := int32(token)
	next.Spec.LeaseTransitions = &t

	_, err := l.api.Update(ctx, next, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		// The resourceVersion moved between read and write, so somebody else
		// won the race. Reporting ErrHeld keeps the caller polling.
		// resourceVersion 在读写之间变了，说明别人赢了这次竞争。报 ErrHeld 让调用方
		// 继续轮询。
		return nil, lease.ErrHeld
	}
	if err != nil {
		return nil, fmt.Errorf("k8slease: update %q: %w", next.Name, err)
	}
	return &heldLease{locker: l, key: key, name: next.Name, token: token, ttl: ttl}, nil
}

// heldLease is one successful acquisition.
//
// Renew and Release serialize on mu. Without it two concurrent Renews would
// read the same resourceVersion and the second Update would fail as a conflict,
// which Renew would then report as ErrLeaseLost: self-inflicted competition
// that drops a lease the process still holds.
// heldLease 表示一次成功的获取。
//
// Renew 与 Release 通过 mu 串行化。没有它，两个并发 Renew 会读到同一个
// resourceVersion，后一个 Update 以冲突失败，Renew 再把它报成 ErrLeaseLost：
// 自己与自己竞争，丢掉了进程其实还持有的租约。
type heldLease struct {
	mu     sync.Mutex
	locker *Locker
	key    string
	name   string
	token  int64
	ttl    time.Duration
}

func (h *heldLease) Key() string  { return h.key }
func (h *heldLease) Token() int64 { return h.token }

func (h *heldLease) String() string { return fmt.Sprintf("%s#%d", h.key, h.token) }

// Renew pushes renewTime forward, refusing once the object names another holder
// or another token.
// Renew 推进 renewTime；对象上的持有者或 token 变了就拒绝。
func (h *heldLease) Renew(ctx context.Context) error {
	tracked := h.locker.beginInflight()
	defer h.locker.endInflight(tracked)

	// Serialize the read-then-write so two renewals of one handle cannot read
	// the same resourceVersion and turn the second into a false ErrLeaseLost.
	// 串行化"先读后写"，同一句柄的两次续期不会读到同一个 resourceVersion，
	// 第二次也就不会变成误报的 ErrLeaseLost。
	h.mu.Lock()
	defer h.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, h.locker.timeout)
	defer cancel()
	cur, err := h.locker.api.Get(ctx, h.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		h.locker.unreserve(h.key)
		return fmt.Errorf("%w: %s", lease.ErrLeaseLost, h)
	}
	if err != nil {
		return fmt.Errorf("k8slease: renew get %q: %w", h.name, err)
	}
	if derefString(cur.Spec.HolderIdentity) != h.locker.identity || int64(derefInt32(cur.Spec.LeaseTransitions)) != h.token {
		// Someone took the key over, so the in-process reservation must go too,
		// otherwise this process could never acquire it again.
		// 该 key 已被接管，进程内占用也必须释放，否则本进程再也获取不到它。
		h.locker.unreserve(h.key)
		return fmt.Errorf("%w: %s", lease.ErrLeaseLost, h)
	}

	now := metav1.NewMicroTime(time.Now())
	seconds := durationSeconds(h.ttl)
	next := cur.DeepCopy()
	next.Spec.RenewTime = &now
	next.Spec.LeaseDurationSeconds = &seconds
	_, err = h.locker.api.Update(ctx, next, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		// A conflict means this renew lost the race. We can no longer prove we
		// hold the lease, so drop the in-process reservation just like the
		// NotFound and changed-holder branches above — otherwise Acquire on this
		// Locker stays stuck in ErrHeld forever after the caller gives up.
		// 冲突意味着这次续期竞争失败，已经无法证明仍持有租约，因此和上面 NotFound、
		// holder 变更两个分支一样释放进程内占用；否则调用方放弃后本 Locker 会永久 ErrHeld。
		h.locker.unreserve(h.key)
		return fmt.Errorf("%w: %s (resource version moved)", lease.ErrLeaseLost, h)
	}
	if err != nil {
		return fmt.Errorf("k8slease: renew %q: %w", h.name, err)
	}
	return nil
}

// Release hands the lease back by clearing the holder and shrinking the term to
// one second, which is the convention the Kubernetes election code uses.
// Release 通过清空持有者并把租期缩到一秒来交还租约，这也是 Kubernetes 选举代码的
// 惯例。
func (h *heldLease) Release(ctx context.Context) error {
	tracked := h.locker.beginInflight()
	defer h.locker.endInflight(tracked)

	// The reservation ends with Release even if the object was already taken
	// over: this process no longer owns the key either way.
	// 无论对象是否已被接管，Release 都结束占用：两种情况下本进程都不再拥有该 key。
	defer h.locker.unreserve(h.key)

	// Share the serialization with Renew: a release racing a renewal would
	// otherwise send an Update built from the same resourceVersion.
	// 与 Renew 共用同一把锁：否则与续期并发的释放会用同一个 resourceVersion
	// 构造 Update。
	h.mu.Lock()
	defer h.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, h.locker.timeout)
	defer cancel()
	cur, err := h.locker.api.Get(ctx, h.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("k8slease: release get %q: %w", h.name, err)
	}
	if derefString(cur.Spec.HolderIdentity) != h.locker.identity || int64(derefInt32(cur.Spec.LeaseTransitions)) != h.token {
		// Already taken over: releasing it would clear the new holder.
		// 已经被接管：再释放会把新持有者清掉。
		return nil
	}

	now := metav1.NewMicroTime(time.Now())
	empty := ""
	seconds := int32(1)
	next := cur.DeepCopy()
	next.Spec.HolderIdentity = &empty
	next.Spec.LeaseDurationSeconds = &seconds
	next.Spec.RenewTime = &now
	if _, err := h.locker.api.Update(ctx, next, metav1.UpdateOptions{}); err != nil {
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("k8slease: release %q: %w", h.name, err)
	}
	return nil
}

// expired reports whether the stored term has run out.
// expired 判断已存储的租期是否用尽。
func expired(l *coordinationv1.Lease, now time.Time) bool {
	if l.Spec.RenewTime == nil || l.Spec.LeaseDurationSeconds == nil {
		return true
	}
	deadline := l.Spec.RenewTime.Add(time.Duration(*l.Spec.LeaseDurationSeconds) * time.Second)
	return !now.Before(deadline)
}

// objectName turns a lease key into a Lease object name, refusing anything that
// would need sanitizing.
// objectName 把租约键转成 Lease 对象名，需要字符替换的一律拒绝。
func objectName(key string) (string, error) {
	if len(key) > 253 || !dnsSubdomain.MatchString(key) {
		return "", fmt.Errorf("%w: %q", ErrInvalidObjectName, key)
	}
	return key, nil
}

func durationSeconds(ttl time.Duration) int32 {
	seconds := int32(math.Ceil(ttl.Seconds()))
	if seconds < 1 {
		seconds = 1
	}
	return seconds
}

func derefString(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func derefInt32(p *int32) int32 {
	if p == nil {
		return 0
	}
	return *p
}

func int32Ptr(v int32) *int32 { return &v }
