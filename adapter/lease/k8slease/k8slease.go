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
}

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

// dnsSubdomain is the name rule for Lease objects.
// dnsSubdomain 是 Lease 对象的命名规则。
var dnsSubdomain = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

// Locker acquires leases through the Kubernetes API.
// Locker 通过 Kubernetes API 获取租约。
type Locker struct {
	api       LeaseAPI
	namespace string
	identity  string
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
	client, err := coordclient.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("k8slease: coordination client: %w", err)
	}
	return &Locker{api: client.Leases(c.Namespace), namespace: c.Namespace, identity: c.Identity}, nil
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
	return &Locker{api: api, namespace: c.Namespace, identity: c.Identity}, nil
}

func validate(c Config) error {
	if c.Namespace == "" {
		return fmt.Errorf("k8slease: namespace is required")
	}
	if c.Identity == "" {
		return fmt.Errorf("k8slease: identity is required")
	}
	return nil
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
	name, err := objectName(key)
	if err != nil {
		return nil, err
	}

	now := metav1.NewMicroTime(time.Now())
	cur, err := l.api.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return l.create(ctx, key, name, ttl, now)
	}
	if err != nil {
		return nil, fmt.Errorf("k8slease: get %q: %w", name, err)
	}

	held, err := l.takeOrJoin(ctx, key, ttl, now, cur)
	if err != nil {
		return nil, err
	}
	return held, nil
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
	created, err := l.api.Create(ctx, obj, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		// Another instance created it between our read and write.
		// 读与写之间被别人创建了。
		return nil, lease.ErrHeld
	}
	if err != nil {
		return nil, fmt.Errorf("k8slease: create %q: %w", name, err)
	}
	return &heldLease{locker: l, key: key, name: name, token: 1, ttl: ttl, version: created.ResourceVersion}, nil
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

	updated, err := l.api.Update(ctx, next, metav1.UpdateOptions{})
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
	return &heldLease{locker: l, key: key, name: next.Name, token: token, ttl: ttl, version: updated.ResourceVersion}, nil
}

// heldLease is one successful acquisition.
// heldLease 表示一次成功的获取。
type heldLease struct {
	locker  *Locker
	key     string
	name    string
	token   int64
	ttl     time.Duration
	version string
}

func (h *heldLease) Key() string  { return h.key }
func (h *heldLease) Token() int64 { return h.token }

func (h *heldLease) String() string { return fmt.Sprintf("%s#%d", h.key, h.token) }

// Renew pushes renewTime forward, refusing once the object names another holder
// or another token.
// Renew 推进 renewTime；对象上的持有者或 token 变了就拒绝。
func (h *heldLease) Renew(ctx context.Context) error {
	cur, err := h.locker.api.Get(ctx, h.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("%w: %s", lease.ErrLeaseLost, h)
	}
	if err != nil {
		return fmt.Errorf("k8slease: renew get %q: %w", h.name, err)
	}
	if derefString(cur.Spec.HolderIdentity) != h.locker.identity || int64(derefInt32(cur.Spec.LeaseTransitions)) != h.token {
		return fmt.Errorf("%w: %s", lease.ErrLeaseLost, h)
	}

	now := metav1.NewMicroTime(time.Now())
	seconds := durationSeconds(h.ttl)
	next := cur.DeepCopy()
	next.Spec.RenewTime = &now
	next.Spec.LeaseDurationSeconds = &seconds
	updated, err := h.locker.api.Update(ctx, next, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		return fmt.Errorf("%w: %s (resource version moved)", lease.ErrLeaseLost, h)
	}
	if err != nil {
		return fmt.Errorf("k8slease: renew %q: %w", h.name, err)
	}
	h.version = updated.ResourceVersion
	return nil
}

// Release hands the lease back by clearing the holder and shrinking the term to
// one second, which is the convention the Kubernetes election code uses.
// Release 通过清空持有者并把租期缩到一秒来交还租约，这也是 Kubernetes 选举代码的
// 惯例。
func (h *heldLease) Release(ctx context.Context) error {
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
