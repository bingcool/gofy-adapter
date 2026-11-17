// Package nacos resolves service names through Nacos naming and plugs into
// client.Resolver.
//
//	resolver, err := nacos.NewFromFiles("nacos.yaml", "application.yaml")
//	c, err := client.New(client.Config{
//		Service:  demoservice.ServiceName,
//		Resolver: resolver,
//	})
//	orders := demoserviceclient.New(c)
//
// The resolver caches instance lists for CacheTTL and serves the last good
// list for up to StaleTTL while Nacos is unreachable. A failed lookup is
// throttled for the same CacheTTL, so an outage costs one timeout per window
// instead of one per request.
// 包 nacos 通过 Nacos 命名服务解析服务名，并接入 client.Resolver。
//
// resolver 按 CacheTTL 缓存实例列表；Nacos 不可达时，最多在 StaleTTL 内继续
// 返回最近一次成功的结果。失败的回源同样按 CacheTTL 节流，因此一次故障只在一个窗口内
// 付一次超时，而不是每个请求都付。
package nacos

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gclient "github.com/bingcool/gofy-interface-api/client"
	nacosconfig "github.com/bingcool/gofy/support/nacos"
)

var (
	// ErrNoInstances is returned when Nacos has no available instance for a
	// service.
	// ErrNoInstances 在 Nacos 中该服务没有可用实例时返回。
	ErrNoInstances = errors.New("nacos resolver: no available instances")
	// ErrUnknownLoadBalancer is returned by New for an unsupported strategy.
	// ErrUnknownLoadBalancer 由 New 针对不支持的负载均衡策略返回。
	ErrUnknownLoadBalancer = errors.New("nacos resolver: unknown load balancer")
	// ErrStaleTTLShorterThanCache is returned by New when the stale window is
	// shorter than the cache window: every cache hit after that point would
	// fail closed even though the list was fetched moments ago.
	// ErrStaleTTLShorterThanCache 由 New 在 stale 窗口短于缓存窗口时返回：那种配置下
	// 每次缓存命中都会 fail-closed，哪怕列表刚拉回来。
	ErrStaleTTLShorterThanCache = errors.New("nacos resolver: stale_ttl is shorter than cache_ttl")
	// ErrStaleExpired means there is no usable address left: Nacos is
	// unreachable (or keeps failing) and the last successful list is older than
	// StaleTTL. The wrapped error, when present, is the newest failure.
	// ErrStaleExpired 表示没有可用地址了：Nacos 不可达（或持续失败），且最近一次成功
	// 的列表已经超过 StaleTTL。若存在被包装的错误，那是最近一次失败原因。
	ErrStaleExpired = errors.New("nacos resolver: cached instances expired")
	// ErrAllZeroWeight means the strategy is weight but every candidate has
	// weight <= 0. Nacos uses weight 0 to drain an instance, so this is a
	// deliberate state rather than a missing field: fail closed instead of
	// sending traffic to instances somebody just drained.
	// ErrAllZeroWeight 表示策略是 weight、但所有候选的权重都 <= 0。Nacos 用权重 0 给
	// 实例摘流，所以这是刻意的状态而不是字段缺失：宁可 fail-closed，也不要把流量发给
	// 刚被摘流的实例。
	ErrAllZeroWeight = errors.New("nacos resolver: all instances have weight 0")
)

// Config configures a Resolver. Connection fields mirror nacos.Config;
// discovery fields mirror application.yaml nacos.discovery_service_client.
// Config 配置一个 Resolver。连接字段与 nacos.Config 对应；discovery 字段对应
// application.yaml 中的 nacos.discovery_service_client。
type Config struct {
	Addr      string
	Host      string
	Port      int
	Username  string
	Password  string
	Namespace string

	// Group defaults to the instance group, then DEFAULT_GROUP.
	// Group 默认取实例的 group，仍为空时使用 DEFAULT_GROUP。
	Group string

	// LoadBalancer is random, round_robin or weight. Default random.
	// LoadBalancer 取 random、round_robin 或 weight，默认 random。
	LoadBalancer string

	// CacheTTL keeps an instance list fresh for this long and also throttles
	// retries after a failure. Default 60s.
	// CacheTTL 是实例列表的保鲜时长，同时也是一次失败后的重试节流窗口，默认 60s。
	CacheTTL time.Duration
	// StaleTTL allows serving a stale list while Nacos is down. Default
	// max(3*CacheTTL, 180s); it must not be shorter than CacheTTL.
	// StaleTTL 允许在 Nacos 不可用时继续返回过期列表，默认 max(3*CacheTTL, 180s)；
	// 不得短于 CacheTTL。
	StaleTTL time.Duration
	// Timeout bounds one instance-list request. Default 2s: resolution sits on
	// the request path, so it must not inherit the config-center timeout.
	// Timeout 限制单次实例列表请求，默认 2s：寻址位于请求路径上，不能沿用配置中心
	// 的超时。
	Timeout time.Duration

	// IncludeUnhealthy asks for and returns unhealthy instances too. The
	// default keeps only healthy instances.
	// IncludeUnhealthy 为 true 时同时请求并返回不健康实例；默认只保留健康实例。
	IncludeUnhealthy bool

	// Clusters restricts discovery to the listed clusters.
	// Clusters 把服务发现范围限制在列出的集群内。
	Clusters []string

	// LocalAutoSwitch enables the local-development fallback: when Group has no
	// instance, resolve in FallbackGroup and prefer that instance's
	// inner_external_base_uri metadata. Default false — a cluster caller must
	// never silently borrow another environment.
	// LocalAutoSwitch 打开本地开发兜底：Group 没有实例时改在 FallbackGroup 解析，并优先
	// 使用实例的 inner_external_base_uri 元数据。默认 false——集群内的调用方绝不能悄悄
	// 借用另一个环境。
	LocalAutoSwitch bool

	// FallbackGroup is the deployment group tried when LocalAutoSwitch is on and
	// Group is empty. Empty, or equal to Group, disables the fallback. It is not
	// a separate knob in application.yaml: ConfigFromNacos takes it from
	// service_register.group_name, which is exactly the group this process
	// registers into — the same source the PHP SDK uses.
	// FallbackGroup 是 LocalAutoSwitch 打开且 Group 为空时改查的部署分组；为空或与 Group
	// 相同表示不兜底。它在 application.yaml 里不是独立配置项：ConfigFromNacos 取
	// service_register.group_name——本进程注册进去的那个分组，与 PHP SDK 同源。
	FallbackGroup string
}

// ConfigFromNacos maps a loaded nacos.Config (nacos.yaml + application.yaml)
// onto resolver settings.
// ConfigFromNacos 把已加载的 nacos.Config（nacos.yaml + application.yaml）映射
// 成 Resolver 配置。
func ConfigFromNacos(cfg nacosconfig.Config) Config {
	return Config{
		Addr:     cfg.Addr,
		Host:     cfg.Host,
		Port:     cfg.Port,
		Username: cfg.Username,
		Password: cfg.Password,
		// The discovery block is normalized by Load, but ConfigFromNacos is
		// exported and may receive a hand-built Config: fall back to the
		// connection namespace/group instead of silently querying public.
		// discovery 段由 Load 归一化，但 ConfigFromNacos 是导出函数，可能收到手工
		// 构造的 Config：这里回退到连接的 namespace/group，而不是悄悄去查 public。
		Namespace:        firstNonEmpty(cfg.Discovery.Namespace, cfg.Namespace),
		Group:            firstNonEmpty(cfg.Discovery.Group, cfg.Group),
		LoadBalancer:     cfg.Discovery.LoadBalancer,
		CacheTTL:         cfg.Discovery.CacheTTL,
		StaleTTL:         cfg.Discovery.StaleTTL,
		Timeout:          cfg.Discovery.Timeout,
		IncludeUnhealthy: !cfg.Discovery.HealthyOnlyEnabled(),
		Clusters:         append([]string(nil), cfg.Discovery.Clusters...),
		LocalAutoSwitch:  cfg.Discovery.LocalAutoSwitch,
		// The fallback group is where this process registers, not a third
		// setting: a local process sets its discovery group to its own group
		// and still registers into the deployment one.
		// 兜底分组就是本进程注册进去的那个分组，不是第三个配置：本地进程把 discovery
		// 分组设成自己的分组，注册仍进部署分组。
		FallbackGroup: cfg.Register.Group,
	}
}

// NewFromFiles builds a resolver from the swoolefy-style pair nacos.yaml and
// application.yaml.
// NewFromFiles 从 swoolefy 风格的 nacos.yaml 与 application.yaml 组合构建
// resolver。
func NewFromFiles(nacosPath, appPath string) (*Resolver, error) {
	cfg, err := nacosconfig.Load(nacosPath, appPath)
	if err != nil {
		return nil, err
	}
	return NewFromConfig(cfg)
}

// NewFromConfig builds a resolver from an already loaded nacos.Config. It is the
// one call an assembly root needs; ConfigFromNacos alone still requires New.
// NewFromConfig 用一份已加载的 nacos.Config 构造 resolver。装配根要的就是这一句；
// 只用 ConfigFromNacos 还得再调一次 New。
func NewFromConfig(cfg nacosconfig.Config) (*Resolver, error) {
	return New(ConfigFromNacos(cfg))
}

// Resolver resolves a service name to a base URL using Nacos naming.
// Resolver 使用 Nacos 命名服务把服务名解析成基地址。
type Resolver struct {
	client *nacosconfig.Client
	cfg    Config

	mu    sync.Mutex
	cache map[string]*entry
	rr    map[string]uint64
	// inflight holds one refresh per service+group so a cache expiry under a
	// burst of Resolve calls triggers a single Nacos query. Callers that find a
	// refresh already running wait for it and share the result instead of all
	// hitting Nacos (and, when Nacos is down, all amplifying the outage). The
	// group is part of the key: the local and the fallback lookup must not share
	// one flight, or the second answer would overwrite the first.
	// inflight 为每个 service+group 保留一次刷新，缓存过期时的并发 Resolve 只会触发一次
	// Nacos 查询。发现已有刷新在途的调用方等待并共享结果，而不是一起打向 Nacos（Nacos
	// 故障时也就不会一起放大故障）。分组是键的一部分：本地查询与兜底查询不能共用一次
	// 在途，否则后一个答案会覆盖前一个。
	inflight map[string]*refresh

	// onFallback is set once at assembly time and read on the resolving
	// goroutine, hence the atomic pointer rather than a plain field.
	// onFallback 在装配期设置一次、在解析 goroutine 上读取，因此用原子指针而不是普通字段。
	onFallback atomic.Pointer[func(FallbackEvent)]
}

// refresh is one in-flight Nacos lookup for a service+group. Closing done publishes
// the outcome: waiters then read the cache entry the leader wrote. err is only
// set when the leader gave up because its own caller went away — the one case
// where no cache entry is written at all.
// refresh 是某个 service 的一次在途查询；关闭 done 即发布结果：等待者随后读取 leader
// 写入的缓存条目。只有 leader 因自身调用方取消而放弃、根本没写缓存时，err 才有值。
type refresh struct {
	done chan struct{}
	err  error
}

// entry is one service+group cache slot. Two timestamps, because they answer
// two different questions: lastAttempt throttles lookups (a failure is an answer
// too), lastSuccess bounds how long a stale list may still be served.
// entry 是一个 service+group 的缓存槽。两个时间戳回答两个不同问题：lastAttempt 用来节流
// （失败也是一次回答），lastSuccess 用来限定旧列表还能用多久。
type entry struct {
	instances   []nacosconfig.Instance
	lastAttempt time.Time
	lastSuccess time.Time
	lastErr     error
}

// New validates cfg and builds a resolver.
// New 校验配置并构建 resolver；未知的负载均衡策略会直接返回错误。
func New(cfg Config) (*Resolver, error) {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2 * time.Second
	}
	conn := nacosconfig.Config{
		Addr:      cfg.Addr,
		Host:      cfg.Host,
		Port:      cfg.Port,
		Username:  cfg.Username,
		Password:  cfg.Password,
		Namespace: cfg.Namespace,
		Timeout:   cfg.Timeout,
	}
	client, err := nacosconfig.NewNamingClient(conn)
	if err != nil {
		return nil, fmt.Errorf("nacos resolver: %w", err)
	}
	if cfg.Namespace == "" {
		cfg.Namespace = client.Config().Namespace
	}
	cfg.Group = strings.TrimSpace(cfg.Group)
	if cfg.Group == "" {
		cfg.Group = nacosconfig.DefaultGroup
	}
	cfg.FallbackGroup = strings.TrimSpace(cfg.FallbackGroup)
	cfg.LoadBalancer = normalizeBalancer(cfg.LoadBalancer)
	switch cfg.LoadBalancer {
	case "random", "round_robin", "weight":
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownLoadBalancer, cfg.LoadBalancer)
	}
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = 60 * time.Second
	}
	if cfg.StaleTTL <= 0 {
		cfg.StaleTTL = max(3*cfg.CacheTTL, 180*time.Second)
	}
	// A shorter stale window would fail every cache hit closed. Refuse it
	// instead of quietly widening it: whoever wrote the number should see it.
	// 更短的 stale 窗口会让每次缓存命中都 fail-closed。直接拒绝而不是悄悄放大：
	// 写这个数字的人应该看到错误。
	if cfg.StaleTTL < cfg.CacheTTL {
		return nil, fmt.Errorf("%w: %s < %s", ErrStaleTTLShorterThanCache, cfg.StaleTTL, cfg.CacheTTL)
	}
	return &Resolver{
		client:   client,
		cfg:      cfg,
		cache:    make(map[string]*entry),
		rr:       make(map[string]uint64),
		inflight: make(map[string]*refresh),
	}, nil
}

// Close releases the idle connections held by the naming client. A resolver is
// usually process-scoped, so this matters at shutdown and in tests.
// Close 释放命名客户端持有的空闲连接。resolver 通常是进程级的，因此只在停机与测试
// 里需要。
func (r *Resolver) Close() error {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.Close()
}

// FallbackEvent describes one lookup that had to be answered from the fallback
// group. It is delivered once per refresh (at most once per CacheTTL), not once
// per call, so wiring it to a logger cannot flood the log.
// FallbackEvent 描述一次只能由兜底分组回答的解析。它每次刷新只投递一次（最多每个
// CacheTTL 一次），而不是每次调用一次，因此接到 logger 上不会刷屏。
type FallbackEvent struct {
	// Service is the resolved service name.
	// Service 是被解析的服务名。
	Service string
	// LocalGroup is the group that answered "no instances".
	// LocalGroup 是回答"没有实例"的分组。
	LocalGroup string
	// FallbackGroup is the group that produced the address.
	// FallbackGroup 是给出地址的分组。
	FallbackGroup string
	// BaseURL is the address the caller received.
	// BaseURL 是调用方拿到的地址。
	BaseURL string
	// FromMetadata reports whether BaseURL came from the instance's
	// inner_external_base_uri instead of its IP:port.
	// FromMetadata 表示 BaseURL 来自实例的 inner_external_base_uri，而不是 IP:port。
	FromMetadata bool
}

// SetOnFallback installs the callback for fallback resolutions. Call it at
// assembly time, before the first Resolve; nil removes it.
// SetOnFallback 设置兜底解析的回调。应在装配期、首次 Resolve 之前调用；传 nil 即移除。
func (r *Resolver) SetOnFallback(fn func(FallbackEvent)) {
	if r == nil {
		return
	}
	if fn == nil {
		r.onFallback.Store(nil)
		return
	}
	r.onFallback.Store(&fn)
}

func (r *Resolver) fireFallback(ev FallbackEvent) {
	if p := r.onFallback.Load(); p != nil && *p != nil {
		(*p)(ev)
	}
}

// autoSwitchOn reports whether a second group may be tried at all: the switch
// must be on and a different group must be configured.
// autoSwitchOn 表示是否允许再试一个分组：开关打开，且配置了不同于本地分组的分组。
func (r *Resolver) autoSwitchOn() bool {
	return r.cfg.LocalAutoSwitch &&
		r.cfg.FallbackGroup != "" &&
		r.cfg.FallbackGroup != r.cfg.Group
}

// cacheKey scopes a cache slot to both the service and the group. The group has
// to be part of the key: a fallback result written under the bare service name
// would keep answering for the local group, and the process would never notice
// that its own instances came back.
// cacheKey 把缓存槽限定到"服务 + 分组"。"分组"必须是键的一部分：把兜底结果写在裸服务名
// 下，它会继续替本地分组作答，本进程也就永远不会发现自己的实例已经回来了。
func cacheKey(service, group string) string { return service + "@" + group }

// groupResolution is one group's answer: the address, whether this call
// refreshed it, whether the group said "no instances", and whether the address
// came from instance metadata.
// groupResolution 是一个分组给出的结果：地址、本次是否回源、是否明确"没有实例"，以及
// 地址是否来自实例元数据。
type groupResolution struct {
	base         string
	refreshed    bool
	empty        bool
	fromMetadata bool
}

// groupEmpty reports whether the group answered successfully with no instance.
// Only that answer justifies the fallback group; an all-zero-weight list is a
// deliberate drain, so it must not borrow another environment's instances.
// groupEmpty 表示该分组成功回答"没有实例"。只有这种答案才允许改查兜底分组；权重全为 0
// 是刻意的摘流，不能因此去借另一个环境的实例。
func groupEmpty(e *entry, err error) bool {
	if !errors.Is(err, ErrNoInstances) {
		return false
	}
	return e == nil || len(e.instances) == 0
}

// Resolve implements client.Resolver.
// Resolve 实现 client.Resolver：先用本地分组选址；本地分组明确回答"没有实例"且打开了
// 本地自动切换时，再按部署分组选址，并优先使用实例发布的外部地址元数据。
func (r *Resolver) Resolve(ctx context.Context, service string) (string, error) {
	service = strings.TrimSpace(service)
	if service == "" {
		return "", errors.New("nacos resolver: service name is required")
	}

	res, err := r.resolveInGroup(ctx, service, r.cfg.Group, false)
	if err == nil {
		return res.base, nil
	}
	// Only "this group answered with no instance" justifies a second group. A
	// Nacos failure does not: both groups live behind the same registry, and the
	// stale list has already been tried by the time we get here.
	// 只有"本分组确认没有实例"才值得换一个分组再查。Nacos 故障不算：两个分组在同一个
	// 注册中心后面，而走到这里时 stale 列表已经试过了。
	if !res.empty || !r.autoSwitchOn() {
		return "", err
	}

	fb, fbErr := r.resolveInGroup(ctx, service, r.cfg.FallbackGroup, true)
	if fbErr != nil {
		return "", fmt.Errorf("%w: %q (local group %q is empty; fallback group %q: %w)",
			ErrNoInstances, service, r.cfg.Group, r.cfg.FallbackGroup, fbErr)
	}
	if fb.refreshed {
		r.fireFallback(FallbackEvent{
			Service:       service,
			LocalGroup:    r.cfg.Group,
			FallbackGroup: r.cfg.FallbackGroup,
			BaseURL:       fb.base,
			FromMetadata:  fb.fromMetadata,
		})
	}
	return fb.base, nil
}

// resolveInGroup runs the cache → singleflight → refresh state machine for one
// group. It is the whole of Resolve's body parameterised by group, so the local
// answer and the fallback answer share the throttle, stale and cancellation
// rules instead of growing a second copy of them.
// resolveInGroup 对单个分组跑一遍"缓存 → 单飞 → 回源"状态机。它就是 Resolve 的全部
// 逻辑按分组参数化的结果，因此本地分组与兜底分组共用同一套节流、stale 与取消规则，
// 而不是各写一份。
func (r *Resolver) resolveInGroup(ctx context.Context, service, group string, fallback bool) (groupResolution, error) {
	key := cacheKey(service, group)
	now := time.Now()
	r.mu.Lock()
	if e := r.cache[key]; e != nil && now.Sub(e.lastAttempt) < r.cfg.CacheTTL {
		base, fromMetadata, err := r.serveLocked(key, service, e, now, fallback)
		r.mu.Unlock()
		return groupResolution{base: base, empty: groupEmpty(e, err), fromMetadata: fromMetadata}, err
	}

	// A refresh for this service is already running: wait for it (respecting
	// the caller's context) and read what it published rather than querying
	// Nacos again.
	// 该 service 已有刷新在途：等待它（尊重调用方 context），读取它发布的结果，而不是
	// 再查一次 Nacos。
	if rf := r.inflight[key]; rf != nil {
		r.mu.Unlock()
		select {
		case <-rf.done:
		case <-ctx.Done():
			return groupResolution{}, fmt.Errorf("nacos resolver: %q: waiting for refresh: %w", service, ctx.Err())
		}
		r.mu.Lock()
		e, refreshErr := r.cache[key], rf.err
		base, fromMetadata, err := r.serveLocked(key, service, e, time.Now(), fallback)
		r.mu.Unlock()
		if e == nil && refreshErr != nil {
			// The leader never wrote: report why it gave up instead of
			// pretending the service has no instances.
			// leader 什么都没写：报出它放弃的原因，而不是假装服务没有实例。
			return groupResolution{}, fmt.Errorf("nacos resolver: %q: %w", service, refreshErr)
		}
		return groupResolution{base: base, empty: groupEmpty(e, err), fromMetadata: fromMetadata}, err
	}

	// Become the leader for this service.
	// 成为该 service 的刷新 leader。
	rf := &refresh{done: make(chan struct{})}
	r.inflight[key] = rf
	r.mu.Unlock()

	instances, listErr := r.list(ctx, service, group)
	if listErr == nil {
		instances = r.filter(instances)
	}
	completed := time.Now()

	r.mu.Lock()
	if listErr != nil && ctx.Err() != nil {
		// The caller went away, so this attempt says nothing about Nacos:
		// neither throttle the next caller nor overwrite the last good entry.
		// 调用方已经离开，这次尝试说明不了 Nacos 的状态：既不节流下一个调用方，也不
		// 覆盖上一条可用记录。
		rf.err = listErr
		delete(r.inflight, key)
		close(rf.done)
		r.mu.Unlock()
		return groupResolution{}, fmt.Errorf("nacos resolver: %q: %w", service, listErr)
	}

	// Timestamp the attempt when it completes, not when it started: a slow
	// ListInstances would otherwise look older than it is and expire early.
	// 用回源完成时刻打时间戳，而不是开始时刻：慢的 ListInstances 否则会显得比实际
	// 更旧、提前过期。
	e := &entry{instances: instances, lastAttempt: completed}
	if listErr == nil {
		// A successful empty list is authoritative: it replaces the previous
		// addresses at once, and the throttle keeps a deploy window from
		// turning every request into a Nacos query.
		// 成功的空列表是权威结果：它立刻覆盖旧地址，同时由节流窗口避免发布期间把每个
		// 请求都变成一次 Nacos 查询。
		e.lastSuccess = completed
	} else {
		e.lastErr = listErr
		if prev := r.cache[key]; prev != nil {
			e.instances = prev.instances
			e.lastSuccess = prev.lastSuccess
		}
	}
	r.cache[key] = e
	delete(r.inflight, key)
	close(rf.done)
	base, fromMetadata, err := r.serveLocked(key, service, e, completed, fallback)
	r.mu.Unlock()
	return groupResolution{base: base, refreshed: true, empty: groupEmpty(e, err), fromMetadata: fromMetadata}, err
}

// serveLocked turns a cache entry into an address. It is the only place that
// decides between "fresh", "stale but usable" and "no usable address", so the
// leader, the waiters and the throttled fast path behave identically. The
// caller must hold r.mu; choosing under the lock is what advances the
// round-robin cursor once per resolved call. preferMetadata is true only on the
// fallback-group path, where the instance's registered pod address may be
// unroutable from here.
// serveLocked 把缓存条目转换成地址。它是唯一判断「新鲜 / 过期但可用 / 没有可用地址」的
// 地方，因此 leader、等待者与被节流的快速路径行为一致。调用方必须持有 r.mu；在锁内选址
// 才能让 round-robin 游标按解析次数推进。preferMetadata 只在兜底分组路径上为 true：
// 那条路径上实例注册的 Pod 地址可能从这里路由不到。
func (r *Resolver) serveLocked(key, service string, e *entry, now time.Time, preferMetadata bool) (string, bool, error) {
	if e == nil {
		return "", false, fmt.Errorf("%w: %q", ErrNoInstances, service)
	}
	if len(e.instances) > 0 && now.Sub(e.lastSuccess) <= r.cfg.StaleTTL {
		inst, err := r.chooseLocked(key, e.instances)
		if err != nil {
			return "", false, fmt.Errorf("%w: %q: %w", ErrNoInstances, service, err)
		}
		base, fromMetadata := baseURL(inst, preferMetadata)
		return base, fromMetadata, nil
	}
	if len(e.instances) == 0 && e.lastErr == nil {
		return "", false, fmt.Errorf("%w: %q", ErrNoInstances, service)
	}
	if e.lastErr != nil {
		// Never had a good list: report the failure itself. "Expired cache"
		// would be misleading when there is no cache to expire.
		// 从未成功过：直接报失败本身。没有缓存可过期时，说"缓存过期"是误导。
		if e.lastSuccess.IsZero() {
			return "", false, fmt.Errorf("nacos resolver: %q: %w", service, e.lastErr)
		}
		return "", false, fmt.Errorf("%w: %q: %w", ErrStaleExpired, service, e.lastErr)
	}
	return "", false, fmt.Errorf("%w: %q (last successful refresh %s ago)",
		ErrStaleExpired, service, now.Sub(e.lastSuccess).Round(time.Second))
}

func (r *Resolver) list(ctx context.Context, service, group string) ([]nacosconfig.Instance, error) {
	return r.client.ListInstances(ctx, nacosconfig.InstanceQuery{
		ServiceName: service,
		Group:       group,
		Namespace:   r.cfg.Namespace,
		Clusters:    r.cfg.Clusters,
		HealthyOnly: !r.cfg.IncludeUnhealthy,
	})
}

func (r *Resolver) filter(instances []nacosconfig.Instance) []nacosconfig.Instance {
	if !r.cfg.IncludeUnhealthy {
		return nacosconfig.AvailableInstances(instances)
	}
	out := make([]nacosconfig.Instance, 0, len(instances))
	for _, inst := range instances {
		if inst.IP != "" && inst.Port > 0 && inst.Enabled {
			out = append(out, inst)
		}
	}
	return out
}

// chooseLocked picks one instance. The cursor key includes the group so a
// fallback lookup does not advance the local group's round-robin position.
// chooseLocked 选一个实例。游标键包含分组，因此兜底查询不会推进本地分组的轮询位置。
func (r *Resolver) chooseLocked(key string, instances []nacosconfig.Instance) (nacosconfig.Instance, error) {
	switch r.cfg.LoadBalancer {
	case "round_robin":
		index := r.rr[key] % uint64(len(instances))
		r.rr[key]++
		return instances[index], nil
	case "weight":
		return weightedPick(instances)
	default:
		return instances[rand.IntN(len(instances))], nil
	}
}

// weightedPick picks by weight, skipping instances whose weight is <= 0. Zero is
// not "unset": Nacos reports the default as 1.0, so a zero means an operator
// drained the instance. Only the weight strategy honours this; random and
// round_robin still include drained instances, which is why draining is a
// weight-strategy operation.
// weightedPick 按权重抽取，跳过权重 <= 0 的实例。0 不是"没设置"：Nacos 会把默认值报成
// 1.0，所以 0 意味着运维把这个实例摘流了。只有 weight 策略遵守这一点；random 与
// round_robin 仍会选中被摘流的实例，因此摘流是 weight 策略下的操作。
func weightedPick(instances []nacosconfig.Instance) (nacosconfig.Instance, error) {
	total := 0.0
	for _, inst := range instances {
		if inst.Weight > 0 {
			total += inst.Weight
		}
	}
	if total <= 0 {
		return nacosconfig.Instance{}, ErrAllZeroWeight
	}
	target := rand.Float64() * total
	for _, inst := range instances {
		if inst.Weight <= 0 {
			continue
		}
		target -= inst.Weight
		if target <= 0 {
			return inst, nil
		}
	}
	// Float rounding can leave target a hair above zero: take the last eligible
	// instance, not the last instance in the slice.
	// 浮点误差可能让 target 略大于 0：取最后一个"有资格"的实例，而不是切片里最后一个。
	for i := len(instances) - 1; i >= 0; i-- {
		if instances[i].Weight > 0 {
			return instances[i], nil
		}
	}
	return nacosconfig.Instance{}, ErrAllZeroWeight
}

// baseURL renders an instance address. With preferMetadata (the fallback-group
// path) a valid inner_external_base_uri wins: the address the instance
// registered may be a pod IP that is unroutable from here. An invalid value is
// ignored for that instance only — letting it through would fail the whole
// service at client.validateResolvedBase.
// baseURL 渲染实例地址。preferMetadata（兜底分组路径）为 true 时，合法的
// inner_external_base_uri 优先：实例注册的地址可能是从这里路由不到的 Pod IP。非法值只
// 对那个实例无效——放它过去会让整个服务在 client.validateResolvedBase 处失败。
func baseURL(inst nacosconfig.Instance, preferMetadata bool) (string, bool) {
	if preferMetadata {
		if uri, ok := metadataBaseURL(inst.Metadata); ok {
			return uri, true
		}
	}
	scheme := strings.TrimSpace(inst.Metadata["scheme"])
	if scheme == "" {
		scheme = "http"
	}
	return scheme + "://" + net.JoinHostPort(inst.IP, strconv.Itoa(inst.Port)), false
}

// metadataBaseURL validates inner_external_base_uri. A path is allowed (a
// gateway prefix is a legitimate base URL) but a query, fragment or userinfo is
// not: the client concatenates the route path onto this value.
// metadataBaseURL 校验 inner_external_base_uri。允许带 path（网关前缀是合法的 base
// URL），但不允许 query、fragment 或 userinfo：客户端会把路由路径拼在这个值后面。
func metadataBaseURL(metadata map[string]string) (string, bool) {
	raw := strings.TrimSpace(metadata[nacosconfig.MetadataInnerExternalBaseURI])
	if raw == "" {
		return "", false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", false
	}
	return strings.TrimRight(raw, "/"), true
}

func normalizeBalancer(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "random":
		return "random"
	case "round_robin", "round-robin", "roundrobin":
		return "round_robin"
	case "weight", "weighted":
		return "weight"
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v = strings.TrimSpace(v); v != "" {
			return v
		}
	}
	return ""
}

var _ gclient.Resolver = (*Resolver)(nil)
