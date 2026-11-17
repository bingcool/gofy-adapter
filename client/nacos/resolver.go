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
	"strconv"
	"strings"
	"sync"
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
		IncludeUnhealthy: !cfg.Discovery.HealthyOnly,
		Clusters:         append([]string(nil), cfg.Discovery.Clusters...),
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
	// inflight holds one refresh per service so a cache expiry under a burst of
	// Resolve calls triggers a single Nacos query. Callers that find a refresh
	// already running wait for it and share the result instead of all hitting
	// Nacos (and, when Nacos is down, all amplifying the outage).
	// inflight 为每个 service 保留一次刷新，缓存过期时的并发 Resolve 只会触发一次 Nacos 查询。
	// 发现已有刷新在途的调用方等待并共享结果，而不是一起打向 Nacos（Nacos 故障时也就不会
	// 一起放大故障）。
	inflight map[string]*refresh
}

// refresh is one in-flight Nacos lookup for a service. Closing done publishes
// the outcome: waiters then read the cache entry the leader wrote. err is only
// set when the leader gave up because its own caller went away — the one case
// where no cache entry is written at all.
// refresh 是某个 service 的一次在途查询；关闭 done 即发布结果：等待者随后读取 leader
// 写入的缓存条目。只有 leader 因自身调用方取消而放弃、根本没写缓存时，err 才有值。
type refresh struct {
	done chan struct{}
	err  error
}

// entry is one service's cache slot. Two timestamps, because they answer two
// different questions: lastAttempt throttles lookups (a failure is an answer
// too), lastSuccess bounds how long a stale list may still be served.
// entry 是一个 service 的缓存槽。两个时间戳回答两个不同问题：lastAttempt 用来节流
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

// Resolve implements client.Resolver.
// Resolve 实现 client.Resolver：缓存未过期时直接选址，否则回源 Nacos；同一 service 的并发
// 回源会被合并成一次查询。失败的回源同样写进缓存，因此一次故障只在每个窗口里付一次超时。
func (r *Resolver) Resolve(ctx context.Context, service string) (string, error) {
	service = strings.TrimSpace(service)
	if service == "" {
		return "", errors.New("nacos resolver: service name is required")
	}

	now := time.Now()
	r.mu.Lock()
	if e := r.cache[service]; e != nil && now.Sub(e.lastAttempt) < r.cfg.CacheTTL {
		base, err := r.serveLocked(service, e, now)
		r.mu.Unlock()
		return base, err
	}

	// A refresh for this service is already running: wait for it (respecting
	// the caller's context) and read what it published rather than querying
	// Nacos again.
	// 该 service 已有刷新在途：等待它（尊重调用方 context），读取它发布的结果，而不是
	// 再查一次 Nacos。
	if rf := r.inflight[service]; rf != nil {
		r.mu.Unlock()
		select {
		case <-rf.done:
		case <-ctx.Done():
			return "", fmt.Errorf("nacos resolver: %q: waiting for refresh: %w", service, ctx.Err())
		}
		r.mu.Lock()
		e, refreshErr := r.cache[service], rf.err
		base, err := r.serveLocked(service, e, time.Now())
		r.mu.Unlock()
		if e == nil && refreshErr != nil {
			// The leader never wrote: report why it gave up instead of
			// pretending the service has no instances.
			// leader 什么都没写：报出它放弃的原因，而不是假装服务没有实例。
			return "", fmt.Errorf("nacos resolver: %q: %w", service, refreshErr)
		}
		return base, err
	}

	// Become the leader for this service.
	// 成为该 service 的刷新 leader。
	rf := &refresh{done: make(chan struct{})}
	r.inflight[service] = rf
	r.mu.Unlock()

	instances, listErr := r.list(ctx, service)
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
		delete(r.inflight, service)
		close(rf.done)
		r.mu.Unlock()
		return "", fmt.Errorf("nacos resolver: %q: %w", service, listErr)
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
		if prev := r.cache[service]; prev != nil {
			e.instances = prev.instances
			e.lastSuccess = prev.lastSuccess
		}
	}
	r.cache[service] = e
	delete(r.inflight, service)
	close(rf.done)
	base, err := r.serveLocked(service, e, completed)
	r.mu.Unlock()
	return base, err
}

// serveLocked turns a cache entry into an address. It is the only place that
// decides between "fresh", "stale but usable" and "no usable address", so the
// leader, the waiters and the throttled fast path behave identically. The
// caller must hold r.mu; choosing under the lock is what advances the
// round-robin cursor once per resolved call.
// serveLocked 把缓存条目转换成地址。它是唯一判断「新鲜 / 过期但可用 / 没有可用地址」的
// 地方，因此 leader、等待者与被节流的快速路径行为一致。调用方必须持有 r.mu；在锁内选址
// 才能让 round-robin 游标按解析次数推进。
func (r *Resolver) serveLocked(service string, e *entry, now time.Time) (string, error) {
	if e == nil {
		return "", fmt.Errorf("%w: %q", ErrNoInstances, service)
	}
	if len(e.instances) > 0 && now.Sub(e.lastSuccess) <= r.cfg.StaleTTL {
		return baseURL(r.chooseLocked(service, e.instances)), nil
	}
	if len(e.instances) == 0 && e.lastErr == nil {
		return "", fmt.Errorf("%w: %q", ErrNoInstances, service)
	}
	if e.lastErr != nil {
		// Never had a good list: report the failure itself. "Expired cache"
		// would be misleading when there is no cache to expire.
		// 从未成功过：直接报失败本身。没有缓存可过期时，说"缓存过期"是误导。
		if e.lastSuccess.IsZero() {
			return "", fmt.Errorf("nacos resolver: %q: %w", service, e.lastErr)
		}
		return "", fmt.Errorf("%w: %q: %w", ErrStaleExpired, service, e.lastErr)
	}
	return "", fmt.Errorf("%w: %q (last successful refresh %s ago)",
		ErrStaleExpired, service, now.Sub(e.lastSuccess).Round(time.Second))
}

func (r *Resolver) list(ctx context.Context, service string) ([]nacosconfig.Instance, error) {
	return r.client.ListInstances(ctx, nacosconfig.InstanceQuery{
		ServiceName: service,
		Group:       r.cfg.Group,
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

func (r *Resolver) chooseLocked(service string, instances []nacosconfig.Instance) nacosconfig.Instance {
	switch r.cfg.LoadBalancer {
	case "round_robin":
		index := r.rr[service] % uint64(len(instances))
		r.rr[service]++
		return instances[index]
	case "weight":
		return weightedPick(instances)
	default:
		return instances[rand.IntN(len(instances))]
	}
}

func weightedPick(instances []nacosconfig.Instance) nacosconfig.Instance {
	total := 0.0
	for _, inst := range instances {
		total += weight(inst.Weight)
	}
	if total <= 0 {
		return instances[rand.IntN(len(instances))]
	}
	target := rand.Float64() * total
	for _, inst := range instances {
		target -= weight(inst.Weight)
		if target <= 0 {
			return inst
		}
	}
	return instances[len(instances)-1]
}

func weight(value float64) float64 {
	if value <= 0 {
		return 1
	}
	return value
}

func baseURL(inst nacosconfig.Instance) string {
	scheme := strings.TrimSpace(inst.Metadata["scheme"])
	if scheme == "" {
		scheme = "http"
	}
	return scheme + "://" + net.JoinHostPort(inst.IP, strconv.Itoa(inst.Port))
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
