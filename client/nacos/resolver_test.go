package nacos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	nacosconfig "github.com/bingcool/gofy/support/nacos"
)

const (
	testListPath     = "/nacos/v1/ns/instance/list"
	testInstancePath = "/nacos/v1/ns/instance"
	testBeatPath     = "/nacos/v1/ns/instance/beat"
)

type fakeNaming struct {
	mu sync.Mutex

	hosts   []nacosconfig.Instance
	fail    bool
	calls   int
	queries []url.Values

	// groupHosts, when non-nil, answers per group (keyed by groupName) instead
	// of the flat hosts list. A resolver that forgets to pass the fallback
	// group then reads the wrong set, and the test fails instead of passing by
	// accident.
	// groupHosts 非 nil 时按 groupName 分组返回，而不是使用扁平的 hosts。resolver 忘记
	// 传兜底分组时会读到错的那一份，测试因此会失败而不是碰巧通过。
	groupHosts map[string][]nacosconfig.Instance

	// failGroups fails one group's list requests while the others keep working,
	// which is how a per-group registry failure is modelled.
	// failGroups 让单个分组的列表请求失败，其他分组照常工作，用来模拟按分组发生的
	// 注册中心故障。
	failGroups map[string]bool

	// listGate, when set, blocks the first list request until released. It lets
	// a test hold one refresh in flight while other Resolve calls pile up.
	// listGate 设置后会阻塞第一次 list 请求直到被释放，用于让一个刷新保持在途、
	// 其他 Resolve 调用同时涌进来。
	listGate *testGate
}

type testGate struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newTestGate() *testGate {
	return &testGate{started: make(chan struct{}, 1), release: make(chan struct{})}
}

// wait blocks only the first caller until release is closed.
// wait 只阻塞第一个调用方，直到 release 被关闭。
func (g *testGate) wait() {
	g.once.Do(func() {
		g.started <- struct{}{}
		<-g.release
	})
}

func (f *fakeNaming) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Block the first list request before taking the mutex, so other Resolve
	// calls can pile up in the singleflight while this one is in flight.
	if r.URL.Path == testListPath && f.listGate != nil {
		f.listGate.wait()
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch r.URL.Path {
	case testListPath:
		f.calls++
		f.queries = append(f.queries, r.URL.Query())
		if f.fail || f.failGroups[r.URL.Query().Get("groupName")] {
			http.Error(w, "boom", http.StatusServiceUnavailable)
			return
		}
		hosts := f.hosts
		if f.groupHosts != nil {
			hosts = f.groupHosts[r.URL.Query().Get("groupName")]
		}
		payload, err := json.Marshal(map[string]any{"hosts": hosts})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(payload)

	case testInstancePath:
		query := r.URL.Query()
		port, _ := strconv.Atoi(query.Get("port"))
		switch r.Method {
		case http.MethodPost:
			inst := nacosconfig.Instance{
				IP:          query.Get("ip"),
				Port:        port,
				Weight:      1,
				Healthy:     query.Get("healthy") == "true",
				Enabled:     query.Get("enabled") == "true",
				Ephemeral:   query.Get("ephemeral") == "true",
				ClusterName: query.Get("clusterName"),
				ServiceName: query.Get("serviceName"),
				Group:       query.Get("groupName"),
				Namespace:   query.Get("namespaceId"),
			}
			replaced := false
			for i := range f.hosts {
				if f.hosts[i].IP == inst.IP && f.hosts[i].Port == inst.Port {
					f.hosts[i] = inst
					replaced = true
				}
			}
			if !replaced {
				f.hosts = append(f.hosts, inst)
			}
		case http.MethodDelete:
			kept := f.hosts[:0]
			for _, host := range f.hosts {
				if host.IP == query.Get("ip") && host.Port == port {
					continue
				}
				kept = append(kept, host)
			}
			f.hosts = kept
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		fmt.Fprint(w, "ok")

	case testBeatPath:
		fmt.Fprint(w, `{"clientBeatInterval":3000,"lightBeatEnabled":false,"code":10200}`)

	default:
		http.NotFound(w, r)
	}
}

func (f *fakeNaming) setHosts(hosts ...nacosconfig.Instance) {
	f.mu.Lock()
	f.hosts = hosts
	f.mu.Unlock()
}

// setGroup replaces one group's answer. Calling it with no hosts makes that
// group answer "no instances".
// setGroup 替换某个分组的返回；不传 hosts 即让该分组回答"没有实例"。
func (f *fakeNaming) setGroup(group string, hosts ...nacosconfig.Instance) {
	f.mu.Lock()
	if f.groupHosts == nil {
		f.groupHosts = make(map[string][]nacosconfig.Instance)
	}
	f.groupHosts[group] = hosts
	f.mu.Unlock()
}

// groupCalls counts the list queries issued for one group.
// groupCalls 统计某个分组被查询了几次。
func (f *fakeNaming) groupCalls(group string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, q := range f.queries {
		if q.Get("groupName") == group {
			n++
		}
	}
	return n
}

func (f *fakeNaming) setFail(fail bool) {
	f.mu.Lock()
	f.fail = fail
	f.mu.Unlock()
}

// setGroupFail fails or recovers one group's list requests.
// setGroupFail 让某个分组的列表请求失败或恢复。
func (f *fakeNaming) setGroupFail(group string, fail bool) {
	f.mu.Lock()
	if f.failGroups == nil {
		f.failGroups = make(map[string]bool)
	}
	f.failGroups[group] = fail
	f.mu.Unlock()
}

func (f *fakeNaming) snapshot() (calls int, queries []url.Values) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, append([]url.Values(nil), f.queries...)
}

func newTestResolver(t *testing.T, cfg Config) (*Resolver, *fakeNaming) {
	t.Helper()
	fake := &fakeNaming{}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	if cfg.Addr == "" {
		cfg.Addr = server.URL
	}
	resolver, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return resolver, fake
}

// TestResolveCoalescesConcurrentRefresh verifies the per-service singleflight:
// N concurrent Resolve calls on an empty cache produce one Nacos query and all
// callers get a usable address. Without coalescing each caller would hit Nacos.
// TestResolveCoalescesConcurrentRefresh 验证 per-service 单飞：空缓存下 N 个并发 Resolve 只产生
// 一次 Nacos 查询，且每个调用方都拿到可用地址。没有合并时每个调用方都会打向 Nacos。
func TestResolveCoalescesConcurrentRefresh(t *testing.T) {
	gate := newTestGate()
	fake := &fakeNaming{
		hosts:    []nacosconfig.Instance{healthyInstance("10.0.0.7", 8080, 1)},
		listGate: gate,
	}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	resolver, err := New(Config{Addr: server.URL, Namespace: "dev", CacheTTL: time.Minute})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	const n = 32
	results := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = resolver.Resolve(context.Background(), "orders")
		}(i)
	}

	close(start)
	// Wait until the leader's list request is actually in flight, then let the
	// other callers reach the singleflight before releasing it.
	// 等到 leader 的 list 请求确实在途，再让其他调用方进入单飞，然后放行。
	<-gate.started
	time.Sleep(50 * time.Millisecond)
	close(gate.release)
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("Resolve[%d]: %v", i, errs[i])
		}
		if results[i] != "http://10.0.0.7:8080" {
			t.Fatalf("Resolve[%d] = %q", i, results[i])
		}
	}
	if calls, _ := fake.snapshot(); calls != 1 {
		t.Fatalf("list calls = %d, want 1 for %d concurrent Resolve", calls, n)
	}
}

func healthyInstance(ip string, port int, weight float64) nacosconfig.Instance {
	return nacosconfig.Instance{
		IP:      ip,
		Port:    port,
		Weight:  weight,
		Healthy: true,
		Enabled: true,
	}
}

func TestResolveCachesInstances(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		Namespace: "dev",
		Group:     "gofy-group",
		CacheTTL:  time.Minute,
	})
	fake.setHosts(healthyInstance("10.0.0.1", 8080, 1), healthyInstance("10.0.0.2", 8081, 1))

	first, err := resolver.Resolve(context.Background(), "demo-service")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	second, err := resolver.Resolve(context.Background(), "demo-service")
	if err != nil {
		t.Fatalf("Resolve again: %v", err)
	}
	if first != "http://10.0.0.1:8080" && first != "http://10.0.0.2:8081" {
		t.Fatalf("first = %q", first)
	}
	if second != "http://10.0.0.1:8080" && second != "http://10.0.0.2:8081" {
		t.Fatalf("second = %q", second)
	}
	if calls, _ := fake.snapshot(); calls != 1 {
		t.Fatalf("list calls = %d, want 1 within CacheTTL", calls)
	}
}

func TestResolveRoundRobin(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		LoadBalancer: "round_robin",
		CacheTTL:     time.Minute,
	})
	fake.setHosts(healthyInstance("10.0.0.1", 8080, 1), healthyInstance("10.0.0.2", 8081, 1))

	first, err := resolver.Resolve(context.Background(), "demo-service")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	second, err := resolver.Resolve(context.Background(), "demo-service")
	if err != nil {
		t.Fatalf("Resolve again: %v", err)
	}
	if first != "http://10.0.0.1:8080" || second != "http://10.0.0.2:8081" {
		t.Fatalf("round robin = %q, %q", first, second)
	}
}

func TestResolveFiltersUnhealthy(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{Namespace: "dev", CacheTTL: time.Minute})
	unhealthy := healthyInstance("10.0.0.2", 8081, 1)
	unhealthy.Healthy = false
	fake.setHosts(healthyInstance("10.0.0.1", 8080, 1), unhealthy)

	base, err := resolver.Resolve(context.Background(), "demo-service")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if base != "http://10.0.0.1:8080" {
		t.Fatalf("base = %q, want the healthy instance", base)
	}
	_, queries := fake.snapshot()
	if got := queries[0].Get("healthyOnly"); got != "true" {
		t.Fatalf("healthyOnly = %q, want true", got)
	}
	if got := queries[0].Get("namespaceId"); got != "dev" {
		t.Fatalf("namespaceId = %q", got)
	}
	if got := queries[0].Get("groupName"); got != nacosconfig.DefaultGroup {
		t.Fatalf("groupName = %q", got)
	}
}

func TestResolveServesStaleWhileNacosIsDown(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		CacheTTL: 10 * time.Millisecond,
		StaleTTL: time.Hour,
	})
	fake.setHosts(healthyInstance("10.0.0.1", 8080, 1))

	first, err := resolver.Resolve(context.Background(), "demo-service")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	fake.setFail(true)
	time.Sleep(20 * time.Millisecond)

	base, err := resolver.Resolve(context.Background(), "demo-service")
	if err != nil {
		t.Fatalf("stale Resolve: %v", err)
	}
	if base != first {
		t.Fatalf("stale base = %q, want %q", base, first)
	}
}

// A failed refresh is an answer too: it must throttle the next lookups, or an
// unreachable Nacos would add one timeout to every single request that misses
// the cache. 100 Resolve calls after the failure may only produce one lookup.
// 失败的回源也是一次回答：它必须节流后续查询，否则 Nacos 不可达会给每个未命中缓存的
// 请求都加一次超时。失败之后的 100 次 Resolve 只允许产生一次回源。
func TestResolveThrottlesFailedRefresh(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		CacheTTL: 50 * time.Millisecond,
		StaleTTL: time.Hour,
	})
	fake.setHosts(healthyInstance("10.0.0.1", 8080, 1))
	first, err := resolver.Resolve(context.Background(), "demo-service")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	fake.setFail(true)
	time.Sleep(60 * time.Millisecond)
	for i := 0; i < 100; i++ {
		base, err := resolver.Resolve(context.Background(), "demo-service")
		if err != nil {
			t.Fatalf("Resolve[%d]: %v", i, err)
		}
		if base != first {
			t.Fatalf("Resolve[%d] = %q, want stale %q", i, base, first)
		}
	}
	if calls, _ := fake.snapshot(); calls != 2 {
		t.Fatalf("list calls = %d, want 2 (one success, one throttled failure)", calls)
	}
}

// A successful empty list replaces the old addresses at once and is cached for
// CacheTTL: a deploy window must not turn every request into a Nacos query.
// 成功的空列表立刻覆盖旧地址，并按 CacheTTL 缓存：发布窗口不该把每个请求都变成一次
// Nacos 查询。
func TestResolveNegativeCachesEmptyList(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		CacheTTL: 80 * time.Millisecond,
		StaleTTL: time.Hour,
	})
	fake.setHosts(healthyInstance("10.0.0.1", 8080, 1))
	if _, err := resolver.Resolve(context.Background(), "demo-service"); err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	fake.setHosts()
	// Wait for the positive entry to expire, otherwise the call is answered
	// from cache and never sees the emptied registry.
	// 等正向缓存过期，否则这次调用直接命中缓存，看不到已经清空的注册表。
	time.Sleep(100 * time.Millisecond)
	if _, err := resolver.Resolve(context.Background(), "demo-service"); !errors.Is(err, ErrNoInstances) {
		t.Fatalf("err after emptying = %v, want ErrNoInstances", err)
	}
	if calls, _ := fake.snapshot(); calls != 2 {
		t.Fatalf("list calls = %d, want 2", calls)
	}
	if _, err := resolver.Resolve(context.Background(), "demo-service"); !errors.Is(err, ErrNoInstances) {
		t.Fatalf("err on empty cache hit = %v, want ErrNoInstances", err)
	}
	if calls, _ := fake.snapshot(); calls != 2 {
		t.Fatalf("list calls = %d, want 2 within CacheTTL", calls)
	}

	// The next instance must be visible once the negative entry expires.
	// 负缓存过期后必须能马上看到新实例。
	fake.setHosts(healthyInstance("10.0.0.2", 8081, 1))
	time.Sleep(100 * time.Millisecond)
	base, err := resolver.Resolve(context.Background(), "demo-service")
	if err != nil {
		t.Fatalf("Resolve after recovery: %v", err)
	}
	if base != "http://10.0.0.2:8081" {
		t.Fatalf("base = %q", base)
	}
}

// A caller that gives up must not abort the shared refresh: the leader keeps
// the lookup running for waiters and later callers on a detached context, then
// reports the cancellation to its own caller. The result is a genuine Nacos
// answer, so it is cached once.
// 调用方放弃不能中断共享刷新：leader 在分离的 context 上继续为等待者与后续调用完成
// 查询，然后只把自己的取消报给该调用方。结果是真实的 Nacos 应答，因此缓存一次。
func TestResolveCancelDoesNotAbortTheSharedRefresh(t *testing.T) {
	gate := newTestGate()
	fake := &fakeNaming{
		hosts:    []nacosconfig.Instance{healthyInstance("10.0.0.1", 8080, 1)},
		listGate: gate,
	}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	resolver, err := New(Config{Addr: server.URL, CacheTTL: time.Minute})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() {
		_, err := resolver.Resolve(ctx, "orders")
		errs <- err
	}()
	<-gate.started
	cancel()
	close(gate.release)

	if err := <-errs; !errors.Is(err, context.Canceled) {
		t.Fatalf("Resolve = %v, want context.Canceled", err)
	}
	base, err := resolver.Resolve(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Resolve after cancel: %v", err)
	}
	if base != "http://10.0.0.1:8080" {
		t.Fatalf("base = %q", base)
	}
	if calls, _ := fake.snapshot(); calls != 1 {
		t.Fatalf("list calls = %d, want 1 (the detached refresh is shared)", calls)
	}
}

func TestNewRejectsStaleShorterThanCache(t *testing.T) {
	_, err := New(Config{CacheTTL: time.Minute, StaleTTL: time.Second})
	if !errors.Is(err, ErrStaleTTLShorterThanCache) {
		t.Fatalf("err = %v, want ErrStaleTTLShorterThanCache", err)
	}
}

func TestConfigFromNacosFallsBackToConnectionScope(t *testing.T) {
	got := ConfigFromNacos(nacosconfig.Config{Namespace: "dev", Group: "gofy-group"})
	if got.Namespace != "dev" || got.Group != "gofy-group" {
		t.Fatalf("namespace/group = %q/%q, want the connection values", got.Namespace, got.Group)
	}
}

func TestResolveExpiresStaleAfterStaleTTL(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		CacheTTL: 10 * time.Millisecond,
		StaleTTL: 20 * time.Millisecond,
	})
	fake.setHosts(healthyInstance("10.0.0.1", 8080, 1))
	if _, err := resolver.Resolve(context.Background(), "demo-service"); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	fake.setFail(true)
	time.Sleep(50 * time.Millisecond)

	if _, err := resolver.Resolve(context.Background(), "demo-service"); err == nil {
		t.Fatal("Resolve served a list older than StaleTTL")
	}
}

func TestResolveNoInstances(t *testing.T) {
	resolver, _ := newTestResolver(t, Config{})
	if _, err := resolver.Resolve(context.Background(), "demo-service"); !errors.Is(err, ErrNoInstances) {
		t.Fatalf("err = %v, want ErrNoInstances", err)
	}
}

func TestResolveRequiresServiceName(t *testing.T) {
	resolver, _ := newTestResolver(t, Config{})
	if _, err := resolver.Resolve(context.Background(), "  "); err == nil {
		t.Fatal("Resolve accepted an empty service name")
	}
}

func TestNewRejectsUnknownLoadBalancer(t *testing.T) {
	if _, err := New(Config{LoadBalancer: "hash"}); !errors.Is(err, ErrUnknownLoadBalancer) {
		t.Fatalf("err = %v, want ErrUnknownLoadBalancer", err)
	}
}

func TestBaseURLUsesSchemeMetadata(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{CacheTTL: time.Minute})
	inst := healthyInstance("10.0.0.1", 8443, 1)
	inst.Metadata = map[string]string{"scheme": "https"}
	fake.setHosts(inst)

	base, err := resolver.Resolve(context.Background(), "demo-service")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if base != "https://10.0.0.1:8443" {
		t.Fatalf("base = %q", base)
	}
}

func TestConfigFromNacosMapsDiscovery(t *testing.T) {
	discovery := nacosconfig.DiscoveryConfig{
		Group:           "gofy-group",
		Namespace:       "dev",
		LoadBalancer:    "weight",
		CacheTTL:        30 * time.Second,
		StaleTTL:        90 * time.Second,
		Clusters:        []string{"DEFAULT"},
		LocalAutoSwitch: true,
	}
	// A hand-built config must say so through the setter: assigning the field
	// is indistinguishable from leaving the zero value.
	// 手工构造的配置必须通过 setter 明确表态：只赋字段与留零值无法区分。
	discovery.SetHealthyOnly(false)
	cfg := nacosconfig.Config{
		Addr:      "http://10.0.0.2:8848",
		Namespace: "dev",
		Register:  nacosconfig.RegisterConfig{Group: "GO_GROUP"},
		Discovery: discovery,
	}
	got := ConfigFromNacos(cfg)
	if got.Addr != "http://10.0.0.2:8848" || got.Namespace != "dev" || got.Group != "gofy-group" {
		t.Fatalf("connection fields = %+v", got)
	}
	if got.LoadBalancer != "weight" || got.CacheTTL != 30*time.Second || got.StaleTTL != 90*time.Second {
		t.Fatalf("discovery fields = %+v", got)
	}
	if !got.IncludeUnhealthy {
		t.Fatal("IncludeUnhealthy = false, want true after SetHealthyOnly(false)")
	}
	if len(got.Clusters) != 1 || got.Clusters[0] != "DEFAULT" {
		t.Fatalf("Clusters = %v", got.Clusters)
	}
	// The fallback group is where this process registers, and the switch comes
	// from the discovery block: both must survive the mapping, otherwise the
	// local-development fallback silently never runs.
	// 兜底分组是本进程注册进去的那个分组，开关来自 discovery 段：两者都必须映射过去，
	// 否则本地开发兜底会静默地永不生效。
	if !got.LocalAutoSwitch || got.FallbackGroup != "GO_GROUP" {
		t.Fatalf("fallback fields = %+v", got)
	}
}

func TestConfigFromNacosZeroValueExcludesUnhealthy(t *testing.T) {
	// The zero value of DiscoveryConfig means "not set", not "include
	// unhealthy": a hand-built Config must not silently widen the candidate set.
	// DiscoveryConfig 的零值表示"未设置"，不是"包含不健康实例"：手工构造的
	// Config 不能悄悄扩大候选集。
	got := ConfigFromNacos(nacosconfig.Config{})
	if got.IncludeUnhealthy {
		t.Fatal("IncludeUnhealthy = true for a zero-value Config, want false")
	}
}

func TestConfigFromNacosExplicitHealthyOnly(t *testing.T) {
	cases := []struct {
		name          string
		enabled       bool
		wantUnhealthy bool
	}{
		{name: "explicit true", enabled: true, wantUnhealthy: false},
		{name: "explicit false", enabled: false, wantUnhealthy: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			discovery := nacosconfig.DiscoveryConfig{}
			discovery.SetHealthyOnly(tc.enabled)
			got := ConfigFromNacos(nacosconfig.Config{Discovery: discovery})
			if got.IncludeUnhealthy != tc.wantUnhealthy {
				t.Fatalf("IncludeUnhealthy = %v, want %v", got.IncludeUnhealthy, tc.wantUnhealthy)
			}
		})
	}
}

func TestNewFromFiles(t *testing.T) {
	dir := t.TempDir()
	nacosPath := filepath.Join(dir, "nacos.yaml")
	appPath := filepath.Join(dir, "application.yaml")
	writeFile(t, nacosPath, `nacos:
  host: 10.0.0.2
  namespace: dev
`)
	writeFile(t, appPath, `nacos:
  discovery_service_client:
    load_balancer: round_robin
    namespace_id: dev
    group_name: gofy-group
`)

	resolver, err := NewFromFiles(nacosPath, appPath)
	if err != nil {
		t.Fatalf("NewFromFiles: %v", err)
	}
	if resolver.cfg.LoadBalancer != "round_robin" {
		t.Fatalf("LoadBalancer = %q", resolver.cfg.LoadBalancer)
	}
	if resolver.cfg.Group != "gofy-group" || resolver.cfg.Namespace != "dev" {
		t.Fatalf("group/namespace = %q/%q", resolver.cfg.Group, resolver.cfg.Namespace)
	}
	if resolver.cfg.CacheTTL != 60*time.Second || resolver.cfg.StaleTTL != 180*time.Second {
		t.Fatalf("cache/stale = %s/%s", resolver.cfg.CacheTTL, resolver.cfg.StaleTTL)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// A zero weight is a drain command, not an unset field: Nacos reports the
// default as 1.0. The weight strategy must never hand it traffic.
// 权重 0 是摘流指令而不是"没设置"：Nacos 会把默认值报成 1.0。weight 策略绝不能给它流量。
func TestWeightStrategySkipsZeroWeight(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		Namespace:    "dev",
		LoadBalancer: "weight",
		CacheTTL:     time.Minute,
	})
	fake.setHosts(healthyInstance("10.0.0.1", 8080, 100), healthyInstance("10.0.0.2", 8080, 0))

	for i := 0; i < 200; i++ {
		base, err := resolver.Resolve(context.Background(), "orders")
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if base != "http://10.0.0.1:8080" {
			t.Fatalf("pick %d returned drained instance %q", i, base)
		}
	}
}

// Negative weights are as ineligible as zero, and must not be normalised to 1.
// 负权重与 0 一样没有资格，不能被归一到 1。
func TestWeightStrategyExcludesNegativeWeight(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		Namespace:    "dev",
		LoadBalancer: "weight",
		CacheTTL:     time.Minute,
	})
	fake.setHosts(healthyInstance("10.0.0.1", 8080, 3), healthyInstance("10.0.0.2", 8080, -5))

	for i := 0; i < 100; i++ {
		base, err := resolver.Resolve(context.Background(), "orders")
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if base != "http://10.0.0.1:8080" {
			t.Fatalf("pick %d returned negative-weight instance %q", i, base)
		}
	}
}

// Draining every instance is an operator action, not a missing field. The
// resolver fails closed with a message that names the reason.
// 把所有实例都摘流是运维动作、不是字段缺失。resolver 直接 fail-closed，并让错误信息
// 点名原因。
func TestWeightStrategyAllZeroFailsClosed(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		Namespace:    "dev",
		LoadBalancer: "weight",
		CacheTTL:     time.Minute,
	})
	fake.setHosts(healthyInstance("10.0.0.1", 8080, 0), healthyInstance("10.0.0.2", 8080, 0))

	_, err := resolver.Resolve(context.Background(), "orders")
	if !errors.Is(err, ErrNoInstances) || !errors.Is(err, ErrAllZeroWeight) {
		t.Fatalf("err = %v, want ErrNoInstances wrapping ErrAllZeroWeight", err)
	}
	if !strings.Contains(err.Error(), "weight 0") {
		t.Fatalf("err = %v, want it to name the weight", err)
	}
}

// An instance registered with inner_external_base_uri is reachable from outside
// the cluster: when the local group is empty, the resolver must switch to the
// deployment group and use that URL instead of the pod address.
// 注册了 inner_external_base_uri 的实例从集群外可达：本地分组为空时，resolver 必须切到
// 部署分组并使用该 URL，而不是 Pod 地址。
func TestResolveFallsBackToDeploymentGroupUsingMetadata(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		Namespace:       "dev",
		Group:           "local",
		FallbackGroup:   "GO_GROUP",
		LocalAutoSwitch: true,
		CacheTTL:        time.Minute,
	})
	fake.setGroup("local")
	fake.setGroup("GO_GROUP", instanceWithMetadata("10.244.0.9", 8080, 1,
		map[string]string{nacosconfig.MetadataInnerExternalBaseURI: "http://demo-dev.example.com:8080/"}))

	var events []FallbackEvent
	resolver.SetOnFallback(func(ev FallbackEvent) { events = append(events, ev) })

	base, err := resolver.Resolve(context.Background(), "demo-service")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if base != "http://demo-dev.example.com:8080" {
		t.Fatalf("base = %q, want the metadata URL without its trailing slash", base)
	}
	if fake.groupCalls("local") != 1 || fake.groupCalls("GO_GROUP") != 1 {
		t.Fatalf("calls local/fallback = %d/%d, want 1/1",
			fake.groupCalls("local"), fake.groupCalls("GO_GROUP"))
	}
	if len(events) != 1 {
		t.Fatalf("fallback events = %d, want 1 per refresh", len(events))
	}
	if ev := events[0]; ev.Service != "demo-service" || ev.LocalGroup != "local" ||
		ev.FallbackGroup != "GO_GROUP" || !ev.FromMetadata || ev.BaseURL != base {
		t.Fatalf("event = %+v", ev)
	}

	// A cache hit must not emit a second event: the hook is per refresh.
	// 缓存命中不能再发事件：钩子是"每次刷新一次"。
	if _, err := resolver.Resolve(context.Background(), "demo-service"); err != nil {
		t.Fatalf("cached Resolve: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("fallback events after cache hit = %d, want 1", len(events))
	}
}

// Without the metadata the deployment instance is still usable: fall back to
// its address rather than failing the lookup.
// 没有该 metadata 时部署分组的实例照样可用：退到它的地址，而不是让解析失败。
func TestResolveFallbackWithoutMetadataUsesAddress(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		Namespace:       "dev",
		Group:           "local",
		FallbackGroup:   "GO_GROUP",
		LocalAutoSwitch: true,
		CacheTTL:        time.Minute,
	})
	fake.setGroup("local")
	fake.setGroup("GO_GROUP", healthyInstance("10.244.0.9", 8080, 1))

	var events []FallbackEvent
	resolver.SetOnFallback(func(ev FallbackEvent) { events = append(events, ev) })

	base, err := resolver.Resolve(context.Background(), "demo-service")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if base != "http://10.244.0.9:8080" {
		t.Fatalf("base = %q", base)
	}
	if len(events) != 1 || events[0].FromMetadata {
		t.Fatalf("events = %+v, want one with FromMetadata=false", events)
	}
}

// A bad metadata value must not poison the whole lookup: it falls back to the
// instance address, and the client's own validation never sees it.
// 坏掉的 metadata 不能让整次解析失败：退到实例地址，客户端那层校验根本看不到它。
func TestResolveFallbackIgnoresInvalidMetadata(t *testing.T) {
	cases := []struct {
		name  string
		value string
	}{
		{name: "not a url", value: "demo-dev.example.com:8080"},
		{name: "wrong scheme", value: "ftp://demo-dev.example.com:8080"},
		{name: "no host", value: "http:///path-only"},
		{name: "with query", value: "http://demo-dev.example.com:8080/?x=1"},
		{name: "with userinfo", value: "http://user:pass@demo-dev.example.com:8080"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resolver, fake := newTestResolver(t, Config{
				Namespace:       "dev",
				Group:           "local",
				FallbackGroup:   "GO_GROUP",
				LocalAutoSwitch: true,
				CacheTTL:        time.Minute,
			})
			fake.setGroup("local")
			fake.setGroup("GO_GROUP", instanceWithMetadata("10.244.0.9", 8080, 1,
				map[string]string{nacosconfig.MetadataInnerExternalBaseURI: tc.value}))

			base, err := resolver.Resolve(context.Background(), "demo-service")
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if base != "http://10.244.0.9:8080" {
				t.Fatalf("base = %q, want the instance address", base)
			}
		})
	}
}

// The switch is off by default: a cluster caller that happens to see an empty
// group must not borrow the deployment group's instances.
// 开关默认关闭：集群内的调用方即使看到空分组，也不能借用部署分组的实例。
func TestResolveDoesNotSwitchWhenDisabled(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		Namespace:     "dev",
		Group:         "local",
		FallbackGroup: "GO_GROUP",
		CacheTTL:      time.Minute,
	})
	fake.setGroup("local")
	fake.setGroup("GO_GROUP", healthyInstance("10.244.0.9", 8080, 1))

	_, err := resolver.Resolve(context.Background(), "demo-service")
	if !errors.Is(err, ErrNoInstances) {
		t.Fatalf("err = %v, want ErrNoInstances", err)
	}
	if n := fake.groupCalls("GO_GROUP"); n != 0 {
		t.Fatalf("fallback group queried %d times with the switch off", n)
	}
}

// When both groups are the same there is nothing to switch to: query it once.
// 两个分组相同时没有可切换的目标：只查一次。
func TestResolveSkipsFallbackWhenGroupsMatch(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		Namespace:       "dev",
		Group:           "GO_GROUP",
		FallbackGroup:   "GO_GROUP",
		LocalAutoSwitch: true,
		CacheTTL:        time.Minute,
	})
	fake.setGroup("GO_GROUP")

	_, err := resolver.Resolve(context.Background(), "demo-service")
	if !errors.Is(err, ErrNoInstances) {
		t.Fatalf("err = %v, want ErrNoInstances", err)
	}
	if calls, _ := fake.snapshot(); calls != 1 {
		t.Fatalf("list calls = %d, want 1", calls)
	}
}

// A Nacos failure is not an empty group: both groups sit behind the same
// registry, so the fallback must not be tried (it would only double the load).
// Nacos 故障不等于"分组为空"：两个分组在同一个注册中心后面，兜底不该再查一次（只会
// 让故障翻倍）。
func TestResolveDoesNotSwitchOnNacosFailure(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		Namespace:       "dev",
		Group:           "local",
		FallbackGroup:   "GO_GROUP",
		LocalAutoSwitch: true,
		CacheTTL:        time.Minute,
	})
	fake.setGroup("local")
	fake.setGroup("GO_GROUP", healthyInstance("10.244.0.9", 8080, 1))
	fake.setFail(true)

	_, err := resolver.Resolve(context.Background(), "demo-service")
	if err == nil {
		t.Fatal("Resolve succeeded with a failing registry")
	}
	if errors.Is(err, ErrNoInstances) {
		t.Fatalf("err = %v, want the registry failure rather than ErrNoInstances", err)
	}
	if n := fake.groupCalls("GO_GROUP"); n != 0 {
		t.Fatalf("fallback group queried %d times during a registry failure", n)
	}
}

// The switch must not be sticky: once the local group has instances again, the
// next refresh uses them and stops borrowing the deployment group. This is what
// the per-group cache key buys — a fallback result stored under the bare
// service name would answer forever.
// 切换不能有粘性：本地分组重新有实例后，下一次刷新就该用它、不再借部署分组。这正是
// "按分组分键"带来的——把兜底结果存在裸服务名下会永远替本地作答。
func TestResolveTriesLocalGroupAgainAfterSwitch(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		Namespace:       "dev",
		Group:           "local",
		FallbackGroup:   "GO_GROUP",
		LocalAutoSwitch: true,
		CacheTTL:        40 * time.Millisecond,
	})
	fake.setGroup("local")
	fake.setGroup("GO_GROUP", instanceWithMetadata("10.244.0.9", 8080, 1,
		map[string]string{nacosconfig.MetadataInnerExternalBaseURI: "http://demo-dev.example.com:8080"}))

	if base, err := resolver.Resolve(context.Background(), "demo-service"); err != nil ||
		base != "http://demo-dev.example.com:8080" {
		t.Fatalf("first Resolve = %q, %v", base, err)
	}

	time.Sleep(60 * time.Millisecond)
	if _, err := resolver.Resolve(context.Background(), "demo-service"); err != nil {
		t.Fatalf("second Resolve: %v", err)
	}
	if n := fake.groupCalls("local"); n < 2 {
		t.Fatalf("local group queried %d times, want the expired empty entry to be retried", n)
	}

	// The local instances are back: the next refresh must use them.
	// 本地实例回来了：下一次刷新必须用它们。
	fake.setGroup("local", healthyInstance("10.0.0.5", 8080, 1))
	time.Sleep(60 * time.Millisecond)
	base, err := resolver.Resolve(context.Background(), "demo-service")
	if err != nil {
		t.Fatalf("third Resolve: %v", err)
	}
	if base != "http://10.0.0.5:8080" {
		t.Fatalf("base = %q, want the local instance address", base)
	}
}

// Both groups empty: the error has to name both, otherwise an operator sees
// "no instances" and has no idea which group was consulted.
// 两个分组都空：错误信息必须同时点名两者，否则运维只看到"没有实例"，不知道查过哪些分组。
func TestResolveFallbackEmptyNamesBothGroups(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		Namespace:       "dev",
		Group:           "local",
		FallbackGroup:   "GO_GROUP",
		LocalAutoSwitch: true,
		CacheTTL:        time.Minute,
	})
	fake.setGroup("local")
	fake.setGroup("GO_GROUP")

	_, err := resolver.Resolve(context.Background(), "demo-service")
	if !errors.Is(err, ErrNoInstances) {
		t.Fatalf("err = %v, want ErrNoInstances", err)
	}
	for _, want := range []string{"local", "GO_GROUP"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want it to mention %q", err, want)
		}
	}
}

func instanceWithMetadata(ip string, port int, weight float64, metadata map[string]string) nacosconfig.Instance {
	inst := healthyInstance(ip, port, weight)
	inst.Metadata = metadata
	return inst
}

// A drained local group is not an empty one: somebody deliberately took the
// instances out of rotation, so borrowing the deployment group would defeat the
// drain (and hide it).
// 本地分组被摘流不等于"没有实例"：有人刻意把这些实例移出了轮转，此时借用部署分组会
// 让摘流失效，还会把它藏起来。
func TestResolveDoesNotSwitchWhenLocalGroupIsDrained(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		Namespace:       "dev",
		Group:           "local",
		FallbackGroup:   "GO_GROUP",
		LocalAutoSwitch: true,
		LoadBalancer:    "weight",
		CacheTTL:        time.Minute,
	})
	fake.setGroup("local", healthyInstance("10.0.0.1", 8080, 0))
	fake.setGroup("GO_GROUP", instanceWithMetadata("10.244.0.9", 8080, 1,
		map[string]string{nacosconfig.MetadataInnerExternalBaseURI: "http://demo-dev.example.com:8080"}))

	_, err := resolver.Resolve(context.Background(), "demo-service")
	if !errors.Is(err, ErrAllZeroWeight) {
		t.Fatalf("err = %v, want ErrAllZeroWeight", err)
	}
	if n := fake.groupCalls("GO_GROUP"); n != 0 {
		t.Fatalf("fallback group queried %d times for a drained local group", n)
	}
}

// Close is a lifecycle boundary: the cached lists and round-robin cursors are
// dropped, and later calls report ErrClosed instead of querying a client whose
// connections were released.
// Close 是生命周期边界：缓存列表与轮询游标被丢弃，之后的调用返回 ErrClosed，而不是
// 在连接已释放的客户端上继续查询。
func TestResolveAfterCloseReturnsErrClosed(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		LoadBalancer: "round_robin",
		CacheTTL:     time.Minute,
	})
	fake.setHosts(healthyInstance("10.0.0.1", 8080, 1))
	if _, err := resolver.Resolve(context.Background(), "orders"); err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	resolver.mu.Lock()
	cacheSize, cursorSize := len(resolver.cache), len(resolver.rr)
	resolver.mu.Unlock()
	if cacheSize != 1 || cursorSize != 1 {
		t.Fatalf("cache/rr = %d/%d, want 1/1 before Close", cacheSize, cursorSize)
	}

	if err := resolver.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	resolver.mu.Lock()
	cacheSize, cursorSize = len(resolver.cache), len(resolver.rr)
	resolver.mu.Unlock()
	if cacheSize != 0 || cursorSize != 0 {
		t.Fatalf("cache/rr = %d/%d after Close, want empty", cacheSize, cursorSize)
	}
	if _, err := resolver.Resolve(context.Background(), "orders"); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
	if calls, _ := fake.snapshot(); calls != 1 {
		t.Fatalf("list calls = %d, want no call after Close", calls)
	}
	if err := resolver.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// A refresh that is already in flight when Close runs must not repopulate the
// maps Close just cleared, and it must report ErrClosed instead of a
// misleading "no instances".
// Close 执行时仍在途的刷新不能重新填充它刚清空的 map，并且必须报 ErrClosed，
// 而不是误导性的"没有实例"。
func TestCloseDuringRefreshLeavesStateEmpty(t *testing.T) {
	gate := newTestGate()
	fake := &fakeNaming{
		hosts:    []nacosconfig.Instance{healthyInstance("10.0.0.1", 8080, 1)},
		listGate: gate,
	}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	resolver, err := New(Config{Addr: server.URL, CacheTTL: time.Minute})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := resolver.Resolve(context.Background(), "orders")
		done <- err
	}()
	<-gate.started
	if err := resolver.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(gate.release)

	if err := <-done; !errors.Is(err, ErrClosed) {
		t.Fatalf("Resolve = %v, want ErrClosed", err)
	}
	resolver.mu.Lock()
	cacheSize, inflightSize := len(resolver.cache), len(resolver.inflight)
	resolver.mu.Unlock()
	if cacheSize != 0 || inflightSize != 0 {
		t.Fatalf("cache/inflight = %d/%d after Close, want empty", cacheSize, inflightSize)
	}
}

// A slot whose stale window has passed can never answer again, so a later
// refresh drops it: one-off service names must not grow the cache monotonically.
// stale 窗口已过的槽不可能再作答，因此后续刷新会清掉它：一次性服务名不能让缓存
// 单调增长。
func TestResolverPrunesSlotsPastStaleTTL(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		LoadBalancer: "round_robin",
		CacheTTL:     10 * time.Millisecond,
		StaleTTL:     20 * time.Millisecond,
	})
	fake.setHosts(healthyInstance("10.0.0.1", 8080, 1))
	if _, err := resolver.Resolve(context.Background(), "orders"); err != nil {
		t.Fatalf("Resolve orders: %v", err)
	}
	time.Sleep(60 * time.Millisecond)

	if _, err := resolver.Resolve(context.Background(), "products"); err != nil {
		t.Fatalf("Resolve products: %v", err)
	}

	resolver.mu.Lock()
	_, ordersKept := resolver.cache[cacheKey("orders", resolver.cfg.Group)]
	cacheSize, cursorSize := len(resolver.cache), len(resolver.rr)
	resolver.mu.Unlock()
	if ordersKept {
		t.Fatal("orders slot survived past StaleTTL")
	}
	if cacheSize != 1 || cursorSize != 1 {
		t.Fatalf("cache/rr = %d/%d, want only the products slot", cacheSize, cursorSize)
	}

	// The pruned service must still resolve: it is refreshed, not blocked.
	// 被清掉的服务仍可解析：它是回源刷新，而不是被拦下。
	if _, err := resolver.Resolve(context.Background(), "orders"); err != nil {
		t.Fatalf("Resolve orders again: %v", err)
	}
	if calls, _ := fake.snapshot(); calls != 3 {
		t.Fatalf("list calls = %d, want 3", calls)
	}
}

// Length-prefixed keys make the service/group split unambiguous: a service name
// containing '@' must not share a slot with a different service whose group
// happens to hold the rest of the string.
// 长度前缀让 service/group 的切分无歧义：服务名带 '@' 时，不能与分组里恰好含有
// 剩余字符的另一个服务共用槽。
func TestCacheKeyDisambiguatesServiceAndGroup(t *testing.T) {
	if cacheKey("a@b", "c") == cacheKey("a", "b@c") {
		t.Fatal("cacheKey collides for values joined by '@'")
	}
	if cacheKey("orders", "local") == cacheKey("orders", "GO_GROUP") {
		t.Fatal("cacheKey ignores the group")
	}
}

// Draining must mean the same thing under every strategy. The default random
// strategy used to ignore weight entirely, so a drained instance kept receiving
// traffic until an operator also switched load_balancer.
// 摘流在每种策略下必须是同一个意思。默认的 random 策略过去完全忽略 weight，被摘流的
// 实例会一直收到流量，直到运维把 load_balancer 也改掉。
func TestRandomStrategySkipsDrainedInstances(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{Namespace: "dev", CacheTTL: time.Minute})
	fake.setHosts(healthyInstance("10.0.0.1", 8080, 1), healthyInstance("10.0.0.2", 8080, 0))

	for i := 0; i < 100; i++ {
		base, err := resolver.Resolve(context.Background(), "orders")
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if base != "http://10.0.0.1:8080" {
			t.Fatalf("pick %d returned drained instance %q", i, base)
		}
	}
}

// The same rule applies to round_robin: the drained instance must not appear in
// the rotation at all.
// 同一条规则适用于 round_robin：被摘流的实例根本不应出现在轮转里。
func TestRoundRobinSkipsDrainedInstances(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		Namespace:    "dev",
		LoadBalancer: "round_robin",
		CacheTTL:     time.Minute,
	})
	fake.setHosts(
		healthyInstance("10.0.0.1", 8080, 1),
		healthyInstance("10.0.0.2", 8080, 0),
		healthyInstance("10.0.0.3", 8080, 1),
	)

	want := []string{"http://10.0.0.1:8080", "http://10.0.0.3:8080"}
	for i := 0; i < 10; i++ {
		base, err := resolver.Resolve(context.Background(), "orders")
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if base != want[i%len(want)] {
			t.Fatalf("pick %d = %q, want %q", i, base, want[i%len(want)])
		}
	}
}

// A fully drained local group is still not an empty one under the default
// strategy: the fallback group must not rescue a deliberate drain.
// 在默认策略下，被全部摘流的本地分组仍然不等于"空分组"：兜底分组不能拯救一次刻意的
// 摘流。
func TestRandomStrategyAllDrainedDoesNotFallBack(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		Namespace:       "dev",
		Group:           "local",
		FallbackGroup:   "GO_GROUP",
		LocalAutoSwitch: true,
		CacheTTL:        time.Minute,
	})
	fake.setGroup("local", healthyInstance("10.0.0.1", 8080, 0))
	fake.setGroup("GO_GROUP", healthyInstance("10.244.0.9", 8080, 1))

	_, err := resolver.Resolve(context.Background(), "demo-service")
	if !errors.Is(err, ErrAllZeroWeight) {
		t.Fatalf("err = %v, want ErrAllZeroWeight", err)
	}
	if n := fake.groupCalls("GO_GROUP"); n != 0 {
		t.Fatalf("fallback group queried %d times for a drained local group", n)
	}
}

// A failed refresh whose outage StaleTTL masks must still be reported once, and
// only once per refresh: the event is what keeps a hidden outage visible.
// 被 StaleTTL 掩盖的刷新失败仍必须上报一次，且每次刷新只报一次：事件是隐藏故障保持
// 可见的唯一途径。
func TestRefreshFailureEventReportsMaskedOutage(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		CacheTTL: 10 * time.Millisecond,
		StaleTTL: time.Hour,
	})
	fake.setHosts(healthyInstance("10.0.0.1", 8080, 1))
	first, err := resolver.Resolve(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	var events []RefreshFailureEvent
	resolver.SetOnRefreshFailure(func(ev RefreshFailureEvent) { events = append(events, ev) })

	fake.setFail(true)
	time.Sleep(20 * time.Millisecond)
	base, err := resolver.Resolve(context.Background(), "orders")
	if err != nil {
		t.Fatalf("stale Resolve: %v", err)
	}
	if base != first {
		t.Fatalf("base = %q, want stale %q", base, first)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1 per failed refresh", len(events))
	}
	ev := events[0]
	if ev.Service != "orders" || !ev.StaleServed || ev.Err == nil {
		t.Fatalf("event = %+v", ev)
	}

	// The throttled cache hit must not fire a second event.
	// 被节流的缓存命中不能再次触发事件。
	if _, err := resolver.Resolve(context.Background(), "orders"); err != nil {
		t.Fatalf("cached Resolve: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events after cache hit = %d, want 1", len(events))
	}
}

// A registry failure in the fallback group must not masquerade as "no
// instances": callers branch on ErrNoInstances, and a misreported outage would
// hide itself behind their degradation path.
// 兜底分组的注册中心故障不能伪装成"没有实例"：调用方会按 ErrNoInstances 分支，错误
// 归类会把故障藏进它们的降级路径里。
func TestFallbackRegistryFailureIsNotErrNoInstances(t *testing.T) {
	resolver, fake := newTestResolver(t, Config{
		Namespace:       "dev",
		Group:           "local",
		FallbackGroup:   "GO_GROUP",
		LocalAutoSwitch: true,
		CacheTTL:        time.Minute,
	})
	fake.setGroup("local")
	fake.setGroup("GO_GROUP", healthyInstance("10.244.0.9", 8080, 1))
	fake.setGroupFail("GO_GROUP", true)

	_, err := resolver.Resolve(context.Background(), "demo-service")
	if err == nil {
		t.Fatal("Resolve succeeded while the fallback registry was failing")
	}
	if errors.Is(err, ErrNoInstances) {
		t.Fatalf("err = %v, want the registry failure rather than ErrNoInstances", err)
	}
	if !strings.Contains(err.Error(), "fallback group") {
		t.Fatalf("err = %v, want it to name the fallback group", err)
	}
}
