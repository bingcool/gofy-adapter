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
		if f.fail {
			http.Error(w, "boom", http.StatusServiceUnavailable)
			return
		}
		payload, err := json.Marshal(map[string]any{"hosts": f.hosts})
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

func (f *fakeNaming) setFail(fail bool) {
	f.mu.Lock()
	f.fail = fail
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

// A caller that gives up must not poison the cache: its cancellation says
// nothing about Nacos, so the next caller has to retry immediately.
// 调用方放弃不能污染缓存：它取消说明不了 Nacos 的状态，下一个调用方必须立刻重试。
func TestResolveCancelDoesNotPoisonCache(t *testing.T) {
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
	if err := <-errs; err == nil {
		t.Fatal("Resolve returned nil error for a cancelled caller")
	}
	close(gate.release)

	base, err := resolver.Resolve(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Resolve after cancel: %v", err)
	}
	if base != "http://10.0.0.1:8080" {
		t.Fatalf("base = %q", base)
	}
	if calls, _ := fake.snapshot(); calls != 2 {
		t.Fatalf("list calls = %d, want 2 (cancel must not be cached)", calls)
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
	cfg := nacosconfig.Config{
		Addr:      "http://10.0.0.2:8848",
		Namespace: "dev",
		Discovery: nacosconfig.DiscoveryConfig{
			Group:        "gofy-group",
			Namespace:    "dev",
			LoadBalancer: "weight",
			CacheTTL:     30 * time.Second,
			StaleTTL:     90 * time.Second,
			HealthyOnly:  false,
			Clusters:     []string{"DEFAULT"},
		},
	}
	got := ConfigFromNacos(cfg)
	if got.Addr != "http://10.0.0.2:8848" || got.Namespace != "dev" || got.Group != "gofy-group" {
		t.Fatalf("connection fields = %+v", got)
	}
	if got.LoadBalancer != "weight" || got.CacheTTL != 30*time.Second || got.StaleTTL != 90*time.Second {
		t.Fatalf("discovery fields = %+v", got)
	}
	if !got.IncludeUnhealthy {
		t.Fatal("IncludeUnhealthy = false, want true when HealthyOnly is false")
	}
	if len(got.Clusters) != 1 || got.Clusters[0] != "DEFAULT" {
		t.Fatalf("Clusters = %v", got.Clusters)
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
