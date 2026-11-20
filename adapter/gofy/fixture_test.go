// Package gofyadapter_test verifies the adapter using only library fixtures.
// The real-contract binding tests live in the project module (§4.7).
// 包 gofyadapter_test 只用库内 fixture 验证 adapter；真实契约的绑定测试归项目模块（§4.7）。
package gofyadapter_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	gofyadapter "github.com/bingcool/gofy-adapter/adapter/gofy"
	"github.com/bingcool/gofy-adapter/internal/testcontract"
	"github.com/bingcool/gofy-interface-api/contracts/common/endpoint"
	"github.com/bingcool/gofy-interface-api/contracts/common/paging"
	"github.com/bingcool/gofy/support"
	"github.com/gin-gonic/gin"
)

type fixtureImpl struct{}

func (fixtureImpl) GetItem(_ context.Context, req *testcontract.ItemRequest) (*testcontract.ItemResponse, error) {
	return &testcontract.ItemResponse{Item: &testcontract.ItemDTO{ID: req.ID, Name: "item"}}, nil
}

func (fixtureImpl) ListItems(_ context.Context, req *testcontract.ListRequest) (*paging.ListData[testcontract.ItemDTO], error) {
	return &paging.ListData[testcontract.ItemDTO]{Total: 1, List: []testcontract.ItemDTO{{ID: req.UserID}}}, nil
}

func (fixtureImpl) CreateItem(_ context.Context, req *testcontract.CreateRequest) (*testcontract.CreateResponse, error) {
	return &testcontract.CreateResponse{Item: &testcontract.ItemDTO{ID: req.UserID, Amount: req.Amount}}, nil
}

func (fixtureImpl) CancelItem(_ context.Context, _ *testcontract.CancelRequest) (*endpoint.NoBody, error) {
	return new(endpoint.NoBody), nil
}

func (fixtureImpl) GetConfig(_ context.Context, _ *testcontract.ConfigRequest) (*testcontract.ConfigResponse, error) {
	return &testcontract.ConfigResponse{AuthRequired: true}, nil
}

func newFixtureRouter() (*gin.Engine, *support.Router) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	// Fixture specs carry the full deployment prefix in Spec.Path; the router
	// itself must not add another one (bindPath trims GroupPrefix).
	// fixture 的 Spec.Path 已含完整部署前缀，Router 不能再叠加一层（bindPath 会按
	// GroupPrefix 裁剪），否则请求路径会错位。
	return engine, support.NewRouter(engine, support.RouterConfig{
		Responder: support.NewResponder(support.ResponderConfig{AlwaysOK: false}),
	})
}

func serve(engine *gin.Engine, method, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

func TestBindAllFixtureRoutesSucceed(t *testing.T) {
	engine, r := newFixtureRouter()
	if err := gofyadapter.BindAll(r, testcontract.Routes(), fixtureImpl{}, gofyadapter.BindOptions{}); err != nil {
		t.Fatalf("BindAll: %v", err)
	}
	rec := serve(engine, http.MethodGet, "/api/test/items/7")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"id":7`) {
		t.Fatalf("response missing item id: %s", rec.Body.String())
	}
}

func TestBindAllRejectsDuplicateOperationID(t *testing.T) {
	_, r := newFixtureRouter()
	// A distinct path keeps gin from panicking on a duplicate registration; only
	// the operation id collides, which is what BindAll must report.
	// 用不同路径避免 gin 重复注册 panic，只让 operation id 冲突。
	dup := endpoint.New[testcontract.ItemRequest, testcontract.ItemResponse](endpoint.Spec{
		OperationID: "GetItem",
		Method:      http.MethodGet,
		Path:        "/dup/{id}",
	})
	routes := []endpoint.Descriptor{testcontract.GetItem, dup}
	err := gofyadapter.BindAll(r, routes, fixtureImpl{}, gofyadapter.BindOptions{})
	if err == nil || !strings.Contains(err.Error(), "duplicate operation id") {
		t.Fatalf("BindAll error = %v, want duplicate operation id", err)
	}
}

// TestBindAllDoesNotRegisterWhenValidationFails pins P2-03: a bad descriptor
// must not leave earlier, otherwise-valid routes registered on the router.
// TestBindAllDoesNotRegisterWhenValidationFails 固定 P2-03：坏描述符不能让前面
// 本应合法的路由先注册到 Router 上。
func TestBindAllDoesNotRegisterWhenValidationFails(t *testing.T) {
	engine, r := newFixtureRouter()
	// GetItem is valid; the second descriptor names a method the implementation
	// does not have, so validation fails after the first route was inspected.
	// GetItem 合法；第二个描述符指向实现里不存在的方法，校验在检查完第一条后失败。
	broken := endpoint.New[testcontract.ItemRequest, testcontract.ItemResponse](endpoint.Spec{
		OperationID: "MissingMethod",
		Method:      http.MethodGet,
		Path:        "/api/test/missing",
	})
	routes := []endpoint.Descriptor{testcontract.GetItem, broken}
	err := gofyadapter.BindAll(r, routes, fixtureImpl{}, gofyadapter.BindOptions{})
	if err == nil || !strings.Contains(err.Error(), "implementation has no method") {
		t.Fatalf("BindAll error = %v, want the missing-method problem", err)
	}
	if registered := engine.Routes(); len(registered) != 0 {
		t.Fatalf("failed BindAll left %d route(s) registered: %v", len(registered), registered)
	}
}

func TestBindAllAggregatesEveryProblem(t *testing.T) {
	_, r := newFixtureRouter()
	bad := endpoint.New[testcontract.ItemRequest, testcontract.ItemResponse](endpoint.Spec{
		OperationID: "BadAuthProbe",
		Method:      "TRACE",
		Path:        "no-leading-slash",
	})
	err := gofyadapter.BindAll(r, []endpoint.Descriptor{bad, bad, testcontract.GetItem}, fixtureImpl{}, gofyadapter.BindOptions{})
	if err == nil {
		t.Fatal("BindAll must reject invalid descriptors")
	}
	for _, want := range []string{"unsupported method", "path must start with /", "duplicate operation id"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("aggregated error missing %q: %v", want, err)
		}
	}
}

func TestBindAllRejectsMissingDependencies(t *testing.T) {
	_, r := newFixtureRouter()
	if err := gofyadapter.BindAll(nil, testcontract.Routes(), fixtureImpl{}, gofyadapter.BindOptions{}); err == nil {
		t.Fatal("nil router must fail")
	}
	if err := gofyadapter.BindAll(r, testcontract.Routes(), nil, gofyadapter.BindOptions{}); err == nil {
		t.Fatal("nil implementation must fail")
	}
}

// TestRateLimitHookReceivesRouterResponder pins F6's adapter half: the hook is
// handed the Router's responder, so the limiter it builds rejects with the same
// envelope as the handlers instead of a library default.
// TestRateLimitHookReceivesRouterResponder 固定 F6 的 adapter 一侧：回调拿到 Router
// 的 responder，它构建的限流因此与 handler 共用同一个信封，而不是库内默认值。
func TestRateLimitHookReceivesRouterResponder(t *testing.T) {
	_, r := newFixtureRouter()
	want := r.Responder()
	var got support.Responder
	err := gofyadapter.BindAll(r, testcontract.Routes(), fixtureImpl{}, gofyadapter.BindOptions{
		RateLimit: func(_ endpoint.Spec, responder support.Responder) support.MiddlewareFunc {
			got = responder
			return nil
		},
	})
	if err != nil {
		t.Fatalf("BindAll: %v", err)
	}
	if got == nil {
		t.Fatal("the rate limit hook never ran")
	}
	if got != want {
		t.Fatal("the hook received a responder other than the router's")
	}
}

// TestBindGroupResolvesNamesInDeclarationOrder also covers the whole auth
// mechanism: the contract has no auth field, so installing "auth" through
// Group.Use is what makes these routes authenticated, and the assertion below
// is the proof that it actually runs.
// TestBindGroupResolvesNamesInDeclarationOrder 同时覆盖了认证机制的全部：契约没有
// 认证字段，因此通过 Group.Use 装上 "auth" 就是这些路由需要认证的全部依据，下面的
// 断言就是它确实执行了的证明。
func TestBindGroupResolvesNamesInDeclarationOrder(t *testing.T) {
	engine, r := newFixtureRouter()
	var order []string
	mw := func(name string) support.MiddlewareFunc {
		return func(c *gin.Context) {
			order = append(order, name)
			c.Next()
		}
	}
	opts := gofyadapter.BindOptions{Middleware: map[string]support.MiddlewareFunc{
		"audit": mw("audit"),
		"auth":  mw("auth"),
	}}
	if err := gofyadapter.BindGroup(r, testcontract.Items, fixtureImpl{}, opts); err != nil {
		t.Fatalf("BindGroup: %v", err)
	}
	rec := serve(engine, http.MethodGet, "/api/test/items/7")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(order) != 2 || order[0] != "audit" || order[1] != "auth" {
		t.Fatalf("middleware order = %v, want [audit auth]", order)
	}
}

func TestBindGroupRejectsUnregisteredName(t *testing.T) {
	_, r := newFixtureRouter()
	err := gofyadapter.BindGroup(r, testcontract.Items, fixtureImpl{}, gofyadapter.BindOptions{})
	if err == nil || !strings.Contains(err.Error(), "audit") {
		t.Fatalf("BindGroup error = %v, want missing name audit", err)
	}
}

func TestBindGroupRejectsInconsistentStampedPrefix(t *testing.T) {
	_, r := newFixtureRouter()
	// Fixture routes are stamped "/api/test" by their own group; declaring them
	// under another prefix must fail before any route goes live.
	// fixture 路由已被自己的分组写入 "/api/test"；换到另一个前缀下声明时必须在
	// 路由上线前失败。
	group := &endpoint.Group{Prefix: "/other", Routes: testcontract.Items.Routes}
	err := gofyadapter.BindGroup(r, group, fixtureImpl{}, gofyadapter.BindOptions{})
	if err == nil || !strings.Contains(err.Error(), "inconsistent route prefixes") {
		t.Fatalf("BindGroup error = %v, want inconsistent route prefixes", err)
	}
}
