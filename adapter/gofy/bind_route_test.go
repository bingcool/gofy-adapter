package gofyadapter_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	gofyadapter "github.com/bingcool/gofy-adapter/adapter/gofy"
	"github.com/bingcool/gofy-adapter/internal/testcontract"
	"github.com/bingcool/gofy-interface-api/contracts/common/endpoint"
)

// routeNameRequest binds the second placeholder shape used by the conflict
// tests; ItemRequest only carries id, so {name} needs its own request type.
// routeNameRequest 提供冲突测试需要的第二种占位符形态；ItemRequest 只有 id，
// {name} 需要自己的请求类型。
type routeNameRequest struct {
	Name string `uri:"name"`
}

type routeConflictImpl struct{}

func (routeConflictImpl) GetByID(_ context.Context, _ *testcontract.ItemRequest) (*testcontract.ItemResponse, error) {
	return &testcontract.ItemResponse{}, nil
}

func (routeConflictImpl) GetByName(_ context.Context, _ *routeNameRequest) (*testcontract.ItemResponse, error) {
	return &testcontract.ItemResponse{}, nil
}

func (routeConflictImpl) GetTrimA(_ context.Context, _ *testcontract.ItemRequest) (*testcontract.ItemResponse, error) {
	return &testcontract.ItemResponse{}, nil
}

func (routeConflictImpl) GetTrimB(_ context.Context, _ *testcontract.ItemRequest) (*testcontract.ItemResponse, error) {
	return &testcontract.ItemResponse{}, nil
}

func conflictRoute[Req, Res any](operationID, method, path string) endpoint.Descriptor {
	return endpoint.New[Req, Res](endpoint.Spec{
		OperationID: operationID,
		Method:      method,
		Path:        path,
	})
}

// TestBindAllRejectsSameGinPathStructure pins P2-03: /users/{id} and
// /users/{name} are one route to Gin, because it keeps a single wildcard per
// position and panics when a second name arrives. BindAll must report it before
// registering anything, not let Gin panic after the first route went live.
// TestBindAllRejectsSameGinPathStructure 固定 P2-03：/users/{id} 与 /users/{name}
// 对 Gin 是同一条路由——每个位置只保留一个通配节点，出现第二个名字就 panic。BindAll
// 必须在注册前报出这个错误，而不是让 Gin 在第一条路由已经上线后 panic。
func TestBindAllRejectsSameGinPathStructure(t *testing.T) {
	engine, r := newFixtureRouter()
	byID := conflictRoute[testcontract.ItemRequest, testcontract.ItemResponse](
		"GetByID", http.MethodGet, "/api/test/users/{id}")
	byName := conflictRoute[routeNameRequest, testcontract.ItemResponse](
		"GetByName", http.MethodGet, "/api/test/users/{name}")
	err := gofyadapter.BindAll(r, []endpoint.Descriptor{byID, byName}, routeConflictImpl{}, gofyadapter.BindOptions{})
	if err == nil || !strings.Contains(err.Error(), "duplicate route") {
		t.Fatalf("BindAll error = %v, want duplicate route", err)
	}
	if !strings.Contains(err.Error(), "GetByID") || !strings.Contains(err.Error(), "GetByName") {
		t.Fatalf("error must name both routes: %v", err)
	}
	if registered := engine.Routes(); len(registered) != 0 {
		t.Fatalf("failed BindAll left %d route(s) registered: %v", len(registered), registered)
	}
}

// TestBindAllRejectsSameStaticPath covers the exact-collision row of the
// matrix: same method, same path. The raw-path check already caught it; the
// normalized key must keep doing so.
// TestBindAllRejectsSameStaticPath 覆盖矩阵里「完全相同路径」这一行：同方法、同路径。
// 原始路径检查本来就能拦住，规范化 key 必须保持同样结果。
func TestBindAllRejectsSameStaticPath(t *testing.T) {
	engine, r := newFixtureRouter()
	first := conflictRoute[testcontract.ItemRequest, testcontract.ItemResponse](
		"GetTrimA", http.MethodGet, "/api/test/same/{id}")
	second := conflictRoute[testcontract.ItemRequest, testcontract.ItemResponse](
		"GetTrimB", http.MethodGet, "/api/test/same/{id}")
	err := gofyadapter.BindAll(r, []endpoint.Descriptor{first, second}, routeConflictImpl{}, gofyadapter.BindOptions{})
	if err == nil || !strings.Contains(err.Error(), "duplicate route") {
		t.Fatalf("BindAll error = %v, want duplicate route", err)
	}
	if registered := engine.Routes(); len(registered) != 0 {
		t.Fatalf("failed BindAll left %d route(s) registered: %v", len(registered), registered)
	}
}

// TestBindAllAllowsSamePathDifferentMethod covers the row Gin accepts: its tree
// is per method, so GET and DELETE on one path are two routes, not a conflict.
// TestBindAllAllowsSamePathDifferentMethod 覆盖 Gin 接受的那一行：它的路由树按方法
// 分开，同一路径上的 GET 与 DELETE 是两条路由，不算冲突。
func TestBindAllAllowsSamePathDifferentMethod(t *testing.T) {
	engine, r := newFixtureRouter()
	get := conflictRoute[testcontract.ItemRequest, testcontract.ItemResponse](
		"GetTrimA", http.MethodGet, "/api/test/multi/{id}")
	del := conflictRoute[testcontract.ItemRequest, testcontract.ItemResponse](
		"GetTrimB", http.MethodDelete, "/api/test/multi/{id}")
	if err := gofyadapter.BindAll(r, []endpoint.Descriptor{get, del}, routeConflictImpl{}, gofyadapter.BindOptions{}); err != nil {
		t.Fatalf("BindAll: %v", err)
	}
	if registered := engine.Routes(); len(registered) != 2 {
		t.Fatalf("registered %d route(s), want 2: %v", len(registered), registered)
	}
}

// TestBindAllRejectsGroupTrimmedCollision covers the prefix-normalization row:
// inside Group("/api/test") the full contract path is trimmed while the already
// relative one is kept, and both end up as the same Gin path. One route may use
// either style, so the two must be compared after bindPath, not before.
// TestBindAllRejectsGroupTrimmedCollision 覆盖前缀规范化那一行：在 Group("/api/test")
// 内，带全前缀的契约路径会被裁剪，已经是相对路径的保持原样，两者最终落在同一个 Gin
// 路径上。两种写法各自被支持，因此比较必须发生在 bindPath 之后。
func TestBindAllRejectsGroupTrimmedCollision(t *testing.T) {
	engine, r := newFixtureRouter()
	group := r.Group("/api/test")
	full := conflictRoute[testcontract.ItemRequest, testcontract.ItemResponse](
		"GetTrimA", http.MethodGet, "/api/test/collide/{id}")
	relative := conflictRoute[testcontract.ItemRequest, testcontract.ItemResponse](
		"GetTrimB", http.MethodGet, "/collide/{id}")
	err := gofyadapter.BindAll(group, []endpoint.Descriptor{full, relative}, routeConflictImpl{}, gofyadapter.BindOptions{})
	if err == nil || !strings.Contains(err.Error(), "duplicate route") {
		t.Fatalf("BindAll error = %v, want duplicate route", err)
	}
	// The reported gin path is group-relative: the adapter never sees the
	// deployment prefix the Router was built with.
	// 错误里的 gin 路径相对当前分组：adapter 看不到 Router 构建时的部署前缀。
	if !strings.Contains(err.Error(), "gin path /collide/:id") {
		t.Fatalf("error must name the normalized gin path: %v", err)
	}
	if registered := engine.Routes(); len(registered) != 0 {
		t.Fatalf("failed BindAll left %d route(s) registered: %v", len(registered), registered)
	}
}
