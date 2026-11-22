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

// routePairRequest carries two placeholders, for the case where a contract path
// tries to put both in one segment.
// routePairRequest 带两个占位符，用于契约路径把两者塞进同一段的情形。
type routePairRequest struct {
	A string `uri:"a"`
	B string `uri:"b"`
}

type placeholderImpl struct{}

func (placeholderImpl) GetPair(_ context.Context, _ *routePairRequest) (*testcontract.ItemResponse, error) {
	return &testcontract.ItemResponse{}, nil
}

func (placeholderImpl) GetByID(_ context.Context, _ *testcontract.ItemRequest) (*testcontract.ItemResponse, error) {
	return &testcontract.ItemResponse{}, nil
}

// TestBindAllRejectsPlaceholderSharingSegment pins the shape Gin panics on:
// "/x/{a}-{b}" becomes "/x/:a-:b", and Gin allows one wildcard per segment. The
// panic would fire during registration, after validation said the set was fine
// and after earlier routes had gone live.
// TestBindAllRejectsPlaceholderSharingSegment 固定 Gin 会 panic 的形态："/x/{a}-{b}"
// 变成 "/x/:a-:b"，而 Gin 每段只允许一个通配符。panic 会发生在注册阶段——校验刚说完
// 没问题、前面的路由也已经上线之后。
func TestBindAllRejectsPlaceholderSharingSegment(t *testing.T) {
	engine, r := newFixtureRouter()
	desc := conflictRoute[routePairRequest, testcontract.ItemResponse](
		"GetPair", http.MethodGet, "/api/test/x/{a}-{b}")
	err := gofyadapter.BindAll(r, []endpoint.Descriptor{desc}, placeholderImpl{}, gofyadapter.BindOptions{})
	if err == nil || !strings.Contains(err.Error(), "whole segment") {
		t.Fatalf("BindAll error = %v, want whole-segment rejection", err)
	}
	if registered := engine.Routes(); len(registered) != 0 {
		t.Fatalf("rejected route was registered anyway: %v", registered)
	}
}

// TestBindAllRejectsPlaceholderWithSuffix covers the silent variant: Gin reads
// "/x/{id}.json" as a wildcard named "id.json", so the ui-tagged field "id"
// would stay empty and the handler would receive a zero value with no error.
// TestBindAllRejectsPlaceholderWithSuffix 覆盖静默的那一类：Gin 把 "/x/{id}.json"
// 读成名为 "id.json" 的通配段，uri:"id" 的字段因此一直为空，handler 拿到零值且没有报错。
func TestBindAllRejectsPlaceholderWithSuffix(t *testing.T) {
	engine, r := newFixtureRouter()
	desc := conflictRoute[testcontract.ItemRequest, testcontract.ItemResponse](
		"GetByID", http.MethodGet, "/api/test/x/{id}.json")
	err := gofyadapter.BindAll(r, []endpoint.Descriptor{desc}, placeholderImpl{}, gofyadapter.BindOptions{})
	if err == nil || !strings.Contains(err.Error(), "whole segment") {
		t.Fatalf("BindAll error = %v, want whole-segment rejection", err)
	}
	if registered := engine.Routes(); len(registered) != 0 {
		t.Fatalf("rejected route was registered anyway: %v", registered)
	}
}

// TestBindAllRejectsGinWildcardSyntax: ":id" and "*rest" are Gin syntax, not
// contract syntax. They skip pathPlaceholders entirely, so the uri-tag check
// never runs, while Gin still binds them as wildcards — a contract that the
// generated client cannot reproduce.
// TestBindAllRejectsGinWildcardSyntax：":id" 与 "*rest" 是 Gin 语法而非契约语法。它们
// 完全跳过 pathPlaceholders，uri 标签校验因此不执行，Gin 却仍按通配段绑定——生成的客户端
// 复现不了这样一条契约。
func TestBindAllRejectsGinWildcardSyntax(t *testing.T) {
	for _, path := range []string{"/api/test/x/:id", "/api/test/x/*rest"} {
		t.Run(path, func(t *testing.T) {
			engine, r := newFixtureRouter()
			desc := conflictRoute[testcontract.ItemRequest, testcontract.ItemResponse](
				"GetByID", http.MethodGet, path)
			err := gofyadapter.BindAll(r, []endpoint.Descriptor{desc}, placeholderImpl{}, gofyadapter.BindOptions{})
			if err == nil || !strings.Contains(err.Error(), "Gin wildcard syntax") {
				t.Fatalf("BindAll error = %v, want Gin-syntax rejection", err)
			}
			if registered := engine.Routes(); len(registered) != 0 {
				t.Fatalf("rejected route was registered anyway: %v", registered)
			}
		})
	}
}

// TestBindAllAcceptsWholeSegmentPlaceholder keeps the new rules from
// over-rejecting the normal shape.
// TestBindAllAcceptsWholeSegmentPlaceholder 确保新规则不会误伤正常形态。
func TestBindAllAcceptsWholeSegmentPlaceholder(t *testing.T) {
	engine, r := newFixtureRouter()
	desc := conflictRoute[testcontract.ItemRequest, testcontract.ItemResponse](
		"GetByID", http.MethodGet, "/api/test/x/{id}")
	if err := gofyadapter.BindAll(r, []endpoint.Descriptor{desc}, placeholderImpl{}, gofyadapter.BindOptions{}); err != nil {
		t.Fatalf("BindAll: %v", err)
	}
	if registered := engine.Routes(); len(registered) != 1 {
		t.Fatalf("registered %d routes, want 1: %v", len(registered), registered)
	}
}
