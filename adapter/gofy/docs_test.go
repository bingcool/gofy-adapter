package gofyadapter_test

import (
	"net/http"
	"strings"
	"testing"

	gofyadapter "github.com/bingcool/gofy-adapter/adapter/gofy"
	"github.com/bingcool/gofy-interface-api/docsui"
)

var docsSpec = []byte(`{"openapi":"3.0.3","info":{"title":"t","version":"1"},"paths":{}}`)

func TestDocsModuleServesOnlyWhenEnabled(t *testing.T) {
	engine, router := newFixtureRouter()
	if err := gofyadapter.DocsModule(docsui.Config{Spec: docsSpec}).Register(router); err != nil {
		t.Fatalf("Register(disabled): %v", err)
	}
	if rec := serve(engine, http.MethodGet, "/docs"); rec.Code != http.StatusNotFound {
		t.Fatalf("disabled docs status = %d, want 404", rec.Code)
	}

	engine, router = newFixtureRouter()
	if err := gofyadapter.DocsModule(docsui.Config{Spec: docsSpec, Enabled: true}).Register(router); err != nil {
		t.Fatalf("Register(enabled): %v", err)
	}
	rec := serve(engine, http.MethodGet, "/openapi.json")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"openapi"`) {
		t.Fatalf("spec status = %d body = %q", rec.Code, rec.Body.String())
	}
	if rec := serve(engine, http.MethodGet, "/docs"); rec.Code != http.StatusOK {
		t.Fatalf("docs status = %d, want 200", rec.Code)
	}
}

// TestDocsModuleRejectsRootPath pins the gin limitation behind the refusal:
// "/" is an exact match, so the UI's assets would 404, and a catch-all cannot
// coexist with the spec route on the same engine. Failing at registration beats
// serving a page that is broken for a reason invisible in the config.
// TestDocsModuleRejectsRootPath 固定拒绝背后的 gin 限制："/" 是精确匹配，UI 的静态
// 资源会 404，而 catch-all 无法与 spec 路由共存于同一个 engine。在注册阶段失败，好过
// 提供一个坏掉却看不出原因的页面。
func TestDocsModuleRejectsRootPath(t *testing.T) {
	_, router := newFixtureRouter()
	err := gofyadapter.DocsModule(docsui.Config{
		Enabled:  true,
		Spec:     docsSpec,
		DocsPath: "/",
	}).Register(router)
	if err == nil {
		t.Fatal(`Register accepted DocsPath="/", which serves the UI without its assets`)
	}
	if !strings.Contains(err.Error(), "DocsPath") {
		t.Fatalf("error does not name the setting: %v", err)
	}
}
