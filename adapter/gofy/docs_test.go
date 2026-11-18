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
