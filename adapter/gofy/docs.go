package gofyadapter

import (
	"github.com/bingcool/gofy-interface-api/docsui"
	"github.com/bingcool/gofy/support"
	"github.com/gin-gonic/gin"
)

type docsModule struct {
	cfg docsui.Config
}

// DocsModule mounts the Swagger UI and OpenAPI document as a gofy module.
//
// Mounting is not enough: docs are served only when cfg.Enabled is true. A
// deployment that keeps the registration in every profile closes the UI with
// that one flag instead of forking the module list. Enable it only outside
// production or behind an internal gateway.
// DocsModule 以 gofy module 形式挂载 Swagger UI 与 OpenAPI 文档。
//
// 仅挂载模块并不够：只有 cfg.Enabled 为 true 时才会提供文档。在所有环境都保留注册
// 的部署只需关闭这个开关即可隐藏 UI，不必为生产环境另写一份 module 列表。只应在
// 非生产环境或内网网关之后启用。
func DocsModule(cfg docsui.Config) support.Module {
	return &docsModule{cfg: cfg}
}

func (m *docsModule) Name() string { return "docs" }

func (m *docsModule) Register(r *support.Router) error {
	// Respect Enabled instead of forcing it: the zero value must not publish the
	// API documentation of a production process by accident.
	// 尊重 Enabled 而不是强制置位：零值配置不能意外发布生产进程的 API 文档。
	if !m.cfg.Enabled {
		return nil
	}
	h, err := docsui.Handler(m.cfg)
	if err != nil {
		return err
	}
	docsPath := m.cfg.EffectiveDocsPath()
	r.GET(docsPath, gin.WrapH(h))
	if docsPath != "/" {
		// Swagger UI loads its assets from DocsPath/.
		// Swagger UI 会从 DocsPath/ 下加载静态资源。
		r.GET(docsPath+"/*any", gin.WrapH(h))
	}
	r.GET(m.cfg.EffectiveSpecPath(), gin.WrapH(h))
	return nil
}
