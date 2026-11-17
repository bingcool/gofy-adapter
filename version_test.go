package adapter

import "testing"

// TestVersion pins the module version. The constant is the single source of
// truth; a release that bumps it should also update the release notes, so an
// accidental edit is worth a failing test.
// TestVersion 固定模块版本。常量是唯一来源；发布时改它要同时更新发布说明，因此误改值得
// 让测试失败。
func TestVersion(t *testing.T) {
	if Version != "1.0.0" {
		t.Fatalf("Version = %q; changing it means a release, not just a constant edit", Version)
	}
}
