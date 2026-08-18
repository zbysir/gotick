package gotick

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersionPrefersInjected(t *testing.T) {
	old := version
	t.Cleanup(func() { version = old })

	version = "v0.5.0"
	assert.Equal(t, "v0.5.0", Version())

	// 容器那条路上传进来的是 metadata-action 给的、不带 v 的版本
	version = "0.5.0"
	assert.Equal(t, "v0.5.0", Version(), "同一个版本在不同产物上不该长得不一样")

	// master 分支推出来的镜像
	version = "edge"
	assert.Equal(t, "edge", Version())
}

func TestVersionFallsBackWithoutInjection(t *testing.T) {
	old := version
	t.Cleanup(func() { version = old })

	// 没有注入时走构建信息。具体落到哪一级取决于怎么构建的——go test 不打 VCS 戳，
	// 但 go build 会从最近的 tag 推出 "v0.4.0+dirty" 这种——所以这里只钉住不变量：
	// 总得说出点什么，而且绝不能把内部的「不知道」原样吐出去。
	version = ""
	got := Version()

	assert.NotEmpty(t, got)
	for _, bad := range []string{"", "(devel)", "unknown"} {
		assert.NotEqual(t, bad, got, "内部表示不该漏到界面上")
	}
}

func TestNormalizeVersion(t *testing.T) {
	// 认不出来的一律当作没有，让 Version 继续往下找
	for _, in := range []string{"", "  ", "(devel)", "devel", "unknown"} {
		assert.Empty(t, normalizeVersion(in), "输入 %q", in)
	}

	assert.Equal(t, "v1.2.3", normalizeVersion("1.2.3"))
	assert.Equal(t, "v1.2.3", normalizeVersion("v1.2.3"))
	assert.Equal(t, "v1.2.3", normalizeVersion("  v1.2.3  "))
	assert.Equal(t, "edge", normalizeVersion("edge"))
}
