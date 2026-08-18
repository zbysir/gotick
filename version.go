package gotick

import (
	"runtime/debug"
	"strings"
)

// modulePath 是这个库的模块路径，用来在构建信息里找自己那一条。
const modulePath = "github.com/zbysir/gotick"

// version 由链接器注入，给那些拿不到模块版本的构建用：
//
//	go build -ldflags="-X github.com/zbysir/gotick.version=v0.5.0"
//
// 容器镜像就属于这种——它在仓库里直接 go build，主模块版本只会是 "(devel)"，
// 而 .dockerignore 排掉了 .git，连 VCS 戳也没有。恰恰是发布出去的那个产物
// 最需要能说清自己是哪一版，所以留这个注入点。
var version string

// Version 返回这个 gotick 是哪一版，形如 "v0.5.0"。
//
// 说不清的时候返回 "devel" 而不是编一个版本号：界面上写着一个错的版本，
// 比写着「这是个开发构建」有害得多。
//
// 优先级从高到低：
//  1. 链接器注入的 version
//  2. 构建信息里的依赖版本——被当作库引入时（ui.NewHandler 挂在业务进程里，
//     也是最常见的用法）这一条最权威，而且完全自动
//  3. 主模块版本——go install ...@v0.5.0 装出来的 CLI 走这条
//  4. VCS 修订号——仓库里本地构建时走这条，短哈希总比什么都没有好
func Version() string {
	if v := normalizeVersion(version); v != "" {
		return v
	}

	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "devel"
	}

	for _, dep := range info.Deps {
		if dep.Path == modulePath {
			if v := normalizeVersion(dep.Version); v != "" {
				return v
			}
		}
	}

	if info.Main.Path == modulePath {
		if v := normalizeVersion(info.Main.Version); v != "" {
			return v
		}
	}

	// 走到这里说明是仓库里直接构建的。有 .git 时 Go 会打上修订号，
	// 拿它的短哈希；工作区有改动就标出来，免得把一个改过的构建当成那个提交。
	var revision, modified string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			modified = s.Value
		}
	}
	if revision != "" {
		if len(revision) > 12 {
			revision = revision[:12]
		}
		if modified == "true" {
			return "devel-" + revision + "-dirty"
		}
		return "devel-" + revision
	}

	return "devel"
}

// normalizeVersion 把拿到的版本串收拾成可以直接显示的样子，
// 认不出来的一律当作没有。
func normalizeVersion(v string) string {
	v = strings.TrimSpace(v)
	switch v {
	case "", "(devel)", "devel", "unknown":
		return ""
	}

	// Docker 那条路上的版本来自 metadata-action，它给的是不带 v 的 "0.5.0"。
	// 补上前缀，免得同一个版本在不同产物上长得不一样。
	if v[0] >= '0' && v[0] <= '9' {
		return "v" + v
	}
	return v
}
