# gotick ui

[gotick](https://github.com/zbysir/gotick) 的检查界面：看一眼你的流程现在跑到哪一步了。

它直接读 Redis，不连 worker——所以你的业务进程在哪跑、有没有在跑，都不影响这个容器能不能看。
镜像里装的是 `gotick` CLI，默认跑 `ui` 子命令。

## 跑起来

```bash
docker run -p 8088:8088 \
  -e REDIS_URL="redis://your-redis:6379/0" \
  -e GOTICK_UI_AUTH="admin:换成你自己的密码" \
  zbysir/gotick-ui
```

打开 <http://127.0.0.1:8088>，用上面那组用户名密码登录。

## 密码是必填的

容器里必须监听 `0.0.0.0` 才够得着，而 gotick 在非本机地址上会强制要求密码：
没设 `GOTICK_UI_AUTH` 就直接拒绝启动，并告诉你为什么。

这不是配置疏漏，是有意的——这个界面会摊开每一个 flow 的元数据（flow 名、调用 ID、
每一步的耗时和报错），默认裸奔到公网比不能启动糟糕得多。

## 配置

全部通过环境变量，没有配置文件。

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis 连接串，和你的 gotick 进程指向同一个 |
| `GOTICK_UI_AUTH` | 空 | `用户名:密码`。监听非本机地址时必填 |
| `GOTICK_UI_ADDR` | `0.0.0.0:8088`（镜像内） | 监听地址 |

## 顺带还有 inspect

同一个镜像也能在命令行里查单次调用，不用开界面：

```bash
docker run --rm -e REDIS_URL="redis://your-redis:6379/0" \
  zbysir/gotick-ui inspect <callId>
```

## 标签

| 标签 | 指向 |
| --- | --- |
| `latest` | 最新的发布版本 |
| `1.2.3` / `1.2` | 具体版本 |
| `edge` | 默认分支的最新提交，会变，别用在生产上 |

支持 `linux/amd64` 和 `linux/arm64`。基础镜像是 `distroless/static`，以 nonroot 用户运行，
镜像约 10 MB。

## 不用容器也行

界面本身是一个普通的 `http.Handler`，可以直接挂到你已有的 mux 上，
连这个容器都不用起。见[仓库说明](https://github.com/zbysir/gotick)。

MIT License.
