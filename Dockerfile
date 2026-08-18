# gotick 的容器镜像。装的是 gotick 这个 CLI，默认跑 ui 子命令，
# inspect 也一并带着（docker run ... gotick inspect <callId>）。
#
# 它只读 Redis，不连 worker，所以这个容器和你的业务进程之间没有任何耦合：
# 业务在哪跑、有没有在跑，都不影响它能不能看。

FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build

WORKDIR /src

# 先只拷依赖清单：依赖没变时这一层命中缓存，改代码不用重新下载模块
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# BuildKit 注入的目标平台。CGO 关掉之后是纯交叉编译，
# 不需要 QEMU，多架构构建和单架构一样快。
ARG TARGETOS
ARG TARGETARCH

# 版本号得从外面传进来。这里是在仓库里直接 go build，主模块版本只会是 "(devel)"，
# 而 .dockerignore 排掉了 .git，连 VCS 戳都没有——不注入的话，恰恰是发布出去的
# 这个产物说不清自己是哪一版。CI 传的是 docker/metadata-action 算出的那个版本。
ARG VERSION=""

# CGO_ENABLED=0 才能进 distroless/static；-s -w 去掉符号表和调试信息
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath \
    -ldflags="-s -w -X github.com/zbysir/gotick.version=${VERSION}" \
    -o /out/gotick ./cmd/gotick

FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.title="gotick" \
      org.opencontainers.image.description="Web inspector for gotick durable workflows. Reads Redis directly — no worker connection, no server to deploy." \
      org.opencontainers.image.source="https://github.com/zbysir/gotick" \
      org.opencontainers.image.licenses="MIT"

COPY --from=build /out/gotick /gotick

# 容器里必须监听 0.0.0.0 才够得着。
#
# 而 gotick 在非 loopback 地址上会强制要求密码：没设 GOTICK_UI_AUTH 就直接
# 拒绝启动。这不是配置疏漏，是有意的——这个界面会摊开每一个 flow 的元数据，
# 默认裸奔到公网比不能启动糟糕得多。所以跑这个镜像必须给 GOTICK_UI_AUTH。
ENV GOTICK_UI_ADDR=0.0.0.0:8088 \
    REDIS_URL=redis://localhost:6379/0

EXPOSE 8088

USER nonroot:nonroot

ENTRYPOINT ["/gotick"]
CMD ["ui"]
