ARG GO_VERSION=1.26
ARG GOPROXY=https://goproxy.cn,direct
ARG GOSUMDB=sum.golang.google.cn

# ---------------------------------------------------------------------------
# 构建阶段：一次编译出 4 个二进制（user / video / interaction / worker）
# 拆分后每个服务是独立进程，镜像通过下面 4 个 target 分别产出，避免重复编译。
# 上下文 = server/：COPY 路径全部相对本目录，不再带 server/ 前缀。
# ---------------------------------------------------------------------------
FROM golang:${GO_VERSION} AS builder
ARG GOPROXY
ARG GOSUMDB
ENV GOPROXY=${GOPROXY} \
  GOSUMDB=${GOSUMDB}

#builder临时容器工作目录
WORKDIR /src/server

COPY go.mod go.sum ./

# 这里不用 --mount=type=cache：依赖必须落进这一层，registry cache（mode=max）
# 才能把它导出到 Harbor，换节点、换流水线时直接复用。
# cache mount 的内容不会被 cache export 带走，跨 TaskRun 等于没有缓存。
RUN go mod download

COPY . .

ENV CGO_ENABLED=0
# - `-trimpath`：去掉构建机绝对路径，panic 堆栈不泄露本地路径
# - `-ldflags="-s -w"`：去掉符号表与调试信息，二进制瘦身
# - 输出全部放到 /out/，后面各 target 从这里 COPY
# - `&&` 串联：任意一个编译失败，整个 RUN 直接失败
#
# 只保留 go-build 的 cache mount：编译中间产物不落进镜像层，
# 避免 registry cache 被 Go build cache 撑大。
# 注意不要再挂 /go/pkg/mod —— cache mount 会遮住上一层已下载的模块，
# 导致 go build 重新下载依赖，registry cache 的收益被抵消。
RUN --mount=type=cache,target=/root/.cache/go-build \
  go build -trimpath -ldflags="-s -w" -o /out/user ./cmd/user && \
  go build -trimpath -ldflags="-s -w" -o /out/video ./cmd/video && \
  go build -trimpath -ldflags="-s -w" -o /out/interaction ./cmd/interaction && \
  go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

# ---------------------------------------------------------------------------
# 运行阶段公共底座：仅含 CA 证书与时区数据，业务镜像在其上按 target 叠加
# ---------------------------------------------------------------------------
FROM alpine:3.21 AS base
# 运行用户固定为 UID 10001：业务进程不需要 root，镜像里也不留 root 默认用户。
# K8s 侧再配 runAsNonRoot: true 可以形成双重约束。
RUN apk add --no-cache ca-certificates tzdata \
  && addgroup -S -g 10001 app \
  && adduser -S -D -H -u 10001 -G app -s /sbin/nologin app \
  && mkdir -p /app/logs \
  && chown -R 10001:10001 /app
WORKDIR /app

# ---------------------------------------------------------------------------
# user-svc：HTTP 8081 / gRPC 9081，库 feed_user
# ---------------------------------------------------------------------------
FROM base AS user
COPY --from=builder /src/server/configs /app/configs
COPY --from=builder /out/user /app/user
EXPOSE 8081 9081
# 直接启动二进制：每个 target 只包含一个服务，不需要 entrypoint 再二次分发
USER 10001:10001
ENTRYPOINT ["/app/user"]

# ---------------------------------------------------------------------------
# video-svc：HTTP 8082 / gRPC 9082，库 feed_video
# 媒体文件（上传源、转码产物、封面）全部写入 MinIO 对象存储
# ---------------------------------------------------------------------------
FROM base AS video
COPY --from=builder /src/server/configs /app/configs
COPY --from=builder /out/video /app/video
EXPOSE 8082 9082
USER 10001:10001
ENTRYPOINT ["/app/video"]

# ---------------------------------------------------------------------------
# interaction-svc：HTTP 8083 / gRPC 9083，库 feed_interaction
# ---------------------------------------------------------------------------
FROM base AS interaction
COPY --from=builder /src/server/configs /app/configs
COPY --from=builder /out/interaction /app/interaction
EXPOSE 8083 9083
USER 10001:10001
ENTRYPOINT ["/app/interaction"]

# ---------------------------------------------------------------------------
# worker：无 HTTP/gRPC 端口，消费 Kafka 并调用 ffmpeg 转码，回刷 feed_video 库
# 需要 ffmpeg 运行时（转码 + 截帧生成封面），因此单独安装。
# worker 直接启动二进制，不使用 entrypoint。
# ---------------------------------------------------------------------------
FROM base AS worker
RUN apk add --no-cache ffmpeg
COPY --from=builder /src/server/configs /app/configs
COPY --from=builder /out/worker /app/worker
USER 10001:10001
ENTRYPOINT ["/app/worker"]
