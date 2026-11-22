## micro-bilibili
一个精简版仿 b 站风格的微服务全栈项目：Go 后端拆成 **user / video / interaction** 三个 HTTP + gRPC 服务与一个 **worker** 异步进程，前端为 Vue 3 单页应用。本仓库只保留业务源码与 Dockerfile，部署清单由独立 manifest 仓库维护。

相比完整业务版本，本仓库刻意收窄了功能边界，把重点放在**可部署、可观测、可演进的工程结构**上，定位是「云原生运维 / SRE 作品线的业务底座」——用一套真实但足够小的微服务拓扑，承载链路追踪、指标采集、健康检查、异步消息与多库拆分等运维实践。

## 技术栈

**后端（Go）**
- 语言/框架：Go 1.26 + Gin（HTTP）+ gRPC（服务间调用）
- 数据库：MySQL 8，单实例三库分治（`feed_user` / `feed_video` / `feed_interaction`）
- 缓存：Redis 7（RDB + AOF 混合持久化）
- 消息队列：Kafka（单节点 KRaft，无 ZooKeeper）
- 对象存储：MinIO（S3 兼容；视频/封面/头像统一走对象存储，DB 只保存对象 key）
- 转码：ffmpeg（worker 消费 Kafka 任务后转码 + 截帧生成封面）
- 鉴权：JWT（Access + Refresh 双 Token）
- 配置：YAML + `APP_*` 环境变量覆盖
- 可观测性：OpenTelemetry Go SDK（Trace + Metrics）+ OTel Collector（gateway 收 OTLP / agent 采 stdout）+ Tempo（Trace）+ Loki（日志）+ Prometheus（指标）+ Grafana（展示）

**前端（Vue 3）**
- Vue 3 + TypeScript + Vite
- Pinia（状态管理）、Vue Router、Tailwind CSS
- 播放器：ArtPlayer
- Axios

**部署**
- 镜像构建：Docker 多阶段构建（`user` / `video` / `interaction` / `worker` / `web` 五个 target）
- 应用编排：Helm Chart 由独立 manifest 仓库（`https://github.com/phx3334/micro-bl--manifest`）维护，只保留 `values-prod.yaml` 一套生产配置
- 平台组件（MySQL / Redis / Kafka / 观测栈 / Harbor / Tekton / ArgoCD）同样由该 manifest 仓库管理：bootstrap 阶段按各组件 README 手工 `helm upgrade` 安装，ArgoCD 就绪后只接管 `infra/overlays/prod` 与业务 Chart 两个 Application

## 目录结构

```
micro-bl/
├── server/                          # Go 后端（4 个进程入口）
│   ├── cmd/                         #   user / video / interaction / worker
│   ├── internal/                    #   业务域（user / video / interaction / model / dto）
│   ├── api/                         #   proto 与生成的 pb
│   ├── pkg/                         #   共享基建库
│   ├── configs/config.yaml          #   配置模板（${ENV:-default} 占位）
│   ├── Dockerfile                   #   多 target：user / video / interaction / worker
│   └── go.mod
├── web/                             # Vue 3 前端
│   ├── src/                         #   源码（api / views / components / stores）
│   ├── public/                      #   静态资源（favicon、默认头像）
│   ├── Dockerfile                   #   node 构建 + nginx 静态托管
│   └── nginx.conf                   #   只托管 SPA，不反代 API
├── project-infra/                   # 独立 manifest 仓库（Terraform / Ansible / Helm / ArgoCD / Tekton）
├── docs/                            # 博客草稿等非部署资料
├── .env.example                     # 本地开发环境变量样例
└── README.md
```

## 服务与端口

| 服务 | HTTP | gRPC | 数据库 | 职责 |
|------|------|------|--------|------|
| `user` | 8081 | 9081 | `feed_user` | 注册登录、JWT、验证码、资料与头像、关注/粉丝、个人主页 |
| `video` | 8082 | 9082 | `feed_video` | 视频上传、转码状态、详情/列表、分区排行、删除 |
| `interaction` | 8083 | 9083 | `feed_interaction` | 点赞、评论、评论点赞 |
| `worker` | 9091（仅健康探针） | 无 | `feed_video` | 消费 `video.transcode` 等 Kafka 消息、ffmpeg 转码、计数回刷、热度重建 |

三个 HTTP 服务与 worker 均暴露 `/health`（存活）与 `/ready`（就绪）；指标通过 OTLP 推送到 otel-collector，由 Prometheus 抓取 collector 的 `:8889`。前端由 nginx 托管（生产在 Istio Ingress Gateway 之后）。

## 本机访问（prod）

只保留 prod 一套环境。只有给人看的入口开 NodePort，内部组件保持 ClusterIP，排障用 `kubectl port-forward`。

| 组件 | 地址 | 用途 |
|------|------|------|
| 前端 + API | `http://<master1-IP>:30080` | web 页面与 `/api/v1/*`（Istio ingressgateway） |
| Harbor | `https://<master1-IP>:30003` | 镜像仓库（admin / 147258） |
| ArgoCD | `http://<master1-IP>:30082` | GitOps（admin / 147258） |
| Tekton webhook | `http://<master1-IP>:30081` | GitHub Webhook Payload URL |
| Grafana | `http://<master1-IP>:30300` | 统一看板（phx / 见 grafana-admin Secret） |
| MinIO API | `https://<master1-IP>:30900` | 视频/封面/头像直链（内部 CA 签发，浏览器需信任 CA） |
| Hubble UI | `http://<master1-IP>:30400` | 网络观测 |

Prometheus、Alertmanager、Tempo、Loki、otel-collector 均为 ClusterIP，排障时：

```bash
kubectl -n observability port-forward svc/prometheus-kube-prometheus-prometheus 9090:9090
kubectl -n observability port-forward svc/tempo 3200:3200
```

前置：

```bash
# 所有 REPLACE_WITH_SERVER_IP 统一填第一台 master 的公网 IP：
# Harbor / MinIO 证书的 IP SAN 只签这一台，填别的节点会 x509 失败。
grep -rn "REPLACE_WITH_SERVER_IP" project-infra

# 浏览器播放 MinIO 视频前，把内部根 CA（micro-bl-ca 的 ca.crt）导入系统/浏览器信任库
```

## 依赖

| 依赖 | 默认地址 | 说明 |
|------|----------|------|
| MySQL 8 | `localhost:3306` | 三库由 manifest 仓库的 `init/01-databases.sql` 首次启动时自动创建（业务账号 `phx`） |
| Redis 7 | `localhost:6379` | 缓存、ZSet 排行、验证码存储；密码见 manifest 仓库的 Redis 配置 |
| Kafka | `localhost:9092` | 转码任务与计数增量；单节点 KRaft 即可，允许自动建 topic |
| MinIO | `localhost:9000` | 视频/封面/头像对象存储；bucket 启动时自动创建。未启动 MinIO 时 user/video/worker 会启动失败（预期行为） |
| otel-collector | `localhost:4317` | 可选；不部署时把 `APP_OTEL_ENABLED` 设为 `false` |

## 环境变量

所有配置以 `configs/config.yaml` 为默认值，用同名 `APP_*` 环境变量覆盖；常用变量表见下方。本地开发时把根目录的 `.env` 复制到 `server/.env`（`godotenv.Load(".env")` 从进程当前目录读取，`go run` 在 `server/` 下执行）。

| 变量 | 说明 | 默认 / 示例 |
|------|------|-------------|
| `APP_JWT_ACCESS_TOKEN_SECRET` / `APP_JWT_REFRESH_TOKEN_SECRET` | JWT 双 Token 密钥 | 空（生产必填） |
| `APP_DATABASE_HOST/PORT/USER/PASSWORD` | MySQL 连接 | `localhost/3306/root` |
| `APP_DATABASE_DBNAME` | 本服务的库：`feed_user` / `feed_video` / `feed_interaction` | 按服务默认 |
| `APP_SERVER_PORT` / `APP_SERVER_GRPC_PORT` | 本服务 HTTP / gRPC 端口 | `8081|8082|8083` / `9081|9082|9083` |
| `APP_GRPC_USER_ADDR` / `APP_GRPC_VIDEO_ADDR` / `APP_GRPC_INTERACTION_ADDR` | 下游 gRPC 地址 | `localhost:9081/9082/9083` |
| `APP_REDIS_HOST/PORT/PASSWORD/DB/KEY_PREFIX` | Redis 连接与键前缀 | `localhost/6379` |
| `APP_KAFKA_BROKERS` | Kafka broker 地址 | `localhost:9092` |
| `APP_MINIO_ENDPOINT` | MinIO 地址（host:port） | `localhost:9000` / 集群内 `minio:9000` |
| `APP_MINIO_ACCESS_KEY` / `APP_MINIO_SECRET_KEY` | MinIO 凭证（K8s 中来自 Secret） | 空（必填） |
| `APP_MINIO_BUCKET` | 对象存储桶（启动时自动创建） | `micro-bl` |
| `APP_MINIO_USE_SSL` | MinIO 是否走 TLS | `false` |
| `APP_MINIO_PUBLIC_READ` | `true` 公开直链；`false` 预签名 URL | `true` |
| `APP_MINIO_URL_EXPIRY` | 预签名 URL 有效期（Go duration） | `24h` |
| `APP_UPLOAD_MAX_FILE_SIZE` | 单文件上传上限（字节） | `1073741824`（1GB） |
| `APP_TRANSCODE_TIMEOUT` | 单次转码超时（秒） | `1800` |
| `APP_FFMPEG_BINARY_PATH` / `OUTPUT_CODEC` / `CRF` / `PRESET` | ffmpeg 参数 | `ffmpeg` / `libx264` / `23` / `medium` |
| `APP_CAPTCHA_LENGTH` | 图形验证码长度 | `6` |
| `APP_SKIP_CAPTCHA` | 跳过图形验证码（仅演示环境开启） | `false` |
| `APP_RUN_CRON` | 是否启用定时任务（仅 worker 为 `true`） | `false` |
| `APP_OTEL_ENABLED` | 是否启用链路追踪 | `false`（`.env` 中为 `true`） |
| `APP_OTEL_ENDPOINT` | OTLP gRPC 地址 | `localhost:4317` |
| `APP_DATABASE_AUTO_MIGRATE` | 启动时自动建表（prod 首次部署为 `true`，建表后改回 `false`） | `false` |

## 已实现功能

- **鉴权主线**：注册/登录（用户名 + 密码 + 图形验证码）、图形验证码、JWT 双 Token（Access + Refresh）、登出、头像上传、资料修改。
- **上传转码主线**：大文件表单上传（1GB / 1 小时超时）→ 源文件直传 MinIO `video/src/{objectID}.{ext}` → 落库（只存 key，状态 `transcoding`）→ 投递 Kafka `video.transcode` → worker 下载源文件、ffmpeg 转码并截帧生成封面 → 产物回写 MinIO `video/{id}.mp4` / `cover/{id}.jpg`，状态置 `published` 或 `failed` → 前端轮询 `video/status`。
- **点赞/播放计数主线**：点赞、评论等写路径只产生 Redis 计数与 Kafka 增量消息，由 worker 聚合后回刷 `videos` 表，避免高并发直写数据库。
- **播放链路**：视频详情、公开列表与热度排序（Redis ZSet）；详情页通过 interaction-svc / user-svc 回显当前用户的点赞与关注状态；视频/封面/头像地址由后端在响应时对对象 key 签名（公开直链或预签名），前端直接消费绝对 URL。
- **社交互动**：关注/取关与粉丝列表、评论与评论点赞、个人主页聚合统计（通过 gRPC 跨服务聚合）。

## 可观测性

- **健康检查**：`user`/`video`/`interaction` 提供 `GET /health`（存活：纯进程检查、零 IO，恒 200）与 `GET /ready`（就绪：`Ping` MySQL 与 Redis，任一不可用返回 503），分别对接 K8s liveness 与 readiness 探针——依赖故障只摘流量不重启，避免重启风暴。
- **指标**：RED 指标（`calls_total` 含 `http_status_code`/`http_route` 维度、秒级延迟直方图 `duration_seconds_bucket`）由 otel-collector 的 **spanmetrics 从 server span 派生**（零代码埋点，且必须在进尾采之前派生以防错误率失真）；5 个业务指标（转码成败、上传成败三分类、Kafka 发布健康、计数回刷账龄、登录成败）由业务代码埋点产出——它们依赖业务语义或根本不在 span 上，不受采样影响；指标名统一由 collector 的 prometheus exporter 翻译（counter 补 `_total`、有单位的补单位，业务代码只写裸名）；两类指标统一 OTLP 推送到 otel-collector `:8889`，Prometheus 只抓 collector 一个目标（服务身份靠 `service_name` 标签）。
- **Trace**：各服务通过 OTLP gRPC 上报到 `otel-collector.observability:4317`（跨命名空间必须带 namespace），collector 转发到 Tempo；HTTP 请求、gRPC 调用、MySQL/Redis 访问、Kafka 消息均有自动埋点，服务名为 `user-svc` / `video-svc` / `interaction-svc` / `worker-svc`。
- **展示**：Grafana `http://<master1-IP>:30300`（NodePort），数据源由 manifest 仓库的 Grafana provisioning 自动注入——Prometheus（uid `prometheus`，默认数据源）与 Tempo（uid `tempo`，支持 trace → metrics 下钻）。
- **开关**：`APP_OTEL_ENABLED=false` 时观测初始化完全 noop，不产生 exporter 与额外开销；采集器不可用时只降级记日志，不阻断业务启动。

## 数据初始化
manifest 仓库中的 Chart 渲染的资源：3 个 HTTP+gRPC 服务（Deployment + 双端口 Service）、worker（无端口）、web、ConfigMap、Secret（prod 使用 `existingSecret` + SealedSecret/外部 Secret，仓库不含明文凭据）、可选 HPA。入口由 Istio 的 Gateway + VirtualService 走 istio-ingressgateway（不再渲染 K8s Ingress）。媒体对象存 MinIO，Chart 不再渲染 uploads PVC/Volume。

几个设计取舍：

- **API 由入口网关直连后端**，不再经 web 的 nginx 反代，少一跳；web 只托管静态资源。
- **媒体统一走 MinIO**：user 写头像、video 写上传源、worker 下载源并写转码产物，三处共用同一 bucket；DB/Redis 只保存对象 key，响应时签名成直链或预签名 URL。
- **敏感配置由外部 Secret 提供**：仓库只保留 `values-prod.yaml` 的 `existingSecret` 引用，真实凭据由 SealedSecret 或 CI 注入；MinIO 凭证键名为 `APP_MINIO_ACCESS_KEY` / `APP_MINIO_SECRET_KEY`。
- **业务服务之间不加启动依赖**：gRPC 客户端懒连接，下游未就绪只影响单次调用。
- **灰度发布**：把 Deployment 换成 Argo Rollouts 的 `Rollout` 即可接 Prometheus 指标门禁，当前 Chart 保持最小依赖默认用原生 Deployment。

## 常用说明

- **Redis 持久化**：RDB + AOF 混合模式，配置由 manifest 仓库维护。
- **默认头像**：前端静态兜底 `web/public/images/avatar-default.jpg`，注册用户头像为空时直接使用；不再依赖服务端种子文件或 uploads 目录。
- **上传超时**：HTTP 读写超时、handler context、Istio VirtualService（1800s）与前端 axios 统一为 1 小时；API 不再经 nginx 反代，nginx 只托管静态资源。
