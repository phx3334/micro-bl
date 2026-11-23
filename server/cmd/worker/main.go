// worker 进程入口（无 HTTP/gRPC）。
//
// 职责（由 APP_WORKER_MODE 决定启用哪些，见下方 mode 解析）：
//  1. 消费 video.transcode：调用 ffmpeg 转码并回写 feed_video 库（transcode/all）
//  2. 消费 video.playcount / video.likecount / video.commentcount：
//     内存聚合后定期回刷 videos 的 play_count / likes_count / comments_count（counters/all）
//  3. cfg.RunCron=true 时执行热度 ZSet 重建定时任务（counters/all）
//
// 库：feed_video；不调用任何服务间 gRPC。
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/robfig/cron/v3"
	"go.uber.org/zap"

	"micro-bl/internal/video"
	"micro-bl/pkg/config"
	"micro-bl/pkg/gormx"
	"micro-bl/pkg/kafkax"
	"micro-bl/pkg/log"
	"micro-bl/pkg/otelx"
	"micro-bl/pkg/redisx"
	"micro-bl/pkg/storagex"
)

func main() {
	// 1. 加载配置
	cfg, err := config.LoadConfig()
	if err != nil {
		zap.L().Error("加载配置失败", zap.Error(err))
		os.Exit(1)
	}
	if os.Getenv("APP_DATABASE_DBNAME") == "" {
		cfg.Database.DBName = "feed_video"
	}

	logger := log.InitZap(&cfg.Zap)

	// 0. 运行模式（APP_WORKER_MODE）：拆分部署用，决定本进程消费哪些 topic。
	//    all（默认）  计数聚合 + 转码全跑（本地调试 / 单进程兼容）
	//    counters     只跑三个计数消费组 + cron（部署为 worker-counters）
	//    transcode    只跑转码消费（部署为 worker-transcode，ffmpeg CPU 密集）
	// 镜像共用，模式由 Deployment 的 env 注入，扩缩容互不拖累。
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("APP_WORKER_MODE")))
	switch mode {
	case "":
		mode = "all"
	case "all", "counters", "transcode":
	default:
		logger.Error("未知的 APP_WORKER_MODE，可选 all/counters/transcode", zap.String("mode", mode))
		os.Exit(1)
	}
	runCounters := mode == "all" || mode == "counters"
	runTranscode := mode == "all" || mode == "transcode"

	// worker 只有 :9091 探针端口（无业务端口、不暴露 /metrics，指标走 OTLP 推送），
	// 但仍要初始化 OTel：MySQL / Redis / Kafka 的自动埋点与转码 span 都依赖全局 Provider 与 propagator。
	cfg.OTEL.ServiceName = "worker-svc"
	otelShutdown, otelErr := otelx.Init(context.Background(), cfg)
	if otelErr != nil {
		logger.Error("OpenTelemetry 初始化失败，观测能力降级", zap.Error(otelErr))
	}

	// 2. 连接 feed_video 库。迁移由 video-svc 负责，worker 不执行 DDL，避免多进程竞争。
	db := gormx.InitGorm(cfg)

	// Redis 只有 ZSet 重建 cron 用；transcode 模式不连 Redis，少一个故障耦合点。
	var redisConn *redisx.RedisConn
	var redisClient *redisx.RedisClient
	if runCounters {
		redisConn, err = redisx.ConnectRedis(&cfg.Redis)
		if err != nil {
			logger.Error("Redis 初始化失败", zap.Error(err))
			os.Exit(1)
		}
		redisClient = redisx.NewRedisClient(redisConn.Client, redisConn.KeyPrefix)
	}

	// 3. 信号处理：SIGINT / SIGTERM 任一到达即触发优雅停机
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 4. 热度 ZSet 重建定时任务（仅 counters 模式且 cfg.RunCron=true 时启动；
	//    transcode 模式跑 cron 没有意义，还会多一份 Redis 依赖）
	var c *cron.Cron
	if cfg.RunCron && runCounters {
		rankingTask := video.NewVideoRankingTask(video.NewVideoRepo(db), redisClient, db, logger)
		c = InitCron(logger)
		rebuildJob := cron.FuncJob(func() {
			if err := rankingTask.RebuildZSetWithLock(context.Background()); err != nil {
				logger.Error("视频热度 ZSet 重建失败", zap.Error(err))
			}
		})
		if _, err := c.AddJob("@every 1m", RecoverCronJob(logger)(rebuildJob)); err != nil {
			logger.Error("注册 cron 任务失败", zap.Error(err))
		}
		c.Start()
		logger.Info("cron started in worker process")
	} else {
		logger.Info("cron disabled in worker process", zap.String("mode", mode), zap.Bool("run_cron", cfg.RunCron))
	}

	// 5. 计数聚合器：每个 videos 计数列一个 pending map + flush 协程（仅 counters 模式）
	var wg sync.WaitGroup

	if runCounters {
		playAgg := newCounterAggregator(db, "play_count", logger)
		likeAgg := newCounterAggregator(db, "likes_count", logger)
		commentAgg := newCounterAggregator(db, "comments_count", logger)

		type consumeTarget struct {
			agg     *counterAggregator
			topic   string
			groupID string
		}
		targets := []consumeTarget{
			{playAgg, kafkax.TopicVideoPlaycount, "worker-playcount"},
			{likeAgg, kafkax.TopicVideoLikecount, "worker-likecount"},
			{commentAgg, kafkax.TopicVideoCommentcount, "worker-commentcount"},
		}

		for _, t := range targets {
			wg.Add(2)
			go func(t consumeTarget) {
				defer wg.Done()
				t.agg.runFlusher(ctx)
			}(t)
			go func(t consumeTarget) {
				defer wg.Done()
				t.agg.consumeLoop(ctx, &cfg.Kafka, t.topic, t.groupID)
			}(t)
		}
	}

	// 6. 转码消费者（仅 transcode 模式）：Storage 实例用于下载源文件、上传转码产物。
	// MinIO 是转码的核心依赖，初始化失败直接退出，避免消费无法完成的任务。
	// counters 模式不连 MinIO：计数回刷不需要它，依赖面越小扩缩容越稳。
	if runTranscode {
		storageInstance, err := storagex.NewStorage(&cfg.Minio)
		if err != nil {
			logger.Error("failed to init MinIO storage for transcode worker", zap.Error(err))
			os.Exit(1)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			// 单次转码超时来自 APP_TRANSCODE_TIMEOUT（秒），0 时由 transcodeOne 兜底 30 分钟
			transcodeTimeout := time.Duration(cfg.Transcode.Timeout) * time.Second
			transcodeConsumeLoop(ctx, &cfg.Kafka, db, storageInstance, cfg.FFmpeg, transcodeTimeout, logger)
		}()
	}

	logger.Info("Worker 已启动",
		zap.String("mode", mode),
		zap.Bool("counters", runCounters),
		zap.Bool("transcode", runTranscode))

	// 心跳只供 liveness 探针使用：它证明进程调度与 HTTP 栈还活着，
	// 不代表 Kafka 消费在推进——消费停滞由 KafkaConsumerLagHigh/Growing 告警负责。
	// 不再额外导出同名指标：反复重启由 kube-prometheus-stack 的 KubePodCrashLooping 覆盖。
	var lastBeat atomic.Int64
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			lastBeat.Store(time.Now().Unix())
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	healthSrv := newHealthServer(db, &lastBeat)
	go func() {
		logger.Info("worker health endpoint listening", zap.String("addr", healthSrv.Addr))
		if err := healthSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("worker health server exited", zap.Error(err))
		}
	}()

	// 7. 阻塞等待信号
	<-ctx.Done()
	logger.Info("worker shutdown signal received")

	// 8. 关闭 cron（仅当启用），等待正在运行的 job 结束
	if c != nil {
		logger.Info("stopping cron...")
		cronCtx := c.Stop()
		select {
		case <-cronCtx.Done():
			logger.Info("cron stopped, all jobs finished")
		case <-time.After(5 * time.Second):
			logger.Warn("cron stop timeout (5s), some jobs may still be running")
		}
	}

	// 9. 等待消费/聚合协程退出（带 10 秒兜底）。
	// 聚合器在 ctx.Done 后会做最后一次 flush；必须等它们完成再关闭 DB。
	logger.Info("waiting for goroutines to exit...")
	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()
	select {
	case <-waitCh:
		logger.Info("all goroutines exited")
	case <-time.After(10 * time.Second):
		logger.Warn("worker goroutines exit timeout (10s), forcing shutdown")
	}

	// 健康端口最后关：排在消费协程退出之后，避免停机过程中探针还在报 Ready
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := healthSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("worker health server shutdown error", zap.Error(err))
	}

	// 10. 释放资源
	// OTel 关闭要在连接释放之前：flush BatchSpanProcessor 中尚未发送的 span
	otelCtx, otelCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer otelCancel()
	if err := otelShutdown(otelCtx); err != nil {
		logger.Error("otel shutdown error", zap.Error(err))
	}

	if redisConn != nil {
		if err := redisConn.Client.Close(); err != nil {
			logger.Error("关闭 Redis 失败", zap.Error(err))
		}
	}
	if sqlDB, dbErr := db.DB(); dbErr == nil {
		_ = sqlDB.Close()
	}
}
