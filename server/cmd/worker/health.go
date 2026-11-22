package main

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"gorm.io/gorm"
)

// newHealthServer 构造 worker 的只读健康端点。
//
// 为什么单独开端口：worker 没有业务端口，但 K8s 探针需要一个可访问端点。
// 两个探针的语义必须分开，否则会把"依赖故障"误判成"进程故障"：
//   - /health（liveness）：只看心跳是否还在推进，**不查任何外部依赖**。
//     依赖抖动重启解决不了，查依赖会让 Kafka/MySQL 每次抖动都触发全体 worker 重启。
//   - /ready（readiness）：探 MySQL（计数回刷与转码结果都写这个库），
//     不可用就返回 503，滚动更新时不会被当成"可用副本"。
func newHealthServer(db *gorm.DB, lastBeat *atomic.Int64) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		// 心跳每 15s 一次；90s 没更新说明进程调度已停摆。
		// 消费是否推进看 Kafka lag 告警，不在 liveness 里判断。
		if time.Now().Unix()-lastBeat.Load() > 90 {
			http.Error(w, "heartbeat stale", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("alive"))
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		sqlDB, err := db.DB()
		if err != nil {
			http.Error(w, "db handle error", http.StatusServiceUnavailable)
			return
		}
		// 与其他服务的 /ready 同口径：2s 超时，防网络黑洞把探针挂死
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if pingErr := sqlDB.PingContext(ctx); pingErr != nil {
			http.Error(w, "mysql unreachable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})

	return &http.Server{
		Addr:              ":9091",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}
