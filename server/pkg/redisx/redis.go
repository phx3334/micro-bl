// Package redisx 封装 Redis 客户端连接与通用原语（键命名、Hash/ZSet 操作、验证码存储）。
//
// 拆服务后各服务共享同一份连接与键前缀约定；业务语义缓存（用户/视频/互动）
// 仍留在各服务自己的 repository 包内，避免基础设施包反向依赖业务。
package redisx

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"fake_tiktok/pkg/config"
	"fake_tiktok/pkg/otelx"
	"fake_tiktok/pkg/tlsx"
)

// RedisConn 封装 Redis 连接和键前缀配置。
//
// 字段说明:
//   - Client: go-redis 客户端实例, 由调用方持有, 生命周期与 App 一致
//   - KeyPrefix: 全局键前缀, 用于缓存命名空间隔离(如 "Dogedoge:v1"),
//     便于按业务版本批量清理缓存
type RedisConn struct {
	Client    *redis.Client // go-redis 客户端实例
	KeyPrefix string        // 全局键前缀, 如 "Dogedoge:v1"
}

// ConnectRedis 创建 Redis 连接并校验服务端持久化配置。
//
// 执行步骤:
//  1. 根据配置创建 Redis 客户端
//  2. Ping 验证连通性, 失败时返回 nil (不阻断启动, 由调用方决策)
//  3. 末尾调用 verifyPersistence 校验 AOF/RDB 状态
//
// 持久化策略说明:
//
//	本函数不再在运行时通过 CONFIG SET 强制开启 AOF, 而是把持久化配置
//	收敛到 infra 仓库的 Redis 配置, 由运维侧在容器启动时挂载应用。
//	这种做法的好处:
//	  - 配置可审计 (config 文件入 git)
//	  - 重启后配置不会因运行期动态修改而漂移
//	  - 应用进程不依赖 CONFIG 命令权限 (云托管 Redis 可能禁用)
//
//	运行时仅做"探测 + 告警": 启动时校验服务端持久化状态,
//	若 AOF/RDB 都未启用, 仅输出 Warn 日志, 不阻断启动
//	(本地开发或测试环境可能故意不挂 redis.conf)。
func ConnectRedis(redis_cfg *config.RedisConfig) (*RedisConn, error) {
	// 单向 TLS：只校验服务端（Redis 不要求客户端证书）。
	// 注意：服务端开启 TLS 后 6379/26379 只接受 TLS，这里的开关必须与服务端一致。
	var tlsCfg *tls.Config
	if redis_cfg.TLSEnabled {
		serverName := redis_cfg.TLSServerName
		if serverName == "" {
			serverName = tlsx.ServerName(redis_cfg.Host)
		}
		var err error
		tlsCfg, err = tlsx.ClientConfig(redis_cfg.TLSCAFile, serverName)
		if err != nil {
			return nil, fmt.Errorf("redis tls config: %w", err)
		}
	}

	// 有哨兵地址走 FailoverClient（主从自动切换），否则直连（本地开发）。
	// 哨兵与数据节点共用同一套 TLS 配置和密码：
	//   - SentinelPassword 显式复用 Password，避免 chart 开启 auth.sentinel 后认证失败；
	//   - TLS ServerName 固定为服务名（证书 SAN），主节点漂移后仍然验签通过。
	sentinels := parseSentinelAddrs(redis_cfg.SentinelAddrs)
	var client *redis.Client
	if len(sentinels) > 0 {
		masterName := redis_cfg.MasterName
		if masterName == "" {
			masterName = "mymaster"
		}
		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:       masterName,
			SentinelAddrs:    sentinels,
			Password:         redis_cfg.Password,
			SentinelPassword: redis_cfg.Password,
			DB:               redis_cfg.DB,
			TLSConfig:        tlsCfg,
		})
	} else {
		client = redis.NewClient(&redis.Options{
			Addr:      fmt.Sprintf("%s:%d", redis_cfg.Host, redis_cfg.Port),
			Password:  redis_cfg.Password,
			DB:        redis_cfg.DB,
			TLSConfig: tlsCfg,
		})
	}

	// 先做 Ping 确认网络可达; 失败时返回错误，避免后续使用 nil *RedisConn 导致 panic。
	// 必须带超时：Redis 地址不可达时若用 Background，TCP 黑洞会让启动永久挂住。
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	_, err := client.Ping(pingCtx).Result()
	if err != nil {
		zap.L().Error("连接Redis失败", zap.Error(err))
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	// 启动后探测持久化配置, 失败仅 Warn 不阻断（同样带超时）
	verifyPersistence(pingCtx, client)

	// Redis 自动埋点：go-redis hook 在每条命令前创建子 span。
	// OTel 关闭时不挂载，连接行为与原实现完全一致（零开销）。
	if otelx.Enabled() {
		if err := redisotel.InstrumentTracing(client); err != nil {
			zap.L().Warn("Redis OpenTelemetry 埋点初始化失败", zap.Error(err))
		}
	}

	return &RedisConn{Client: client, KeyPrefix: redis_cfg.KeyPrefix}, nil
}

// parseSentinelAddrs 把逗号分隔的哨兵地址拆成切片（去空白、忽略空项）。
// 返回空切片表示未配置哨兵，调用方走直连模式。
func parseSentinelAddrs(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	addrs := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			addrs = append(addrs, p)
		}
	}
	return addrs
}

// verifyPersistence 探测 Redis 服务端的持久化策略并输出诊断日志。
//
// 实现细节:
//   - 通过 CONFIG GET appendonly 判断 AOF 是否启用
//   - 通过 CONFIG GET save 判断 RDB 快照策略是否非空
//   - 任何 RPC 错误仅 Warn (不影响启动)
//   - 业务规则: AOF off + save 为空 → 双重裸奔, 必须 Warn 提示运维挂载 redis.conf
//
// 并发安全: 纯只读探测, 不修改服务端状态, 多次并发调用安全。
// 性能: 两次 RTT (约亚毫秒级), 启动期执行一次, 无热路径开销。
//
// 注意: 此函数固定返回 nil (即不返回 error),
// 失败情形已通过 zap.L().Warn 暴露, 调用方无需额外处理。
func verifyPersistence(ctx context.Context, client *redis.Client) error {
	// 1. 探测 AOF 开关
	aofRes, err := client.ConfigGet(ctx, "appendonly").Result()
	if err != nil {
		// 探测失败 (例如云 Redis 禁用 CONFIG 命令) → 仅 Warn, 不阻断
		zap.L().Warn("Redis 持久化探测失败 (appendonly)",
			zap.Error(err),
			zap.String("hint", "若使用云托管 Redis, 可能未开放 CONFIG 命令, 请在控制台侧确认持久化策略"))
		return nil
	}

	// 2. 探测 RDB save 策略
	saveRes, err := client.ConfigGet(ctx, "save").Result()
	if err != nil {
		// 同上, 探测失败仅 Warn
		zap.L().Warn("Redis 持久化探测失败 (save)",
			zap.Error(err),
			zap.String("hint", "请在 redis.conf 中显式声明 save 策略, 并以 ConfigMap 挂载"))
		return nil
	}

	// 3. 解析返回值: ConfigGet 返回 map[string]string
	//    - "appendonly" → "yes" / "no"
	//    - "save"       → 空格分隔的多档策略, 至少 2 个数字 (秒 + 写次数)
	aofEnabled := aofRes["appendonly"] == "yes"
	saveCfg := saveRes["save"]
	// 判定 RDB 策略"空"的两种情形:
	//   (1) 配置项不存在或为空字符串
	//   (2) 显式禁用 save ""  → ConfigGet 返回值通常为 ""
	// 任一情形都视为未配置 RDB 快照
	saveEmpty := saveCfg == ""

	// 4. 业务规则: AOF 与 RDB 都不存在 → 数据零持久化, 强 Warn
	if !aofEnabled && saveEmpty {
		zap.L().Warn("Redis 未启用任何持久化策略",
			zap.String("appendonly", aofRes["appendonly"]),
			zap.String("save", saveCfg),
			zap.String("risk", "重启或崩溃将丢失全部数据, 务必在 infra 仓库的 Redis 配置中启用 AOF/RDB"),
		)
		return nil
	}

	// 5. 部分启用: 仍按配置允许启动, 但明确告知运维当前持久化层级
	zap.L().Info("Redis 持久化策略已校验",
		zap.String("appendonly", aofRes["appendonly"]),
		zap.String("save", saveCfg),
	)
	return nil
}
