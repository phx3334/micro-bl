// Package gormx 封装 GORM(MySQL) 连接池初始化。
//
// 连接池超时等参数属于"基础设施约定"，拆服务后各服务都复用同一份实现，
// 避免各自抄一遍导致参数漂移。
package gormx

import (
	"fmt"
	"os"
	"time"

	mysqldrv "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/plugin/opentelemetry/tracing"

	"fake_tiktok/pkg/config"
	"fake_tiktok/pkg/tlsx"
)

func InitGorm(cfg *config.Config) *gorm.DB {
	dbCfg := cfg.Database
	// 用官方 driver 的 Config.FormatDSN() 而不是 fmt.Sprintf + url.QueryEscape：
	// MySQL DSN 不是 URL，driver 不做百分号解码。QueryEscape 会把密码里的
	// @ / 空格 / % 改成 %40 / + / %25，等于拿"转义后的错误密码"去认证，
	// 含特殊字符的密码必然登录失败。
	dsnCfg := mysqldrv.NewConfig()
	dsnCfg.User = dbCfg.User
	dsnCfg.Passwd = dbCfg.Password
	dsnCfg.Net = "tcp"
	dsnCfg.Addr = fmt.Sprintf("%s:%d", dbCfg.Host, dbCfg.Port)
	dsnCfg.DBName = dbCfg.DBName
	dsnCfg.ParseTime = true
	dsnCfg.Loc = time.Local
	dsnCfg.Params = map[string]string{"charset": "utf8mb4"}

	// 单向 TLS：只校验服务端证书，客户端不出示证书。
	// go-sql-driver 要求先把 tls.Config 注册成一个名字，再在 DSN 里用 tls=<name> 引用。
	if dbCfg.TLSEnabled {
		serverName := dbCfg.TLSServerName
		if serverName == "" {
			serverName = tlsx.ServerName(dbCfg.Host)
		}
		tlsCfg, err := tlsx.ClientConfig(dbCfg.TLSCAFile, serverName)
		if err != nil {
			zap.L().Error("构造 MySQL TLS 配置失败", zap.Error(err))
			os.Exit(1)
		}
		const tlsName = "fake-tiktok-internal"
		if err := mysqldrv.RegisterTLSConfig(tlsName, tlsCfg); err != nil {
			zap.L().Error("注册 MySQL TLS 配置失败", zap.Error(err))
			os.Exit(1)
		}
		dsnCfg.TLSConfig = tlsName
	}

	db, err := gorm.Open(gormmysql.Open(dsnCfg.FormatDSN()), &gorm.Config{})
	if err != nil {
		zap.L().Error("连接数据库失败", zap.Error(err))
		os.Exit(1)
	}

	sqlDB, err := db.DB()
	if err != nil {
		zap.L().Error("获取数据库连接失败", zap.Error(err))
		os.Exit(1)
	}
	sqlDB.SetMaxIdleConns(dbCfg.MaxIdleConns)
	sqlDB.SetMaxOpenConns(dbCfg.MaxOpenConns)

	// ---- 连接池超时控制（行业标准） ----
	// ConnMaxLifetime：连接最大存活时间，超过后连接会被关闭并从池中移除。
	//   - MySQL 服务端默认 wait_timeout=28800s(8h)，连接池中超过该时间的连接
	//     已被服务端静默关闭，客户端使用时会报 "driver: bad connection"。
	//   - 行业标准建议设为 5~30 分钟，远小于 wait_timeout，确保连接在被服务端
	//     关闭之前就被池主动回收重建。
	//   - 配置值为 0 时使用默认值 10 分钟（兼顾安全性和连接复用效率）。
	connMaxLifetime := 10 * time.Minute
	if dbCfg.ConnMaxLifetime > 0 {
		connMaxLifetime = time.Duration(dbCfg.ConnMaxLifetime) * time.Second
	}
	sqlDB.SetConnMaxLifetime(connMaxLifetime)

	// ConnMaxIdleTime：连接最大空闲时间，超过后空闲连接会被关闭。
	//   - 与 ConnMaxLifetime 的区别：ConnMaxLifetime 是连接从创建到销毁的总寿命，
	//     而 ConnMaxIdleTime 只计算连接处于空闲状态的时间。
	//   - 行业标准建议设为 ConnMaxLifetime 的 1/2 ~ 1/3，在保持连接复用效率的同时
	//     避免空闲连接长期占用数据库服务端的连接槽位。
	//   - 配置值为 0 时使用默认值 5 分钟。
	connMaxIdleTime := 5 * time.Minute
	if dbCfg.ConnMaxIdleTime > 0 {
		connMaxIdleTime = time.Duration(dbCfg.ConnMaxIdleTime) * time.Second
	}
	sqlDB.SetConnMaxIdleTime(connMaxIdleTime)

	// MySQL 自动埋点：每条 SQL 生成子 span，在 Tempo 中可直接看到慢查询落在哪一步。
	// 用 WithoutMetrics 只保留 trace：DB 指标交给业务 RED 指标 + Prometheus 观察，
	// 避免每张表/每种语句都生成一套指标，撑爆 registry 与抓取体积。
	// 挂载失败只告警：观测能力缺失不应阻断服务启动。
	if cfg.OTEL.Enabled {
		if err := db.Use(tracing.NewPlugin(tracing.WithoutMetrics())); err != nil {
			zap.L().Warn("GORM OpenTelemetry 插件初始化失败，SQL 将不产生 span", zap.Error(err))
		}
	}

	return db
}
