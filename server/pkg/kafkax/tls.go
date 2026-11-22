package kafkax

import (
	"crypto/tls"

	"fake_tiktok/pkg/config"
	"fake_tiktok/pkg/tlsx"
)

// tlsConfig 按配置构造「只验服务端」的 TLS 配置；未开启时返回 nil，调用方保持原行为。
//
// 多 broker 场景下 ServerName 故意留空：每个 broker 的连接地址不同，
// kafka-go 会在握手前按实际拨号的 host 回填，写死一个名字反而会让其余 broker 验不过。
func tlsConfig(cfg *config.KafkaConfig) (*tls.Config, error) {
	if cfg == nil || !cfg.TLSEnabled {
		return nil, nil
	}
	return tlsx.ClientConfig(cfg.TLSCAFile, cfg.TLSServerName)
}
