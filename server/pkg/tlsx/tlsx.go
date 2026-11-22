// Package tlsx 统一内部中间件「单向 TLS」的客户端配置。
//
// 单向指：只校验服务端证书（RootCAs + ServerName），客户端不出示证书。
// 服务端证书由 cert-manager 的 fake-tiktok-ca-issuer 签发，根证书以文件形式
// 挂进容器（约定路径 /certs/internal-ca.crt，可通过配置覆盖）。
package tlsx

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strings"
)

// LoadCAPool 读取 PEM 根证书，返回验链用的证书池。
func LoadCAPool(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("tlsx: read ca file %q: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("tlsx: no valid certificate in %q", caFile)
	}
	return pool, nil
}

// ClientConfig 构造「只验服务端」的 TLS 配置。
//   - caFile 为空时回退系统根证书（将来换公网 CA 时不用改代码）
//   - serverName 必须与证书 SAN 一致；为空时由底层库按连接地址回填
//     （MySQL/Redis/MinIO 会回填，Kafka 每个 broker 地址不同，刻意留空）
func ClientConfig(caFile, serverName string) (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: serverName,
	}
	if caFile == "" {
		return cfg, nil
	}
	pool, err := LoadCAPool(caFile)
	if err != nil {
		return nil, err
	}
	cfg.RootCAs = pool
	return cfg, nil
}

// ServerName 去掉 host:port 里的端口，得到 TLS 校验用的主机名。
// 证书 SAN 写的是主机名或 IP，带端口比较必然失败。
func ServerName(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}
	return strings.TrimSpace(host)
}
