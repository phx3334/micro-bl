// Package apperr 对错误做"业务 / 系统"分类，供 HTTP 层选择状态码与细分错误码。
//
// 分类约定：
//   - 业务错误（Business）：用户输入、权限、资源不存在等，责任在调用方，
//     返回 HTTP 200 + body code=4——沿用本项目前端统一拦截器的约定（数字）。
//   - 系统错误（System）：MySQL / Redis / 对象存储 / 跨服务 gRPC 等基础设施故障，
//     返回 HTTP 500 + body code=<细分字符串错误码>，msg 保留底层原因供排障。
//
// 为什么默认按系统错误处理：从仓储层直接透传的 gorm/redis 错误本质就是基础设施
// 故障，走 5xx 才能被 spanmetrics 计入 5xx 错误率、被尾采 error 策略保留链路、
// 被 Envoy 的重试与异常点驱逐看见。业务错误必须显式用 Business 标注，漏标会
// 立刻表现为 500（响亮且可发现），比悄悄返回 200 的"监控失明"安全得多。
package apperr

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Kind 错误类别。
type Kind int

const (
	// KindBusiness 业务错误：客户端改参数 / 换资源即可恢复。
	KindBusiness Kind = iota
	// KindSystem 系统错误：服务端或其依赖不可用，重试无用。
	KindSystem
)

// Error 同时携带分类与细分码的错误实现。
type Error struct {
	kind  Kind
	code  string // 细分错误码，仅系统错误使用
	msg   string // 对外文案
	cause error  // 原始错误，进入错误链供 errors.Is/As 穿透
}

// Error 输出对外文案。系统错误拼上原始原因（demo 环境便于直接排障；
// 生产规范是只写日志、对外给通用文案）。
func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %v", e.msg, e.cause)
	}
	return e.msg
}

// Unwrap 保留错误链，让 errors.Is/As 能穿透到 gorm、grpc、net 等底层错误。
func (e *Error) Unwrap() error { return e.cause }

// -----------------------------------------------------------------------------
// 构造
// -----------------------------------------------------------------------------

// Business 构造业务错误（HTTP 200 + code=4）。
func Business(msg string) error {
	return &Error{kind: KindBusiness, msg: msg}
}

// Businessf 同 Business，支持格式化文案。
func Businessf(format string, args ...any) error {
	return &Error{kind: KindBusiness, msg: fmt.Sprintf(format, args...)}
}

// System 构造系统错误（HTTP 500 + code=<code>）。
// cause 会进入错误链并出现在对外 msg 中，便于定位。
func System(code, msg string, cause error) error {
	return &Error{kind: KindSystem, code: code, msg: msg, cause: cause}
}

// Internal 归不进具体基础设施的系统错误（业务代码自身的缺陷）。
func Internal(msg string, cause error) error {
	return System("INTERNAL_ERROR", msg, cause)
}

// DB 数据库错误，按底层原因细分错误码。
func DB(msg string, err error) error {
	return System(dbCode(err), msg, err)
}

// Redis 缓存 / 会话存储错误。
func Redis(msg string, err error) error {
	return System(redisCode(err), msg, err)
}

// Storage 对象存储（MinIO）错误。
func Storage(msg string, err error) error {
	return System(storageCode(err), msg, err)
}

// RPC 跨服务 gRPC 调用错误（下游不可用，不是"资源不存在"）。
func RPC(msg string, err error) error {
	return System(rpcCode(err), msg, err)
}

// -----------------------------------------------------------------------------
// 细分错误码判定
// -----------------------------------------------------------------------------

// dbCode 无法可靠区分"查询还是写入"，但能区分"连不上 / 超时 / 其它"。
func dbCode(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "DB_TIMEOUT"
	}
	if errors.Is(err, driver.ErrBadConn) || isDialError(err) {
		return "DB_CONNECT_FAIL"
	}
	return "DB_FAIL"
}

func redisCode(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "REDIS_TIMEOUT"
	}
	if isDialError(err) {
		return "REDIS_CONNECT_FAIL"
	}
	return "REDIS_FAIL"
}

func storageCode(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "MINIO_TIMEOUT"
	}
	if isDialError(err) {
		return "MINIO_CONNECT_FAIL"
	}
	return "MINIO_FAIL"
}

func rpcCode(err error) string {
	switch status.Code(err) {
	case codes.Unavailable:
		return "GRPC_UNAVAILABLE"
	case codes.DeadlineExceeded:
		return "GRPC_TIMEOUT"
	case codes.ResourceExhausted:
		return "GRPC_RESOURCE_EXHAUSTED"
	default:
		return "GRPC_INTERNAL"
	}
}

// isDialError 判断是否为连接类错误（dial tcp ...: connect: connection refused）。
func isDialError(err error) bool {
	var op *net.OpError
	if errors.As(err, &op) {
		return true
	}
	var se *os.SyscallError
	return errors.As(err, &se)
}

// -----------------------------------------------------------------------------
// 查询
// -----------------------------------------------------------------------------

// IsSystem 判断是否系统错误。
// 未显式分类的裸 error（仓储层直接透传）一律按系统错误处理。
func IsSystem(err error) bool {
	if err == nil {
		return false
	}
	var e *Error
	if errors.As(err, &e) {
		return e.kind == KindSystem
	}
	return true
}

// CodeOf 返回系统错误的细分码；业务错误返回空串。
// 系统错误若未指定细分码，回退为 INTERNAL_ERROR。
func CodeOf(err error) string {
	var e *Error
	if errors.As(err, &e) && e.kind == KindSystem && e.code != "" {
		return e.code
	}
	if IsSystem(err) {
		return "INTERNAL_ERROR"
	}
	return ""
}
