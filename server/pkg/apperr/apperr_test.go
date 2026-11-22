package apperr

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBusinessAndSystemClassification(t *testing.T) {
	business := Business("bad input")
	if IsSystem(business) {
		t.Fatal("business error classified as system")
	}
	if code := CodeOf(business); code != "" {
		t.Fatalf("business error code = %q, want empty", code)
	}

	system := DB("query failed", driver.ErrBadConn)
	if !IsSystem(system) {
		t.Fatal("DB error classified as business")
	}
	if code := CodeOf(system); code != "DB_CONNECT_FAIL" {
		t.Fatalf("DB code = %q, want DB_CONNECT_FAIL", code)
	}
	if !strings.Contains(system.Error(), "query failed") {
		t.Fatalf("system error message lost: %q", system.Error())
	}

	// 裸 error（仓储层直接透传）必须按系统错误处理，避免被伪装成 200 + 业务码
	bare := errors.New("unexpected")
	if !IsSystem(bare) {
		t.Fatal("bare error must be treated as system error")
	}
	if code := CodeOf(bare); code != "INTERNAL_ERROR" {
		t.Fatalf("bare error code = %q, want INTERNAL_ERROR", code)
	}
}

func TestSystemSubcodes(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{name: "db timeout", err: DB("db", context.DeadlineExceeded), want: "DB_TIMEOUT"},
		{name: "db bad conn", err: DB("db", driver.ErrBadConn), want: "DB_CONNECT_FAIL"},
		{name: "db other", err: DB("db", errors.New("syntax")), want: "DB_FAIL"},
		{name: "redis timeout", err: Redis("redis", context.DeadlineExceeded), want: "REDIS_TIMEOUT"},
		{name: "storage timeout", err: Storage("minio", context.DeadlineExceeded), want: "MINIO_TIMEOUT"},
		{name: "grpc unavailable", err: RPC("rpc", status.Error(codes.Unavailable, "down")), want: "GRPC_UNAVAILABLE"},
		{name: "grpc timeout", err: RPC("rpc", status.Error(codes.DeadlineExceeded, "slow")), want: "GRPC_TIMEOUT"},
		{name: "internal", err: Internal("bug", errors.New("panic")), want: "INTERNAL_ERROR"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CodeOf(tc.err); got != tc.want {
				t.Fatalf("CodeOf() = %q, want %q", got, tc.want)
			}
		})
	}
}
