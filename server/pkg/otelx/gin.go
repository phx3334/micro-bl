package otelx

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// gin.Context 中保存链路信息的 key，业务/日志可直接读取。
const (
	TraceIDKey = "trace_id"
	SpanIDKey  = "span_id"
)

// GinMiddleware 是自研的 HTTP 埋点中间件（不引入 contrib/otelgin），产出 server span：
//   - 与下游 gRPC / MySQL / Redis 子 span 在 Tempo 中串成完整链路
//   - RED 指标（calls_total、duration 直方图）不在这里记：由 otel-collector 的
//     spanmetrics connector 从这些 span 派生，且必须在进尾采之前派生（见 config.yaml）
//
// 必须注册在 GinLogger 之前：日志中间件依赖本中间件写回请求 ctx 与 trace_id。
func GinMiddleware() gin.HandlerFunc {
	tracer := otel.Tracer("micro-bl/http")

	return func(c *gin.Context) {
		// 1. 续接上游链路：有 traceparent 就复用 trace，没有则新建。
		//    Extract 到 request ctx 后，下游 gRPC/DB/Redis 调用都会自动带上同一 trace_id。
		ctx := otel.GetTextMapPropagator().Extract(c.Request.Context(),
			propagation.HeaderCarrier(c.Request.Header))

		// 2. 路由模板而非真实路径：/api/v1/video/:id 这类模板能避免 span 属性基数爆炸。
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}

		ctx, span := tracer.Start(ctx, c.Request.Method+" "+route,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.method", c.Request.Method),
				attribute.String("http.route", route),
				attribute.String("http.user_agent", c.Request.UserAgent()),
				attribute.String("net.peer.ip", c.ClientIP()),
			),
		)
		defer span.End()
		// panic 穿过本中间件时（GinRecovery 注册在更外层，要等这里重抛后才恢复），
		// 下面 c.Next() 之后的状态回写不会执行——不兜底则 panic 的 span 缺
		// http.status_code、无 ERROR 状态：spanmetrics 少算 5xx（进程 SLO 告警失明）、
		// 尾采 errors 策略不保留该链路。外层 GinRecovery 必把 panic 转成 HTTP 500，
		// 据此补状态后原样重抛，响应与错误日志仍由 Recovery 统一负责。
		defer func() {
			if r := recover(); r != nil {
				span.SetAttributes(attribute.Int("http.status_code", http.StatusInternalServerError))
				span.SetStatus(codes.Error, http.StatusText(http.StatusInternalServerError))
				panic(r)
			}
		}()

		// 3. 把带 span 的 ctx 放回请求，业务取 c.Request.Context() 即自动继承链路。
		c.Request = c.Request.WithContext(ctx)
		if sc := span.SpanContext(); sc.IsValid() {
			c.Set(TraceIDKey, sc.TraceID().String())
			c.Set(SpanIDKey, sc.SpanID().String())
		}

		// 4. 回写 traceparent 响应头：前端 / CDN / 网关可据此把用户请求与后端链路关联。
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(c.Writer.Header()))

		c.Next()

		// 5. status 打到 span 上：spanmetrics 据此算错误率，尾采据此必留错误链路。
		//    5xx 额外标 span 失败（HTTP 200 的业务失败由业务代码自己 SetStatus）。
		status := c.Writer.Status()
		span.SetAttributes(attribute.Int("http.status_code", status))
		if status >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(status))
		}
	}
}
