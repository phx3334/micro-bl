package httpx

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"micro-bl/pkg/apperr"
)

// Response 统一响应体。
//
// Code 类型随错误类别变化（数字就数字，字符串就字符串）：
//   - 成功 / 业务错误：数字 3 / 4，沿用前端拦截器 res.code === 3 的约定；
//   - 系统错误：细分字符串错误码，如 "DB_CONNECT_FAIL"，供排障直接定位。
type Response struct {
	Code interface{} `json:"code"`
	Data interface{} `json:"data"`
	Msg  string      `json:"msg"`
}

const (
	ERROR   = 4
	SUCCESS = 3
)

// Result 写响应，code 原样透传（数字保持数字，字符串保持字符串），HTTP 恒 200。
func Result(code interface{}, data interface{}, msg string, c *gin.Context) {
	c.JSON(http.StatusOK, Response{
		Code: code,
		Data: data,
		Msg:  msg,
	})
}

func OkWithMsg(c *gin.Context, msg string) {
	Result(SUCCESS, nil, msg, c)
}

func OkWithData(c *gin.Context, data interface{}) {
	Result(SUCCESS, data, "success", c)
}

// OkWithDetail 返回成功响应，包含自定义数据和消息。
// 命名与 OkWithData / OkWithMsg 保持一致（With 首字母大写）。
func OkWithDetail(c *gin.Context, data interface{}, msg string) {
	Result(SUCCESS, data, msg, c)
}

// FailWithMsg 业务失败：HTTP 200 + code=4。
// 保留原行为——前端统一走 res.code 分支，无需按状态码分叉。
func FailWithMsg(c *gin.Context, msg string) {
	Result(ERROR, nil, msg, c)
}

// FailWithErr 按错误类别写失败响应：
//   - 业务错误 → HTTP 200 + {"code":4,"msg":<业务原因>}
//   - 系统错误 → HTTP 500 + {"code":"DB_CONNECT_FAIL","msg":<含底层原因>}
//
// 系统错误必须走 5xx：spanmetrics 才能把它算进 5xx 错误率，尾采 error 策略
// 才会保留这条链路，Envoy 的重试 / 异常点驱逐才有依据。
func FailWithErr(c *gin.Context, err error) {
	if apperr.IsSystem(err) {
		c.JSON(http.StatusInternalServerError, Response{
			Code: apperr.CodeOf(err),
			Data: nil,
			Msg:  err.Error(),
		})
		return
	}
	Result(ERROR, nil, err.Error(), c)
}

// NoAuth 鉴权失败：HTTP 200 + code=4 + data.reload=true（前端据此跳登录页）。
func NoAuth(message string, c *gin.Context) {
	Result(ERROR, gin.H{"reload": true}, message, c)
}
