package httpx

import (
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"micro-bl/pkg/apperr"
)

func newTestContext(t *testing.T) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	return c, recorder
}

func TestFailWithErrBusinessStays200(t *testing.T) {
	c, recorder := newTestContext(t)
	FailWithErr(c, apperr.Business("bad input"))

	if recorder.Code != http.StatusOK {
		t.Fatalf("business error status = %d, want 200", recorder.Code)
	}
	var body Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Code != float64(ERROR) {
		t.Fatalf("business code = %v, want %d", body.Code, ERROR)
	}
}

func TestFailWithErrSystemIs500(t *testing.T) {
	c, recorder := newTestContext(t)
	FailWithErr(c, apperr.DB("query failed", driver.ErrBadConn))

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("system error status = %d, want 500", recorder.Code)
	}
	var body Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Code != "DB_CONNECT_FAIL" {
		t.Fatalf("system code = %v, want DB_CONNECT_FAIL", body.Code)
	}
}
