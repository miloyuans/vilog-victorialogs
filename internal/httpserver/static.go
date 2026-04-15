package httpserver

import (
	_ "embed"
	"net/http"

	"github.com/gin-gonic/gin"
)

//go:embed static/index.html
var indexHTML []byte

func (s *Server) handleIndex(c *gin.Context) {
	c.Data(http.StatusOK, "text/html; charset=utf-8", indexHTML)
}
