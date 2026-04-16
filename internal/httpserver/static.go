package httpserver

import (
	"embed"
	"io/fs"
	"net/http"

	"github.com/gin-gonic/gin"
)

//go:embed static/*
var staticFiles embed.FS

func embeddedStaticFS() http.FileSystem {
	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		panic(err)
	}
	return http.FS(sub)
}

func (s *Server) handleIndex(c *gin.Context) {
	indexHTML, err := fs.ReadFile(staticFiles, "static/index.html")
	if err != nil {
		c.String(http.StatusInternalServerError, "embedded index not found")
		return
	}
	c.Data(http.StatusOK, "text/html; charset=utf-8", indexHTML)
}
