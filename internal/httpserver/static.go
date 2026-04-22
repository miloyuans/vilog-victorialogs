package httpserver

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"

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
	versioned := injectStaticVersion(string(indexHTML), s.staticVersion)
	setNoCacheHeaders(c)
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(versioned))
}

func (s *Server) handleAsset(c *gin.Context) {
	setNoCacheHeaders(c)
	http.StripPrefix("/assets/", http.FileServer(embeddedStaticFS())).ServeHTTP(c.Writer, c.Request)
}

func setNoCacheHeaders(c *gin.Context) {
	c.Header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
	c.Header("Pragma", "no-cache")
	c.Header("Expires", "0")
}

func injectStaticVersion(indexHTML, version string) string {
	replacer := strings.NewReplacer(
		`href="/assets/styles.css"`, `href="/assets/styles.css?v=`+version+`"`,
		`href="/assets/styles.overrides.css"`, `href="/assets/styles.overrides.css?v=`+version+`"`,
		`src="/assets/app.js"`, `src="/assets/app.js?v=`+version+`"`,
		`src="/assets/app.overrides.js"`, `src="/assets/app.overrides.js?v=`+version+`"`,
		`src="/assets/app.explore.js"`, `src="/assets/app.explore.js?v=`+version+`"`,
	)
	return replacer.Replace(indexHTML)
}
