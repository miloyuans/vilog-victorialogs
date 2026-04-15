package httpserver

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"vilog-victorialogs/internal/model"
)

const readinessCheckTimeout = 2 * time.Second

func (s *Server) registerRoutes() {
	s.engine.GET("/", s.handleIndex)
	s.engine.GET("/healthz", s.handleHealthz)
	s.engine.GET("/readyz", s.handleReadyz)

	api := s.engine.Group("/api")
	s.registerDatasourceRoutes(api)
	s.registerQueryRoutes(api)
	s.registerTagRoutes(api)
	s.registerRetentionRoutes(api)
}

func (s *Server) handleHealthz(c *gin.Context) {
	c.JSON(http.StatusOK, model.HealthResponse{
		Status:  "ok",
		Time:    time.Now().UTC().Format(time.RFC3339),
		Version: s.buildInfo.Version,
	})
}

func (s *Server) handleReadyz(c *gin.Context) {
	components := make([]model.ComponentStatus, 0, len(s.checkers))
	overallStatus := "ok"
	statusCode := http.StatusOK

	for _, checker := range s.checkers {
		checkCtx, cancel := context.WithTimeout(c.Request.Context(), readinessCheckTimeout)
		err := checker.Ping(checkCtx)
		cancel()

		component := model.ComponentStatus{
			Name:   checker.Name(),
			Status: "ok",
		}
		if err != nil {
			component.Status = "error"
			component.Error = err.Error()
			overallStatus = "degraded"
			statusCode = http.StatusServiceUnavailable
		}

		components = append(components, component)
	}

	c.JSON(statusCode, model.HealthResponse{
		Status:     overallStatus,
		Time:       time.Now().UTC().Format(time.RFC3339),
		Version:    s.buildInfo.Version,
		Components: components,
	})
}
