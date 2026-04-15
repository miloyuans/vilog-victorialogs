package httpserver

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"vilog-victorialogs/internal/config"
	"vilog-victorialogs/internal/middleware"
	"vilog-victorialogs/internal/scheduler"
	discoverysvc "vilog-victorialogs/internal/service/discovery"
	datasourcesvc "vilog-victorialogs/internal/service/datasource"
	querysvc "vilog-victorialogs/internal/service/query"
	retentionsvc "vilog-victorialogs/internal/service/retention"
	"vilog-victorialogs/internal/util"
)

type BuildInfo struct {
	Version string
	Commit  string
	Date    string
}

type ReadinessChecker interface {
	Name() string
	Ping(ctx context.Context) error
}

type Dependencies struct {
	Datasources *datasourcesvc.Service
	Discovery   *discoverysvc.Service
	Query       *querysvc.Service
	Retention   *retentionsvc.Service
	Scheduler   *scheduler.Manager
}

type Server struct {
	cfg       config.Config
	engine    *gin.Engine
	server    *http.Server
	logger    *zap.Logger
	checkers  []ReadinessChecker
	buildInfo BuildInfo
	deps      Dependencies
}

func New(cfg config.Config, logger *zap.Logger, checkers []ReadinessChecker, buildInfo BuildInfo, deps Dependencies) (*Server, error) {
	gin.SetMode(ginMode(cfg.App.Environment, cfg.Logging.Development))

	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(middleware.RequestID())
	engine.Use(middleware.ZapLogger(logger))

	matcher, err := util.ParseIPMatcher(cfg.Security.Whitelist)
	if err != nil {
		return nil, fmt.Errorf("parse security whitelist: %w", err)
	}

	engine.Use(middleware.IPWhitelist(matcher, cfg.Security.TrustProxyHeaders))
	engine.Use(middleware.RateLimit(cfg.Security.RateLimitRPS, cfg.Security.RateLimitBurst, cfg.Security.TrustProxyHeaders))

	if err := engine.SetTrustedProxies(cfg.HTTP.TrustedProxies); err != nil {
		return nil, fmt.Errorf("configure trusted proxies: %w", err)
	}

	srv := &Server{
		cfg:       cfg,
		engine:    engine,
		logger:    logger,
		checkers:  checkers,
		buildInfo: buildInfo,
		deps:      deps,
		server: &http.Server{
			Addr:         cfg.HTTP.Addr,
			Handler:      engine,
			ReadTimeout:  cfg.HTTP.ReadTimeout,
			WriteTimeout: cfg.HTTP.WriteTimeout,
			IdleTimeout:  cfg.HTTP.IdleTimeout,
		},
	}

	srv.registerRoutes()

	return srv, nil
}

func (s *Server) Run() error {
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func ginMode(environment string, development bool) string {
	if development || strings.EqualFold(environment, "development") || strings.EqualFold(environment, "local") {
		return gin.DebugMode
	}
	return gin.ReleaseMode
}
