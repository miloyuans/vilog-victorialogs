package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"vilog-victorialogs/internal/client/victorialogs"
	"vilog-victorialogs/internal/config"
	"vilog-victorialogs/internal/httpserver"
	"vilog-victorialogs/internal/scheduler"
	"vilog-victorialogs/internal/service/cache"
	"vilog-victorialogs/internal/service/datasource"
	"vilog-victorialogs/internal/service/discovery"
	"vilog-victorialogs/internal/service/query"
	"vilog-victorialogs/internal/service/retention"
	"vilog-victorialogs/internal/service/telegram"
	mongostore "vilog-victorialogs/internal/store/mongo"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config.yaml", "path to configuration file")
	flag.Parse()

	cfg, resolvedConfigPath, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	logger, err := newLogger(cfg.Logging)
	if err != nil {
		log.Fatalf("create logger: %v", err)
	}
	defer func() {
		_ = logger.Sync()
	}()

	logger.Info("configuration loaded", zap.String("path", resolvedConfigPath))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startupCtx, cancel := context.WithTimeout(ctx, cfg.Mongo.ConnectTimeout+cfg.Mongo.PingTimeout)
	defer cancel()

	store, err := mongostore.New(startupCtx, cfg.Mongo)
	if err != nil {
		logger.Fatal("initialize mongo store", zap.Error(err))
	}
	if err := store.InitIndexes(startupCtx); err != nil {
		logger.Fatal("initialize mongo indexes", zap.Error(err))
	}

	victoriaClient := victorialogs.New(cfg.VictoriaLogs)
	cacheService := cache.New(store, cfg.Cache, logger.Named("cache"))
	telegramService := telegram.New(cfg.Telegram)
	datasourceService := datasource.New(store, victoriaClient)
	discoveryService := discovery.New(store, cacheService, datasourceService, victoriaClient, telegramService, cfg.Discovery, cfg.VictoriaLogs)
	queryService := query.New(store, cacheService, victoriaClient, cfg.Cache, logger.Named("query"))
	retentionService := retention.New(store, victoriaClient, cfg.Retention)
	schedulerManager := scheduler.New(store, retentionService)

	if cfg.Retention.SchedulerEnabled {
		if err := schedulerManager.Start(ctx); err != nil {
			logger.Error("start retention scheduler", zap.Error(err))
		}
	}

	if cfg.Discovery.StartupEnabled {
		go discoveryService.RunStartupDiscovery(context.Background())
	}
	go queryService.StartHotSync(ctx)

	server, err := httpserver.New(cfg, logger, []httpserver.ReadinessChecker{store}, httpserver.BuildInfo{
		Version: version,
		Commit:  commit,
		Date:    date,
	}, httpserver.Dependencies{
		Datasources: datasourceService,
		Discovery:   discoveryService,
		Query:       queryService,
		Retention:   retentionService,
		Scheduler:   schedulerManager,
	})
	if err != nil {
		logger.Fatal("initialize http server", zap.Error(err))
	}

	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- server.Run()
	}()

	logger.Info("service started", zap.String("addr", cfg.HTTP.Addr), zap.String("version", version))

	select {
	case err := <-serverErrCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("http server exited unexpectedly", zap.Error(err))
		}
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown http server", zap.Error(err))
	}

	schedulerStopCtx := schedulerManager.Stop()
	<-schedulerStopCtx.Done()

	if err := store.Close(shutdownCtx); err != nil {
		logger.Error("disconnect mongo", zap.Error(err))
	}

	logger.Info("service stopped")
}

func newLogger(cfg config.LoggingConfig) (*zap.Logger, error) {
	zapConfig := zap.NewProductionConfig()
	if cfg.Development {
		zapConfig = zap.NewDevelopmentConfig()
	}

	level := zap.NewAtomicLevel()
	if err := level.UnmarshalText([]byte(cfg.Level)); err != nil {
		return nil, err
	}
	zapConfig.Level = level

	return zapConfig.Build()
}
