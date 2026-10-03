package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/daqiaoliang-coder/ai-gateway/internal/config"
	"github.com/daqiaoliang-coder/ai-gateway/internal/gateway"
)

func main() {
	configPath := flag.String("config", "configs/gateway.json", "path to gateway JSON config")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("configuration error", "error", err)
		os.Exit(2)
	}
	service, err := gateway.New(cfg, logger)
	if err != nil {
		logger.Error("gateway initialization error", "error", err)
		os.Exit(2)
	}

	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           service.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
		// WriteTimeout stays unset so long-lived SSE responses are not cut off.
	}

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("AI gateway listening", "addr", cfg.ListenAddr)
		serveErr <- server.ListenAndServe()
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-signals:
		logger.Info("shutdown requested", "signal", sig.String())
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			logger.Error("graceful shutdown failed", "error", err)
			_ = server.Close()
			os.Exit(1)
		}
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server stopped", "error", err)
			os.Exit(1)
		}
	}
}
