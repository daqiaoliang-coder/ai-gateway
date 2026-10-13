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
	"github.com/daqiaoliang-coder/ai-gateway/internal/policy"
)

func main() {
	configPath := flag.String("config", "configs/gateway.json", "path to gateway JSON config")
	policyPath := flag.String("policy", "", "path to the behavior contract JSON; defaults to the built-in contract")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("configuration error", "error", err)
		os.Exit(2)
	}
	contract := policy.Default()
	if *policyPath != "" {
		loaded, err := policy.Load(*policyPath)
		if err != nil {
			logger.Error("behavior contract error", "error", err)
			os.Exit(2)
		}
		contract = loaded
	} else {
		logger.Info("no behavior contract supplied, using built-in defaults",
			"total_deadline", contract.Timeouts.TotalDeadline,
			"max_attempts", contract.Retry.MaxAttempts)
	}
	service, err := gateway.New(cfg, contract, logger)
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

	drainTimeout, err := time.ParseDuration(contract.Drain.Timeout)
	if err != nil {
		logger.Error("invalid drain timeout", "error", err)
		os.Exit(2)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-signals:
		// Stop accepting new traffic first, then let in-flight requests finish.
		service.Drain()
		logger.Info("shutdown requested, draining", "signal", sig.String(), "drain_timeout", drainTimeout.String())
		ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			logger.Error("graceful shutdown did not finish in time", "error", err)
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
