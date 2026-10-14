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

	"github.com/daqiaoliang-coder/ai-gateway/internal/audit"
	"github.com/daqiaoliang-coder/ai-gateway/internal/config"
	"github.com/daqiaoliang-coder/ai-gateway/internal/cost"
	"github.com/daqiaoliang-coder/ai-gateway/internal/gateway"
	"github.com/daqiaoliang-coder/ai-gateway/internal/policy"
	"github.com/daqiaoliang-coder/ai-gateway/internal/quota"
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

	prices, err := cost.NewTable(cfg.Prices)
	if err != nil {
		logger.Error("price table error", "error", err)
		os.Exit(2)
	}

	// Quota state is shared across replicas, so multi-replica deployments obey
	// one tenant budget instead of one per pod. When it cannot be reached, the
	// contract decides whether to degrade, fail open or fail closed.
	quotaMgr, err := quota.NewManagerFromContract(contract, nil)
	if err != nil {
		logger.Error("quota store unavailable at startup",
			"mode", contract.Quota.Store.Mode,
			"addr", contract.Quota.Store.Redis.Addr,
			"error", err,
			"hint", "start Redis, or set quota.store.mode to local for single instance deployments")
		os.Exit(2)
	}
	defer quotaMgr.Close()

	sweepCtx, stopSweeper := context.WithCancel(context.Background())
	defer stopSweeper()
	sweepInterval, _ := time.ParseDuration(contract.Quota.Store.SweepInterval)
	quotaMgr.StartSweeper(sweepCtx, sweepInterval, func(released int) {
		logger.Info("reclaimed expired reservations", "count", released)
	})

	recorder, err := newRecorder(contract)
	if err != nil {
		logger.Error("audit sink error", "error", err)
		os.Exit(2)
	}
	if recorder != nil {
		recorder.Start()
		defer func() {
			if err := recorder.Stop(); err != nil {
				logger.Error("audit sink close failed", "error", err)
			}
			logger.Info("audit events written", "written", recorder.Written(), "dropped", recorder.Dropped(), "failed", recorder.Failed())
		}()
	}

	service, err := gateway.New(cfg, gateway.Options{
		Contract: contract,
		Logger:   logger,
		Prices:   prices,
		Quota:    quotaMgr,
		Recorder: recorder,
	})
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

// newRecorder builds the audit pipeline the contract asks for. The recorder is
// asynchronous, so a slow or full sink never blocks a request.
func newRecorder(contract *policy.Contract) (*audit.Recorder, error) {
	if !contract.Usage.EmitAuditEvents {
		return nil, nil
	}
	var sinks []audit.Sink
	switch contract.Usage.AuditSink {
	case "stdout", "both":
		sinks = append(sinks, audit.NewStdoutSink(os.Stdout))
	}
	if contract.Usage.AuditSink == "durable" || contract.Usage.AuditSink == "both" {
		fileSink, err := audit.NewFileSink(contract.Usage.AuditFilePath)
		if err != nil {
			return nil, err
		}
		sinks = append(sinks, fileSink)
	}
	if len(sinks) == 0 {
		return nil, nil
	}
	return audit.NewRecorder(sinks, 4096), nil
}
