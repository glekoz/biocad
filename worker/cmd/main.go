package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/glekoz/biocad/worker/config"
	"github.com/glekoz/biocad/worker/internal/repository"
	"github.com/glekoz/biocad/worker/internal/service"
	"github.com/glekoz/biocad/worker/pkg/logger"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func main() {
	// чтобы локально работало
	godotenv.Load(`C:\Users\ppota\WebDev\Golang\biocad\.env`)
	cfg, err := config.NewConfig()
	if err != nil {
		log.Fatal(err)
	}
	logger := logger.New(os.Stdout, nil)

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", cfg.PG.User, cfg.PG.Password, cfg.PG.Host, cfg.PG.Port, cfg.PG.DBName, cfg.PG.SSLMode)

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Fatal("Failed to parse postgres config:", err)
	}
	poolCfg.MaxConns = int32(cfg.PG.PoolMax)

	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		log.Fatal("Failed to create postgres pool:", err)
	}
	defer pool.Close()

	repo := repository.New(pool)

	// Initialize service
	svc, err := service.New(cfg.Worker, logger, repo)
	if err != nil {
		log.Fatal("Failed to create service:", err)
	}

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("Received signal, shutting down", "signal", sig)
		cancel()
	}()

	// Run the service
	// if err := svc.Run(ctx); err != nil && err != context.Canceled {
	// 	log.Fatal("Service error:", err)
	// }
	svc.Run(ctx)

	logger.Info("Application stopped gracefully")
}
