package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/glekoz/biocad/worker/config"
	"github.com/glekoz/biocad/worker/internal/repository"
	"github.com/glekoz/biocad/worker/internal/service"
	"github.com/glekoz/biocad/worker/pkg/logger"
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
	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := repository.NewPool(ctx, cfg)
	if err != nil {
		logger.Error("Подключение к БД", "error", err)
		return
	}
	defer pool.Close()

	repo := repository.New(ctx, pool)

	// Initialize service
	svc, err := service.New(cfg.Worker, logger, repo)
	if err != nil {
		logger.Error("Создание сервиса", "error", err)
		return
	}

	// Handle OS signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("Received signal, shutting down", "signal", sig)
		cancel()
	}()

	svc.Run(ctx)

	logger.Info("Application stopped gracefully")
}
