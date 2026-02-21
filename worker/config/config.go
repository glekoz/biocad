package config

import (
	"fmt"

	"github.com/caarlos0/env/v11"
)

type (
	Config struct {
		Worker Worker
		PG     PG
	}

	Worker struct {
		FromPath                     string `env:"BIOCAD_DIR_PATH,required"`
		PollIntervalMs               int    `env:"BIOCAD_POLL_INTERVAL_MS" envDefault:"5000"`
		MaxWorkers                   int    `env:"BIOCAD_MAX_WORKERS" envDefault:"3"`
		BatchSize                    int    `env:"BIOCAD_BATCH_SIZE" envDefault:"1000"`
		FileProcessingTimeoutSeconds int    `env:"BIOCAD_FILE_PROCESSING_TIMEOUT_SECONDS" envDefault:"300"`
	}

	PG struct {
		User     string `env:"BIOCAD_PG_USER,required"`
		Password string `env:"BIOCAD_PG_PASSWORD,required"`
		Host     string `env:"BIOCAD_PG_HOST,required"`
		Port     int    `env:"BIOCAD_PG_PORT,required"`
		DBName   string `env:"BIOCAD_PG_DBNAME,required"`
		SSLMode  string `env:"BIOCAD_PG_SSLMODE,required"`
		PoolMax  int    `env:"BIOCAD_PG_POOL_MAX,required"`
	}
)

func NewConfig() (Config, error) {
	cfg := &Config{}
	err := env.Parse(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("config error: %w", err)
	}

	return *cfg, nil
}
