package config

import (
	"fmt"

	"github.com/caarlos0/env/v11"
)

type (
	Config struct {
		Server Server
		PG     PG
	}

	Server struct {
		Port                string `env:"BIOCAD_SERVER_PORT,required"`
		ReadTimeoutSeconds  int    `env:"BIOCAD_SERVER_READ_TIMEOUT_SECONDS" envDefault:"15"`
		WriteTimeoutSeconds int    `env:"BIOCAD_SERVER_WRITE_TIMEOUT_SECONDS" envDefault:"15"`
		IdleTimeoutSeconds  int    `env:"BIOCAD_SERVER_IDLE_TIMEOUT_SECONDS" envDefault:"60"`
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
