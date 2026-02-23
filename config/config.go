package config

import (
	"flag"
	"os"
	"strconv"
)

type Config struct {
	Port     int
	DataDir  string
	User     string
	Password string
}

func Parse() *Config {
	cfg := &Config{}
	flag.IntVar(&cfg.Port, "port", envInt("MULLDB_PORT", 5433), "listen port")
	flag.StringVar(&cfg.DataDir, "datadir", envStr("MULLDB_DATADIR", "./data"), "data directory")
	flag.StringVar(&cfg.User, "user", envStr("MULLDB_USER", "admin"), "auth username")
	flag.StringVar(&cfg.Password, "password", envStr("MULLDB_PASSWORD", ""), "auth password")
	flag.Parse()
	return cfg
}

func envStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}
