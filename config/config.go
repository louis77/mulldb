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
	LogLevel int
	Migrate  bool
	Fsync    bool
}

func Parse() *Config {
	cfg := &Config{}
	flag.IntVar(&cfg.Port, "port", envInt("MULLDB_PORT", 5433), "listen port")
	flag.StringVar(&cfg.DataDir, "datadir", envStr("MULLDB_DATADIR", "./data"), "data directory")
	flag.StringVar(&cfg.User, "user", envStr("MULLDB_USER", "admin"), "auth username")
	flag.StringVar(&cfg.Password, "password", envStr("MULLDB_PASSWORD", ""), "auth password")
	flag.IntVar(&cfg.LogLevel, "log-level", envInt("MULLDB_LOG_LEVEL", 0), "log verbosity (0=off, 1=SQL statements)")
	flag.BoolVar(&cfg.Migrate, "migrate", false, "migrate WAL file format if needed")
	flag.BoolVar(&cfg.Fsync, "fsync", envBool("MULLDB_FSYNC", true), "enable fsync on WAL writes (disable for speed at risk of data loss on crash)")
	flag.Parse()
	return cfg
}

func envStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
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
