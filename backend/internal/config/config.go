package config

// package config handles configuration loading
// It emerges setting from Configs/config.yaml + .env into *Config for the app to use
// Three-layer configuration sources,from lowest to highest priority:
// 1.config.yaml-defines the configuration structure and default values,
// 2.Environment vars - the primary injection method for contain
// overrides values from yaml

import (
	"fmt"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/go-playground/validator/v10"
	"github.com/goccy/go-yaml"
	"github.com/joho/godotenv"
)

// Config represents the root configuration
// Other module depend solely on this struct instead reading env directly
type Config struct {
	Server ServerConfig `yaml:"server"`
	MySQL  MySQLConfig  `yaml:"mysql"`
	Redis  RedisConfig  `yaml:"redis"`
	JWT    JWTConfig    `yaml:"jwt"`
	Kafka  KafKaConfig  `yaml:"kafka"`
}

// Server Config represents the setting for HTTP server.
type ServerConfig struct {
	Port string `yaml:"port" env:"SERVER_PORT" validate:"required"`
}

type LogConfig struct {
	Level  string `yaml:"level" env:"LOG_LEVEL" validate:"required,oneof=debug info warn error"`
	Format string `yaml:"format" env:"LOG_FORMAT" validate:"required,oneof=json console"`
}

// MySQLConfig represents the setting for Mysql connection and connection pool.
type MySQLConfig struct {
	Host         string `yaml:"host"     env:"DB_HOST"     validate:"required"`
	Port         string `yaml:"port"     env:"DB_PORT"     validate:"required"`
	User         string `yaml:"user"     env:"DB_USER"     validate:"required"`
	Password     string `yaml:"password" env:"DB_PASSWORD" validate:"required"`
	DBName       string `yaml:"db_name"  env:"DB_NAME"     validate:"required"`
	Charset      string `yaml:"charset"  env:"DB_CHARSET"  validate:"required"`
	MaxOpenConns int    `yaml:"max_open_conns" env:"DB_MAX_OPEN_CONNS" validate:"min=1"`
	MAXIdleConns int    `yaml:"max_idle_conns" env:"DB_MAX_IDLE_CONNS" validate:"min=1"`

	//values: X  -> X hours
	ConnMaxLifetime int `yaml:"conn_max_lifetime" env:"DB_CONN_MAX_LIFETIME" validate:"min=1"`

	LogLevel string `yaml:"log_level" env:"DB_LOG_LEVEL" validate:"required,oneof=silent error warn info"`
}

type RedisConfig struct {
	Host     string `yaml:"host"     env:"REDIS_HOST" validate:"required"`
	Port     string `yaml:"port"     env:"REDIS_PORT" validate:"required"`
	Password string `yaml:"password" env:"REDIS_PASSWORD"`
	DB       int    `yaml:"db"       env:"REDIS_DB"   validate:"min=0,max=15"`
}

// JWTConfig represents the setting for signuatre and expiration time.
type JWTConfig struct {
	Secret      string `yaml:"secret" env:"JWT_SECRET" validate:"required"`
	ExpireHours int    `yaml:"expires_hours" env:"JWT_EXPIRE_HOURS" validate:"min=1"`
}

type KafKaConfig struct {
	Brokers []string `yaml:"brokers"  env:"KAFKA_BROKERS"  validate:"required,min=1"`
	Topic   string   `yaml:"topic"    env:"KAFKA_TOPIC"    validate:"required"`
	GroupId string   `yaml:"group_id" env:"KAFKA_GROUP_ID" validate:"required"`
}

// Load Read setting through yaml and env
// ypath is the path to the config.yaml file.
func Load(ypath string) (*Config, error) {
	_ = godotenv.Load()

	data, err := os.ReadFile(ypath)
	if err != nil {
		return nil, fmt.Errorf("failed to read the config file %s : %w", ypath, err)
	}

	cfg := &Config{}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config.yaml %s : %w", ypath, err)

	}

	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("parse env: %w", err)
	}

	if err := validator.New().Struct(cfg); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return cfg, nil
}

func (m MySQLConfig) DSN() string {
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=%s&parseTime=true&loc=Local",
		m.User, m.Password, m.Host, m.Port, m.DBName, m.Charset,
	)
}

func (j JWTConfig) AccessTokenExpire() time.Duration {
	return time.Duration(j.ExpireHours) * time.Hour
}

func (r RedisConfig) Addr() string {
	return fmt.Sprintf("%s:%s", r.Host, r.Port)
}
