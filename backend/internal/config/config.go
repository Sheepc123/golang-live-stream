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
	Server     ServerConfig     `yaml:"server"`
	MySQL      MySQLConfig      `yaml:"mysql"`
	Redis      RedisConfig      `yaml:"redis"`
	JWT        JWTConfig        `yaml:"jwt"`
	Kafka      KafKaConfig      `yaml:"kafka"`
	Log        LogConfig        `yaml:"log"`
	Experiment ExperimentConfig `yaml:"experiment"`
}

type ExperimentConfig struct {
	// true = 弹幕绕过 Kafka,在 WS 请求路径上同步写 MySQL。
	// 实验 B「Kafka 到底有没有必要」的对照组。
	LegacySyncDBWrite bool `yaml:"legacy_sync_db_write" env:"EXP_LEGACY_SYNC_DB_WRITE"`

	// true = 点赞和进出场即时广播,不经过 Aggregator 聚合。
	// 实验 C 的对照组。
	LegacyInstantCounters bool `yaml:"legacy_instant_counters" env:"EXP_LEGACY_INSTANT_COUNTERS"`
}

// Server Config represents the setting for HTTP server.
type ServerConfig struct {
	Port      string `yaml:"port" env:"SERVER_PORT" validate:"required"`
	PprofPort string `yaml:"pprof_port" env:"PPROF_PORT"`

	// MetricsPort 是 cmd/consumer 暴露 /metrics 的端口。
	// server 进程不用它 —— server 的 /metrics 挂在 Gin 的 8080 上。
	// consumer 是独立进程,必须有自己的端口,否则两个进程抢 8080。
	MetricsPort string `yaml:"metrics_port" env:"METRICS_PORT" validate:"required"`

	// MaxWSConns 全局 WebSocket 连接上限。0 = 不限。
	//
	// 为什么默认不限:这个值必须按机器实测定,填一个拍脑袋的数
	// 比不填更危险 —— 压测时会撞到自己设的假天花板,
	// 却以为是机器到顶了(见 metrics.WSRejected 的注释)。
	// 阶段 IV 跑出真实拐点后再填。
	MaxWSConns int `yaml:"max_ws_conns" env:"MAX_WS_CONNS" validate:"min=0"`

	// MaxConnsPerUser 单用户并发连接上限。0 = 不限。
	//
	// ⚠️ 这是「单实例内」的限制。多实例部署时一个用户可以在每个实例上
	// 各开满额度,真正的全局限制要挪到 Redis(阶段 VI)。
	MaxConnsPerUser int `yaml:"max_conns_per_user" env:"MAX_CONNS_PER_USER" validate:"min=0"`
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

	AutoMigrate bool `yaml:"auto_migrate" env:"DB_AUTO_MIGRATE"`
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

	Acks string `yaml:"acks" env:"KAFKA_ACKS" validate:"required,oneof=all local none"`
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
