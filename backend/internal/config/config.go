package config

// package config handles configuration loading
// It emerges setting from Configs/config.yaml + .env into *Config for the app to use

import (
	"fmt"
	"os"
	"time"

	"github.com/goccy/go-yaml"
	"github.com/joho/godotenv"
)

// Config represents the root configuration
// Other module depend solely on this struct instead reading env directly
type Config struct {
	Server ServerConfig `yaml:"server"`
	MySQL  MySQLConfig  `yaml:"mysql"`
	JWT    JWTConfig    `yaml:"jwt"`
}

// Server Config represents the setting for HTTP server.
type ServerConfig struct {
	Port string `yaml:"port"`
}

// MySQLConfig represents the setting for Mysql connection and connection pool.
type MySQLConfig struct {
	Host         string `yaml:"host"`
	Port         string `yaml:"port"`
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	DBName       string `yaml:"db_name"`
	Charset      string `yaml:"charset"`
	MaxOpenConns int    `yaml:"max_open_conns"`
	MAXIdleConns int    `yaml:"max_idle_conns"`

	//values: X  -> X hours
	ConnMaxLifetime int `yaml:"conn_max_lifetime"`
}

// JWTConfig represents the setting for signuatre and expiration time.
type JWTConfig struct {
	Secret      string `yaml:"secret"`
	ExpireHours int    `yaml:"expires_hours"`
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

	cfg.OverrideFromEnv()

	return cfg, nil
}

// OverrideFromEnv overrides configuration from the Env variable
// we can replace it later through viper
func (c *Config) OverrideFromEnv() {
	//Mysql
	setStr(&c.MySQL.Host, "DB_HOST")
	setStr(&c.MySQL.Port, "DB_PORT")
	setStr(&c.MySQL.User, "DB_USER")
	setStr(&c.MySQL.Password, "DB_PASSWORD")
	setStr(&c.MySQL.DBName, "DB_NAME")

	// JWT
	setStr(&c.JWT.Secret, "JWT_SECRET")

	// server
	setStr(&c.Server.Port, "SERVER_PORT")
}

func setStr(target *string, key string) {
	if v := os.Getenv(key); v != "" {
		*target = v
	}
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
