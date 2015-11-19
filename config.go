package main

import (
	"path/filepath"
	"gopkg.in/ini.v1"
	"fmt"
	"strings"
	"os"
)

type Config struct {
	SqlServer string
	Database string
	User string
	Password string
}

func LoadConfiguration() ([]*Config, error) {
	cfg, err := ini.Load(filepath.Join(filepath.Dir(os.Args[0]), "_config.ini"))
	if err != nil {
		return nil, err
	}
	cfg.BlockMode = false
	configs := make([]*Config, 0, 10)
	for _, section := range cfg.Sections() {
		if strings.HasPrefix(section.Name(), "Database") {
			config := &Config{}
			config.SqlServer = strings.TrimSpace(section.Key("Server").String())
			config.Database = strings.TrimSpace(section.Key("Database").String())
			config.User = strings.TrimSpace(section.Key("User").String())
			config.Password = strings.TrimSpace(section.Key("Password").String())
			configs = append(configs, config)
		}
	}

	return configs, nil
}

func (cfg *Config) GetConnectionString() string {
	return getConnectionString(cfg.SqlServer, cfg.Database, cfg.User, cfg.Password)
}

func getConnectionString(server, database, user, password string) string {
	connStr := fmt.Sprintf("server=%s;database=%s", server, database)
	if user != "" {
		connStr += fmt.Sprintf(";user id=%s", user)
	}
	if password != "" {
		connStr += fmt.Sprintf(";password=%s", password)
	}
	return connStr
}