package main

import (
	"path/filepath"
	"gopkg.in/ini.v1"
	"fmt"
	"strings"
	"os"
)

type Config struct {
	SqlServer1 string
	Database1 string
	User1 string
	Password1 string

	SqlServer2 string
	Database2 string
	User2 string
	Password2 string
}

func LoadConfiguration() (*Config, error) {
	cfg, err := ini.Load(filepath.Join(filepath.Dir(os.Args[0]), "_config.ini"))
	if err != nil {
		return nil, err
	}
	cfg.BlockMode = false
	config := &Config{}

	databaseSection1 := cfg.Section("Database1")
	config.SqlServer1 = strings.TrimSpace(databaseSection1.Key("Server").String())
	config.Database1 = strings.TrimSpace(databaseSection1.Key("Database").String())
	config.User1 = strings.TrimSpace(databaseSection1.Key("User").String())
	config.Password1 = strings.TrimSpace(databaseSection1.Key("Password").String())

	databaseSection2 := cfg.Section("Database2")
	config.SqlServer2 = strings.TrimSpace(databaseSection2.Key("Server").String())
	config.Database2 = strings.TrimSpace(databaseSection2.Key("Database").String())
	config.User2 = strings.TrimSpace(databaseSection2.Key("User").String())
	config.Password2 = strings.TrimSpace(databaseSection2.Key("Password").String())

	return config, nil
}

func (cfg *Config) GetConnectionString1() string {
	return getConnectionString(cfg.SqlServer1, cfg.Database1, cfg.User1, cfg.Password1)
}

func (cfg *Config) GetConnectionString2() string {
	return getConnectionString(cfg.SqlServer2, cfg.Database2, cfg.User2, cfg.Password2)
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