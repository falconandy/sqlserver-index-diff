package indexdiff

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/ini.v1"
)

type Config struct {
	Server   string
	Port     int
	Database string
	User     string
	Password string
}

func loadConfiguration() ([]*Config, error) {
	cfg, err := ini.Load(filepath.Join(filepath.Dir(os.Args[0]), "_config.ini"))
	if err != nil {
		return nil, err
	}
	cfg.BlockMode = false
	configs := make([]*Config, 0, 10)
	for _, section := range cfg.Sections() {
		if strings.HasPrefix(section.Name(), "Database") {
			config := &Config{}
			config.Server = strings.TrimSpace(section.Key("Server").String())
			config.Port, _ = strconv.Atoi(strings.TrimSpace(section.Key("Port").String()))
			config.Database = strings.TrimSpace(section.Key("Database").String())
			config.User = strings.TrimSpace(section.Key("User").String())
			config.Password = strings.TrimSpace(section.Key("Password").String())
			configs = append(configs, config)
		}
	}

	return configs, nil
}
