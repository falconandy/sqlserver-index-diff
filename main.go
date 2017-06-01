package indexdiff

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
)

type index struct {
	scheme   string
	table    string
	index    string
	enabled  bool
	unique   bool
	columns  []string
	included []string
}

func (idx *index) String() string {
	result := fmt.Sprintf("%s.%s (%s)", idx.scheme, idx.table, strings.Join(idx.columns, ", "))
	if len(idx.included) > 0 {
		result += fmt.Sprintf(" INCLUDED(%s)", strings.Join(idx.included, ", "))
	}
	if !idx.enabled {
		result += " DISABLED"
	}
	if idx.unique {
		result += " UNIQUE"
	}
	result += " --NAME=" + idx.index
	return result
}

func SaveSortedIndexes() {
	cfgs, err := loadConfiguration()
	if err != nil {
		log.Fatal(err)
	}
	done := make(chan bool, len(cfgs))
	for _, cfg := range cfgs {
		go getAnSaveSortedIndexes(cfg, done)
	}
	for i := 0; i < len(cfgs); i ++ {
		<-done
	}
}

func getAnSaveSortedIndexes(cfg *Config, done chan<- bool) {
	var engine Engine
	if cfg.Port == 0 {
		engine = NewMsSqlEngine(cfg)
	} else {
		engine = NewPostgresEngine(cfg)
	}

	indexes := engine.GetIndexes()
	fileName := cfg.Database+"__"+strings.Replace(cfg.Server, `\`, "_", -1)
	if cfg.Port != 0 {
		fileName += fmt.Sprintf("_%d", cfg.Port)
	}
	saveSortedIndexes(fileName, indexes)
	done <- true
}

func saveSortedIndexes(fileName string, indexes []*index) {
	strIndexes := make([]string, len(indexes))
	for i, idx := range indexes {
		strIndexes[i] = idx.String()
	}
	sort.Strings(strIndexes)

	file, err := os.Create(fileName + ".sql")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range strIndexes {
		fmt.Fprintln(w, line)
	}
	err = w.Flush()
	if err != nil {
		log.Fatal(err)
	}
}
