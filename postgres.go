package indexdiff

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"

	_ "github.com/lib/pq"
)

var postgresIndexesQuery = `
select schemaname, tablename, indexname, indexdef
from pg_indexes
where schemaname <> 'pg_catalog'
order by schemaname, tablename, indexname, indexdef;`

var postgresIndexRe = regexp.MustCompile(`.* USING btree \((.*)\)`)

type PostgresEngine struct {
	config *Config
}

func NewPostgresEngine(config *Config) *PostgresEngine {
	return &PostgresEngine{config: config}
}

func (e *PostgresEngine) getConnectionString() string {
	connStr := fmt.Sprintf("host=%s port=%d database=%s sslmode=disable", e.config.Server, e.config.Port, e.config.Database)
	if e.config.User != "" {
		connStr += fmt.Sprintf(" user=%s", e.config.User)
	}
	if e.config.Password != "" {
		connStr += fmt.Sprintf(" password=%s", e.config.Password)
	}
	return connStr
}

func (e *PostgresEngine) GetIndexes() []*index {
	conn, err := sql.Open("postgres", e.getConnectionString())
	defer conn.Close()

	rows, err := conn.Query(postgresIndexesQuery)
	if err != nil {
		log.Fatal("Query failed:", err.Error())
	}
	defer rows.Close()

	var schemeName, tableName, indexName, indexDef string
	indexes := make([]*index, 0)
	var prevSchemeName, prevTableName, prevIndexName string
	var currentIndex *index
	for rows.Next() {
		err = rows.Scan(&schemeName, &tableName, &indexName, &indexDef)
		if err != nil {
			log.Fatal("Scan failed:", err.Error())
		}
		if schemeName != prevSchemeName || tableName != prevTableName || indexName != prevIndexName {
			prevSchemeName, prevTableName, prevIndexName = schemeName, tableName, indexName
			currentIndex = &index{scheme: schemeName, table: tableName, index: indexName, enabled: true, columns: make([]string, 0, 10)}
			indexes = append(indexes, currentIndex)

			match := postgresIndexRe.FindStringSubmatch(indexDef)
			columns := strings.Split(match[1], ",")
			for _, column := range columns {
				column = strings.TrimSpace(column)
				currentIndex.columns = append(currentIndex.columns, column)
			}
		}
	}
	return indexes
}
