package indexdiff

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

var postgresIndexesQuery = `
select 'a', '', '', '', 0, true, false, true;`

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

	var schemeName, tableName, columnName, indexName string
	var indexColumnId int
	var isDescending, isIncluded, isDisabled bool
	indexes := make([]*index, 0)
	var prevSchemeName, prevTableName, prevIndexName string
	var currentIndex *index
	for rows.Next() {
		err = rows.Scan(&schemeName, &tableName, &columnName, &indexName, &indexColumnId, &isDescending, &isIncluded, &isDisabled)
		if err != nil {
			log.Fatal("Scan failed:", err.Error())
		}
		if schemeName != prevSchemeName || tableName != prevTableName || indexName != prevIndexName {
			prevSchemeName, prevTableName, prevIndexName = schemeName, tableName, indexName
			currentIndex = &index{scheme: schemeName, table: tableName, index: indexName, enabled: !isDisabled, columns: make([]string, 0, 10), included: make([]string, 0, 10)}
			indexes = append(indexes, currentIndex)
		}
		if !isIncluded {
			if isDescending {
				columnName += " DESC"
			}
			currentIndex.columns = append(currentIndex.columns, columnName)
		} else {
			currentIndex.included = append(currentIndex.included, columnName)
		}
	}
	return indexes
}
