package indexdiff

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/denisenkom/go-mssqldb"
)

var msIndexesQuery = `
select  s1.name as SchemaName, t1.name as TableName, c1.name as ColumnName, i1.name as IndexName, ic1.key_ordinal, ic1.is_descending_key, ic1.is_included_column, i1.is_disabled, i1.is_unique
from    sys.schemas s1
        join sys.tables t1 on t1.schema_id = s1.schema_id
        join sys.columns c1 on t1.object_id = c1.object_id
        join sys.types ty1 on ty1.system_type_id = c1.system_type_id and ty1.user_type_id = c1.user_type_id
        join sys.index_columns ic1 on ic1.object_id = c1.object_id and ic1.column_id = c1.column_id
        join sys.indexes i1 on i1.object_id = ic1.object_id and i1.index_id = ic1.index_id
where i1.is_hypothetical = 0
order by s1.name, t1.name, i1.name, ic1.key_ordinal`

type MsSqlEngine struct {
	config *Config
}

func NewMsSqlEngine(config *Config) *MsSqlEngine {
	return &MsSqlEngine{config: config}
}

func (e *MsSqlEngine) getConnectionString() string {
	connStr := fmt.Sprintf("server=%s;database=%s", e.config.Server, e.config.Database)
	if e.config.User != "" {
		connStr += fmt.Sprintf(";user id=%s", e.config.User)
	}
	if e.config.Password != "" {
		connStr += fmt.Sprintf(";password=%s", e.config.Password)
	}
	return connStr
}

func (e *MsSqlEngine) GetIndexes() []*index {
	conn, err := sql.Open("mssql", e.getConnectionString())
	defer conn.Close()

	rows, err := conn.Query(msIndexesQuery)
	if err != nil {
		log.Fatal("Query failed:", err.Error())
	}
	defer rows.Close()

	var schemeName, tableName, columnName, indexName string
	var indexColumnId int
	var isDescending, isIncluded, isDisabled, isUnique bool
	indexes := make([]*index, 0)
	var prevSchemeName, prevTableName, prevIndexName string
	var currentIndex *index
	for rows.Next() {
		err = rows.Scan(&schemeName, &tableName, &columnName, &indexName, &indexColumnId, &isDescending, &isIncluded, &isDisabled, &isUnique)
		if err != nil {
			log.Fatal("Scan failed:", err.Error())
		}
		if schemeName != prevSchemeName || tableName != prevTableName || indexName != prevIndexName {
			prevSchemeName, prevTableName, prevIndexName = schemeName, tableName, indexName
			currentIndex = &index{scheme: schemeName, table: tableName, index: indexName, enabled: !isDisabled, unique: isUnique, columns: make([]string, 0, 10), included: make([]string, 0, 10)}
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
