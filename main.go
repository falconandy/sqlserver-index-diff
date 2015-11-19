package main

import (
	"database/sql"
	_ "github.com/denisenkom/go-mssqldb"
	"log"
	"fmt"
	"strings"
	"sort"
	"os"
	"bufio"
)

var indexes_query = `
select  s1.name as SchemaName, t1.name as TableName, c1.name as ColumnName, i1.name as IndexName, ic1.index_column_id, ic1.is_descending_key, ic1.is_included_column, i1.is_disabled
from    sys.schemas s1
        join sys.tables t1 on t1.schema_id = s1.schema_id
        join sys.columns c1 on t1.object_id = c1.object_id
        join sys.types ty1 on ty1.system_type_id = c1.system_type_id and ty1.user_type_id = c1.user_type_id
        join sys.index_columns ic1 on ic1.object_id = c1.object_id and ic1.column_id = c1.column_id
        join sys.indexes i1 on i1.object_id = ic1.object_id and i1.index_id = ic1.index_id
where i1.is_hypothetical = 0
order by s1.name, t1.name, i1.name, ic1.index_column_id`

type index struct {
	scheme string
	table string
	index string
	enabled bool
	columns []string
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
	result += " --NAME=" + idx.index
	return result
}

func main() {
	cfg, err := LoadConfiguration()
	if err != nil {
		log.Fatal(err)
	}
	indexes1 := getIndexes(cfg.GetConnectionString1())
	indexes2 := getIndexes(cfg.GetConnectionString2())
	saveSortedIndexes(cfg.Database1, indexes1)
	saveSortedIndexes(cfg.Database2, indexes2)
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

func getIndexes(connectionString string) []*index {
	conn, err := sql.Open("mssql", connectionString)
	defer conn.Close()

	rows, err := conn.Query(indexes_query)
	if err != nil {
		log.Fatal("Query failed:", err.Error())
	}
	defer rows.Close()

	var schemeName string
	var tableName string
	var columnName string
	var indexName string
	var indexColumnId int
	var isDescending bool
	var isIncluded bool
	var isDisabled bool
	indexes := make([]*index, 0)
	var prevSchemeName string
	var prevTableName string
	var prevIndexName string
	var currentIndex *index
	for rows.Next() {
		err = rows.Scan(&schemeName, &tableName, &columnName, &indexName, &indexColumnId, &isDescending, &isIncluded, &isDisabled)
		if err != nil {
			log.Fatal("Scan failed:", err.Error())
		}
		if schemeName != prevSchemeName || tableName != prevTableName || indexName != prevIndexName {
			prevSchemeName, prevTableName, prevIndexName = schemeName, tableName, indexName
			if currentIndex != nil {
				indexes = append(indexes, currentIndex)
			}
			currentIndex = &index{scheme:schemeName, table:tableName, index:indexName, enabled:!isDisabled, columns:make([]string, 0, 10), included:make([]string, 0, 10)}
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
	if currentIndex != nil {
		indexes = append(indexes, currentIndex)
	}
	return indexes
}