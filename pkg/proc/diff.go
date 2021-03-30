package proc

import (
	"fmt"
	"reflect"
	"strings"
)

func procDiff(fromToml, fromDB schema) (result *Queries) {
	result = &Queries{}
	if !reflect.DeepEqual(fromToml.tablesMap, fromDB.tablesMap) {
		procTableDiff(fromToml, fromDB, result)
	}
	if !reflect.DeepEqual(fromToml.indexInfosMap, fromDB.indexInfosMap) {
		procIndexDiff(fromToml, fromDB, result)
	}

	return
}

func procTableDiff(fromToml, fromDB schema, result *Queries) {
	for _, ti := range fromToml.tables {
		// tomlにあってDBにないテーブルはcreate
		if _, exist := fromDB.tablesMap[ti.name]; !exist {
			result.CreateTables = append(result.CreateTables, buildCreateTableQuery(ti))
			continue
		}
		if !reflect.DeepEqual(ti.columns, fromDB.tablesMap[ti.name].columns) {
			for idx, tc := range ti.columns {
				if _, exist := fromDB.tablesMap[ti.name].columnsMap[tc.name]; !exist {
					// tomlにあってDBにないカラムはadd
					var beforeColumnName string
					if idx != 0 {
						beforeColumnName = ti.columns[idx-1].name
					}
					result.AddColumns = append(result.AddColumns, buildAddColumnTableQuery(ti, tc, beforeColumnName))
					continue
				}
				if !reflect.DeepEqual(tc, fromDB.tablesMap[ti.name].columnsMap[tc.name]) {
					// 両方にあるがカラム内容に差分がある場合modify
					result.ModifyColumns = append(result.ModifyColumns, buildModifyColumnTableQuery(ti, tc))
				}
			}
		}
	}

	for _, ti := range fromDB.tables {
		// DBにあってtomlにないテーブルはdelete
		if _, exist := fromToml.tablesMap[ti.name]; !exist {
			result.DropTables = append(result.DropTables, buildDropTableQuery(ti))
			continue
		}
		if !reflect.DeepEqual(ti.columns, fromDB.tablesMap[ti.name].columns) {
			for _, tc := range ti.columns {
				if _, exist := fromToml.tablesMap[ti.name].columnsMap[tc.name]; !exist {
					// DBにあってtomlにないカラムはdrop
					result.DropColumns = append(result.DropColumns, buildDropColumnTableQuery(ti, tc))
				}
			}
		}
	}
}

func buildCreateTableQuery(ti tableInfo) string {
	result := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %v (`, ti.name)
	columnQueries := []string{}
	var primary string
	for _, column := range ti.columns {
		definition := []string{fmt.Sprintf("`%v`", column.name)}
		if column.size == "" {
			definition = append(definition, column.columnType)
		} else {
			definition = append(definition, fmt.Sprintf(`%v(%v)`, column.columnType, column.size))
		}
		if column.unsigned {
			definition = append(definition, "UNSIGNED")
		}
		if !column.null {
			definition = append(definition, "NOT NULL")
		}
		if column.defaultValue.need {
			definition = append(definition, fmt.Sprintf(`DEFAULT '%v'`, column.defaultValue.value))
		}
		if column.autoInc {
			// TODO: 一旦auto_incつきは強制でprimaryにする
			definition = append(definition, "AUTO_INCREMENT")
			primary = fmt.Sprintf(", PRIMARY KEY (`%v`)", column.name)
		}
		columnQueries = append(columnQueries, strings.Join(definition, " "))
	}
	result += strings.Join(columnQueries, ",") + primary + `)`

	return result
}

func buildAddColumnTableQuery(ti tableInfo, tc tableColumn, beforeColumnName string) string {
	result := fmt.Sprintf(`ALTER TABLE %v ADD COLUMN `, ti.name)
	var position string
	if beforeColumnName == "" {
		position = "FIRST"
	} else {
		position = fmt.Sprintf("AFTER `%v`", beforeColumnName)
	}
	definition := []string{fmt.Sprintf("`%v`", tc.name)}
	if tc.size == "" {
		definition = append(definition, tc.columnType)
	} else {
		definition = append(definition, fmt.Sprintf(`%v(%v)`, tc.columnType, tc.size))
	}
	if tc.unsigned {
		definition = append(definition, "UNSIGNED")
	}
	if !tc.null {
		definition = append(definition, "NOT NULL")
	}
	if tc.defaultValue.need {
		definition = append(definition, fmt.Sprintf(`DEFAULT '%v'`, tc.defaultValue.value))
	}
	if tc.autoInc {
		definition = append(definition, "AUTO_INCREMENT")
	}
	result += strings.Join(definition, " ") + fmt.Sprintf(" %v", position)

	return result
}

func buildModifyColumnTableQuery(ti tableInfo, tc tableColumn) string {
	result := fmt.Sprintf(`ALTER TABLE %v MODIFY COLUMN `, ti.name)

	definition := []string{fmt.Sprintf("`%v`", tc.name)}
	if tc.size == "" {
		definition = append(definition, tc.columnType)
	} else {
		definition = append(definition, fmt.Sprintf(`%v(%v)`, tc.columnType, tc.size))
	}
	if tc.unsigned {
		definition = append(definition, "UNSIGNED")
	}
	if !tc.null {
		definition = append(definition, "NOT NULL")
	}
	if tc.defaultValue.need {
		definition = append(definition, fmt.Sprintf(`DEFAULT '%v'`, tc.defaultValue.value))
	}
	if tc.autoInc {
		definition = append(definition, "AUTO_INCREMENT")
	}
	result += strings.Join(definition, " ")

	return result
}

func buildDropTableQuery(ti tableInfo) string {
	return fmt.Sprintf(`DROP TABLE %v`, ti.name)
}

func buildDropColumnTableQuery(ti tableInfo, tc tableColumn) string {
	return fmt.Sprintf(`ALTER TABLE %v DROP COLUMN %v`, ti.name, tc.name)
}

// TODO: 面倒なので一旦primaryの付け外しはスルー
func procIndexDiff(fromToml, fromDB schema, result *Queries) {
	for idxName, ii := range fromToml.indexInfosMap {
		if ii.indexName == "PRIMARY" {
			continue
		}
		// tomlにあってDBにないindexはadd
		if _, exist := fromDB.indexInfosMap[idxName]; !exist {
			result.AddIndexes = append(result.AddIndexes, buildAddIndexQuery(ii))
			continue
		}
		// 同一index名で差分がある場合delete add
		if !reflect.DeepEqual(ii, fromDB.indexInfosMap[idxName]) {
			result.DropIndexes = append(result.DropIndexes, buildDeleteIndexQuery(fromDB.indexInfosMap[idxName]))
			result.AddIndexes = append(result.AddIndexes, buildAddIndexQuery(ii))
		}
	}

	for idxName, ii := range fromDB.indexInfosMap {
		if ii.indexName == "PRIMARY" {
			continue
		}
		// DBにあってtomlにないindexはdrop
		if _, exist := fromToml.indexInfosMap[idxName]; !exist {
			result.DropIndexes = append(result.DropIndexes, buildDeleteIndexQuery(ii))
		}
	}
}

func buildAddIndexQuery(ii *indexInfo) string {
	var indexType string
	if ii.unique {
		indexType = "UNIQUE INDEX"
	} else {
		indexType = "INDEX"
	}

	var columns string
	for _, column := range ii.columns {
		columns += "`" + column + "`,"
	}
	columns = strings.TrimRight(columns, ",")

	return fmt.Sprintf(`ALTER TABLE %v ADD %v %v (%v)`, ii.tableName, indexType, ii.indexName, columns)
}

func buildDeleteIndexQuery(ii *indexInfo) string {
	return fmt.Sprintf(`ALTER TABLE %v DROP INDEX %v`, ii.tableName, ii.indexName)
}