package proc

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var dbTypeReg = regexp.MustCompile(`(.+)\((.+)\)(.*)`)

func parseDB(dbName string) (result schema, err error) {
	tableRows, err := dbConn.Query("SHOW TABLES")
	if err != nil {
		return
	}
	defer func() { _ = tableRows.Close() }()
	if err = tableRows.Err(); err != nil {
		return
	}

	tables := []string{}
	for tableRows.Next() {
		var tableName string
		err = tableRows.Scan(
			&tableName,
		)
		if err != nil {
			fmt.Println(err)
			continue
		}
		tables = append(tables, tableName)
	}

	result = schema{
		tables:         []tableInfo{},
		tablesMap:      map[string]tableInfo{},
		indexInfosMap:  map[string]*indexInfo{},
		primaryKeysMap: map[string][]string{},
	}
	var desc *sql.Rows
	for _, table := range tables {
		ti := tableInfo{name: table, columns: []tableColumn{}, columnsMap: map[string]tableColumn{}}
		desc, err = dbConn.Query(fmt.Sprintf("DESC %v", table))
		if err != nil {
			return
		}
		if err = desc.Err(); err != nil {
			return
		}
		for desc.Next() {
			dc := &descColumns{}
			err = desc.Scan(
				&dc.field, &dc.columnType, &dc.null, &dc.key, &dc.defaultValue, &dc.extra,
			)
			if err != nil {
				fmt.Println(err)
				continue
			}
			tc := tableColumn{}
			tc.name = dc.field
			lowerType := strings.ToLower(dc.columnType)
			if strings.Contains(lowerType, "(") {
				// 括弧が含まれていればサイズ指定があるカラムタイプ
				res := dbTypeReg.FindAllStringSubmatch(lowerType, -1)
				if len(res) <= 0 {
					err = errors.New(fmt.Sprintf("table: %v column: %v type: %v is unknown format", table, dc.field, dc.columnType))
					return
				}
				tc.columnType = res[0][1]
				tc.size = res[0][2]
				if len(res[0]) >= 3 && strings.Contains(res[0][3], "unsigned") {
					tc.unsigned = true
				}
			} else {
				// TODO: 他のカラムタイプもなんかあるかもしれん
				splitedType := strings.Split(lowerType, " ")
				if len(splitedType) > 1 && strings.Contains(splitedType[1], "unsigned") {
					tc.unsigned = true
				}
				tc.columnType = splitedType[0]
			}
			if dc.null == "Yes" {
				tc.null = true
			}
			if dc.defaultValue.Valid {
				tc.defaultValue.need = true
				tc.defaultValue.value = dc.defaultValue.String
			}
			if strings.Contains(dc.extra, "auto_increment") {
				tc.autoInc = true
			}
			ti.columns = append(ti.columns, tc)
			ti.columnsMap[tc.name] = tc
		}
		result.tables = append(result.tables, ti)
		result.tablesMap[table] = ti
	}
	if desc != nil {
		if err = desc.Close(); err != nil {
			return
		}
	}

	result.indexInfosMap, result.primaryKeysMap, err = parseDBIndex(dbName)
	if err != nil {
		return
	}

	return
}

func parseDBIndex(dbName string) (indexInfos map[string]*indexInfo, pksMap map[string][]string, err error) {
	indexInfos = map[string]*indexInfo{}
	pksMap = map[string][]string{}

	var rows *sql.Rows
	rows, err = dbConn.Query(indexQuery(), dbName)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	if err = rows.Err(); err != nil {
		return
	}

	for rows.Next() {
		idxInfo := &indexInfo{}
		var nonUnique int
		var seq int // not use
		var columnName string
		err = rows.Scan(
			&idxInfo.tableName, &nonUnique, &idxInfo.indexType, &idxInfo.indexName, &seq, &columnName,
		)
		if err != nil {
			fmt.Println(err)
			continue
		}
		if nonUnique == 0 {
			idxInfo.unique = true
		}
		if _, exist := indexInfos[idxInfo.indexName]; !exist {
			indexInfos[idxInfo.indexName] = idxInfo
		}
		indexInfos[idxInfo.indexName].columns = append(indexInfos[idxInfo.indexName].columns, columnName)
		// 2021-06-15現在、ExportTomlでしか利用していないがDBから読むときもPrimaryの情報を選別しておく
		if idxInfo.indexName == "PRIMARY" {
			if _, exist := pksMap[idxInfo.tableName]; !exist {
				pksMap[idxInfo.tableName] = []string{}
			}
			pksMap[idxInfo.tableName] = append(pksMap[idxInfo.tableName], columnName)
		}
	}

	return
}

func indexQuery() string {
	return "SELECT TABLE_NAME, NON_UNIQUE, INDEX_TYPE, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME from INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = ? ORDER BY INDEX_NAME, SEQ_IN_INDEX"
}
