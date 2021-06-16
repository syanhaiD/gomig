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
		tables:          []tableInfo{},
		tablesMap:       map[string]tableInfo{},
		indexInfosSlice: map[string][]string{},
		indexInfosMap:   map[string]map[string]*indexInfo{},
	}
	indexInfosMap, indexMapSlice, err := parseDBIndex(dbName)
	if err != nil {
		return
	}
	for tableName, idxes := range indexMapSlice {
		dupChecker := map[string]struct{}{}
		for _, idxName := range idxes {
			if _, exist := result.indexInfosSlice[tableName]; !exist {
				result.indexInfosSlice[tableName] = []string{}
			}
			if _, exist := dupChecker[idxName]; !exist {
				dupChecker[idxName] = struct{}{}
				result.indexInfosSlice[tableName] = append(result.indexInfosSlice[tableName], idxName)
			}
		}
	}

	partitionsInfoMap, err := parseDBPartition(dbName)
	if err != nil {
		return
	}
	useMroongaTableMap, err := parseDBMroongaTable(dbName)
	if err != nil {
		return
	}

	var desc *sql.Rows
	for _, table := range tables {
		ti := tableInfo{name: table, columns: []tableColumn{}, columnsMap: map[string]tableColumn{}, partition: partitionInfo{}}
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
			if strings.ToLower(dc.null) == "yes" {
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
		// partition
		if pInfo, exist := partitionsInfoMap[table]; exist {
			ti.partition = pInfo
		}
		// engine
		if _, exist := useMroongaTableMap[table]; exist {
			ti.engine = "Mroonga"
		}
		result.tables = append(result.tables, ti)
		result.tablesMap[table] = ti
		// idx
		result.indexInfosMap[table] = map[string]*indexInfo{}
		if _, exist := indexInfosMap[table]; exist {
			result.indexInfosMap[table] = indexInfosMap[table]
		}
	}
	if desc != nil {
		if err = desc.Close(); err != nil {
			return
		}
	}

	return
}

func parseDBIndex(dbName string) (indexInfos map[string]map[string]*indexInfo, indexMapSlice map[string][]string, err error) {
	indexInfos = map[string]map[string]*indexInfo{}
	indexMapSlice = map[string][]string{}

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
		if _, exist := indexInfos[idxInfo.tableName]; !exist {
			indexInfos[idxInfo.tableName] = map[string]*indexInfo{}
		}
		if _, exist := indexMapSlice[idxInfo.tableName]; !exist {
			indexMapSlice[idxInfo.tableName] = []string{}
		}
		indexMapSlice[idxInfo.tableName] = append(indexMapSlice[idxInfo.tableName], idxInfo.indexName)
		if _, exist := indexInfos[idxInfo.tableName][idxInfo.indexName]; !exist {
			indexInfos[idxInfo.tableName][idxInfo.indexName] = idxInfo
		}
		if idxInfo.indexType == "FULLTEXT" {
			idxInfo.comment = `'tokenizer "TokenBigramSplitSymbolAlphaDigit"'`
		}
		indexInfos[idxInfo.tableName][idxInfo.indexName].columns = append(indexInfos[idxInfo.tableName][idxInfo.indexName].columns, columnName)
	}

	return
}

func parseDBPartition(dbName string) (partitionInfosMap map[string]partitionInfo, err error) {
	partitionInfosMap = map[string]partitionInfo{}

	var rows *sql.Rows
	rows, err = dbConn.Query(partitionQuery(), dbName)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	if err = rows.Err(); err != nil {
		return
	}

	for rows.Next() {
		pInfo := partitionInfo{startNum: "1", eachRow: "10000"}
		var tableName string
		var partitionBaseName string
		err = rows.Scan(
			&tableName, &partitionBaseName, &pInfo.endNum, &pInfo.partitionType, &pInfo.keyColumn,
		)
		if err != nil {
			fmt.Println(err)
			continue
		}
		pInfo.partitionType = strings.ToLower(pInfo.partitionType)
		splitPartitionName := strings.Split(partitionBaseName, pInfo.endNum)
		pInfo.baseName = strings.Join(splitPartitionName[:len(splitPartitionName)-1], pInfo.endNum)
		partitionInfosMap[tableName] = pInfo
	}

	return
}

func parseDBMroongaTable(dbName string) (useMroongaTableMap map[string]struct{}, err error) {
	useMroongaTableMap = map[string]struct{}{}

	var rows *sql.Rows
	rows, err = dbConn.Query(mroongaEngineQuery(), dbName)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	if err = rows.Err(); err != nil {
		return
	}

	for rows.Next() {
		var tableName string
		var engine string
		err = rows.Scan(
			&tableName, &engine,
		)
		if err != nil {
			fmt.Println(err)
			continue
		}
		useMroongaTableMap[tableName] = struct{}{}
	}

	return
}

func indexQuery() string {
	return "SELECT TABLE_NAME, NON_UNIQUE, INDEX_TYPE, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME from INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = ? ORDER BY INDEX_NAME, SEQ_IN_INDEX"
}

func partitionQuery() string {
	query := "SELECT ifp.TABLE_NAME, ifp.PARTITION_NAME, ifp.PARTITION_ORDINAL_POSITION, ifp.PARTITION_METHOD, ifp.PARTITION_EXPRESSION" +
		" FROM INFORMATION_SCHEMA.PARTITIONS ifp" +
		" LEFT JOIN INFORMATION_SCHEMA.PARTITIONS AS ifp2 ON ifp.TABLE_NAME = ifp2.TABLE_NAME AND ifp.PARTITION_ORDINAL_POSITION < ifp2.PARTITION_ORDINAL_POSITION" +
		" WHERE ifp.TABLE_SCHEMA = ? AND ifp.PARTITION_NAME IS NOT NULL AND ifp2.PARTITION_ORDINAL_POSITION IS NULL" +
		" ORDER BY ifp.TABLE_NAME"

	return query
}

func mroongaEngineQuery() string {
	return "SELECT table_name, engine FROM information_schema.tables WHERE table_schema = ? AND engine = 'Mroonga'"
}
