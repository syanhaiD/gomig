package proc

import (
	"errors"
	"fmt"
	"github.com/BurntSushi/toml"
	"strings"
)

func parseToml(tomlPath, env, settingTomlPath string) (result schema, err error) {
	var trial interface{}
	_, err = toml.DecodeFile(tomlPath, &trial)
	if err != nil {
		return
	}

	result = schema{
		tables:          []tableInfo{},
		tablesMap:       map[string]tableInfo{},
		indexInfosSlice: map[string][]string{},
		indexInfosMap:   map[string]map[string]*indexInfo{},
	}
	parsed := trial.(map[string]interface{})
	databaseSettingKey := fmt.Sprintf("database_%v", env)
	var databaseMapIF interface{}
	var exist bool
	if settingTomlPath == "" {
		databaseMapIF, exist = parsed[databaseSettingKey]
	} else {
		var dbSettingIF interface{}
		_, err = toml.DecodeFile(settingTomlPath, &dbSettingIF)
		if err != nil {
			return
		}
		dbSettingMap := dbSettingIF.(map[string]interface{})
		databaseMapIF, exist = dbSettingMap[databaseSettingKey]
	}
	if !exist {
		err = errors.New("database setting are not found")
		return
	}
	var ok bool
	databaseMap := databaseMapIF.(map[string]interface{})
	if result.database.Name, ok = databaseMap["name"].(string); !ok {
		err = errors.New("key database.name are not found")
		return
	}
	if result.database.User, ok = databaseMap["user"].(string); !ok {
		err = errors.New("key database.user are not found")
		return
	}
	if result.database.Pass, ok = databaseMap["pass"].(string); !ok {
		err = errors.New("key database.pass are not found")
		return
	}
	if result.database.Host, ok = databaseMap["host"].(string); !ok {
		err = errors.New("key database.host are not found")
		return
	}
	if result.database.Port, ok = databaseMap["port"].(string); !ok {
		err = errors.New("key database.port are not found")
		return
	}
	if result.database.Charset, ok = databaseMap["charset"].(string); !ok {
		result.database.Charset = "utf8mb4,utf8"
	}
	if result.database.Collation, ok = databaseMap["collation"].(string); !ok {
		result.database.Collation = "utf8mb4_general_ci"
	}

	tablesSliceIF, ok := parsed["tables"].([]map[string]interface{})
	if !ok {
		err = errors.New("tables are not found")
		return
	}
	for _, tableIFMap := range tablesSliceIF {
		var ti tableInfo
		indexInfos := map[string]*indexInfo{}
		indexSlice := []string{}
		ti, indexInfos, indexSlice, err = parseTables(tableIFMap)
		if err != nil {
			return
		}
		result.tables = append(result.tables, ti)
		result.tablesMap[ti.name] = ti
		result.indexInfosMap[ti.name] = indexInfos
		dupChecker := map[string]struct{}{}
		for _, idxName := range indexSlice {
			if _, already := dupChecker[idxName]; !already {
				dupChecker[idxName] = struct{}{}
				result.indexInfosSlice[ti.name] = append(result.indexInfosSlice[ti.name], idxName)
			}
		}
	}

	return
}

func parseTables(tableIFMap map[string]interface{}) (result tableInfo, indexInfos map[string]*indexInfo, indexSlice []string, err error) {
	result = tableInfo{
		columnsMap: map[string]tableColumn{},
		partition:  partitionInfo{},
	}
	indexInfos = map[string]*indexInfo{}
	indexSlice = []string{}

	if nameIF, exist := tableIFMap["name"]; exist {
		result.name = nameIF.(string)
	} else {
		err = errors.New("require table.name")
		return
	}

	if columnsIF, exist := tableIFMap["columns"]; exist {
		columnsSliceIF := columnsIF.([]interface{})
		for _, columnsMapIF := range columnsSliceIF {
			columnsMap := columnsMapIF.(map[string]interface{})
			var tc tableColumn
			tc, err = parseColumns(columnsMap)
			if err != nil {
				return
			}
			result.columns = append(result.columns, tc)
			result.columnsMap[tc.name] = tc
		}
	} else {
		err = errors.New("require table.columns")
		return
	}

	if primaryIF, exist := tableIFMap["primary"]; exist {
		primaries := primaryIF.([]interface{})
		for _, pri := range primaries {
			primariesString := pri.(string)
			if primariesString == "" {
				continue
			}
			primariesSlice := strings.Split(primariesString, ",")
			if _, exist := indexInfos["PRIMARY"]; !exist {
				indexInfos["PRIMARY"] = &indexInfo{tableName: result.name, unique: true, indexName: "PRIMARY", indexType: "BTREE", columns: []string{}}
			}
			indexInfos["PRIMARY"].columns = append(indexInfos["PRIMARY"].columns, primariesSlice...)
			indexSlice = append(indexSlice, "PRIMARY")
		}
	}
	if indexIF, exist := tableIFMap["index"]; exist {
		indexes := indexIF.([]interface{})
		for _, idx := range indexes {
			indexesString := idx.(string)
			if indexesString == "" {
				continue
			}
			indexesSlice := strings.Split(indexesString, ",")
			indexName := "idx_" + result.name + "_" + strings.Join(indexesSlice, "_and_")
			if _, exist := indexInfos[indexName]; !exist {
				indexInfos[indexName] = &indexInfo{tableName: result.name, indexName: indexName, indexType: "BTREE", columns: []string{}}
			}
			indexInfos[indexName].columns = append(indexInfos[indexName].columns, indexesSlice...)
			indexSlice = append(indexSlice, indexName)
		}
	}
	if uniqIndexIF, exist := tableIFMap["unique_index"]; exist {
		indexes := uniqIndexIF.([]interface{})
		for _, idx := range indexes {
			indexesString := idx.(string)
			if indexesString == "" {
				continue
			}
			indexesSlice := strings.Split(indexesString, ",")
			indexName := "idx_" + result.name + "_" + strings.Join(indexesSlice, "_and_")
			if _, exist := indexInfos[indexName]; !exist {
				indexInfos[indexName] = &indexInfo{tableName: result.name, unique: true, indexName: indexName, indexType: "BTREE", columns: []string{}}
			}
			if _, exist := indexInfos["PRIMARY"]; !exist {
				for _, idxColumnName := range indexesSlice {
					if !result.columnsMap[idxColumnName].null {
						// uniqueでnot nullかつprimaryを明示していない場合、最初にnot null uniqueを指定したカラムが勝手にprimaryになるので
						// 既にprimary keyが存在する場合のみnot null uniqueを許可する
						err = errors.New(fmt.Sprintf("table: %v column: %v For those that are NOT NULL and UNIQUE, please specify primary", result.name, idxColumnName))
						return
					}
				}
			}
			indexInfos[indexName].columns = append(indexInfos[indexName].columns, indexesSlice...)
			indexSlice = append(indexSlice, indexName)
		}
	}
	if ftkIF, exist := tableIFMap["fulltext_index"]; exist {
		indexes := ftkIF.([]interface{})
		for _, idx := range indexes {
			indexesString := idx.(string)
			if indexesString == "" {
				continue
			}
			indexesSlice := strings.Split(indexesString, ",")
			indexName := "ftk_" + result.name + "_" + strings.Join(indexesSlice, "_and_")
			if _, exist := indexInfos[indexName]; !exist {
				indexInfos[indexName] = &indexInfo{tableName: result.name, indexName: indexName, indexType: "FULLTEXT", columns: []string{}}
				indexInfos[indexName].comment = `'tokenizer "TokenBigramSplitSymbolAlphaDigit"'`
			}
			indexInfos[indexName].columns = append(indexInfos[indexName].columns, indexesSlice...)
			indexSlice = append(indexSlice, indexName)
		}
	}
	if engineIF, exist := tableIFMap["engine"]; exist {
		// 一旦Mroongaのみ対応
		engine := strings.ToLower(engineIF.(string))
		engine = strings.Title(engine)
		result.engine = "Mroonga"
	}
	if partitionIF, exist := tableIFMap["partition"]; exist {
		partitionMap := partitionIF.(map[string]interface{})
		result.partition, err = parsePartition(partitionMap)
		if err != nil {
			return
		}
	}

	return
}

func parseColumns(columnsMap map[string]interface{}) (result tableColumn, err error) {
	result = tableColumn{}

	if columnIF, exist := columnsMap["name"]; exist {
		result.name = columnIF.(string)
	} else {
		err = errors.New("require table.column.name")
		return
	}
	if columnIF, exist := columnsMap["type"]; exist {
		result.columnType = strings.ToLower(columnIF.(string))
	} else {
		err = errors.New("require table.column.type")
		return
	}
	if columnIF, exist := columnsMap["unsigned"]; exist {
		result.unsigned = columnIF.(bool)
	}
	if columnIF, exist := columnsMap["size"]; exist {
		result.size = columnIF.(string)
	} else {
		if result.columnType == "char" || result.columnType == "varchar" {
			err = errors.New(fmt.Sprintf("column type %v require size", result.columnType))
			return
		}
		switch result.columnType {
		case "int":
			if result.unsigned {
				result.size = "10"
			} else {
				result.size = "11"
			}
		case "bigint":
			result.size = "20"
		case "tinyint":
			if result.unsigned {
				result.size = "3"
			} else {
				result.size = "4"
			}
		case "smallint":
			if result.unsigned {
				result.size = "5"
			} else {
				result.size = "6"
			}
		case "mediumint":
			if result.unsigned {
				result.size = "8"
			} else {
				result.size = "9"
			}
		}
	}
	if columnIF, exist := columnsMap["autoinc"]; exist {
		result.autoInc = columnIF.(bool)
	}
	if columnIF, exist := columnsMap["null"]; exist {
		result.null = columnIF.(bool)
	}
	// emptyを明示的に設定したいかどうかの判別
	dd := defaultDetail{}
	if columnIF, exist := columnsMap["default"]; exist {
		dd.need = true
		dd.value = columnIF.(string)
	} else {
		dd.need = false
	}
	result.defaultValue = dd

	return
}

// 入力ミス関係のチェックはしてないので注意
func parsePartition(partitionMap map[string]interface{}) (result partitionInfo, err error) {
	result = partitionInfo{}

	if pIF, exist := partitionMap["type"]; exist {
		result.partitionType = strings.ToLower(pIF.(string))
	} else {
		err = errors.New("require partition.type")
		return
	}
	result.partitionType = "range" // 2021-06-16 range固定

	if pIF, exist := partitionMap["key"]; exist {
		result.keyColumn = strings.ToLower(pIF.(string))
	} else {
		err = errors.New("require partition.key")
		return
	}
	if pIF, exist := partitionMap["basename"]; exist {
		result.baseName = strings.ToLower(pIF.(string))
	} else {
		err = errors.New("require partition.basename")
		return
	}
	if pIF, exist := partitionMap["start"]; exist {
		result.startNum = pIF.(string)
	}
	result.startNum = "1" // 2021-06-16 一旦1固定

	if pIF, exist := partitionMap["end"]; exist {
		result.endNum = pIF.(string)
	} else {
		err = errors.New("require partition.end")
		return
	}
	if pIF, exist := partitionMap["each"]; exist {
		result.eachRow = pIF.(string)
	}
	result.eachRow = "10000" // 2021-06-16 一旦10000固定

	return
}
