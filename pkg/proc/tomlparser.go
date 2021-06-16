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
		tables:         []tableInfo{},
		tablesMap:      map[string]tableInfo{},
		indexInfosMap:  map[string]map[string]*indexInfo{},
		engine:         map[string]string{},
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
		var engine string
		indexInfos := map[string]map[string]*indexInfo{}
		ti, indexInfos, engine, err = parseTables(tableIFMap)
		if err != nil {
			return
		}
		result.tables = append(result.tables, ti)
		result.tablesMap[ti.name] = ti
		for key, value := range indexInfos {
			result.indexInfosMap[key] = value
		}
		if engine != "" {
			result.engine[ti.name] = engine
		}
	}

	return
}

func parseTables(tableIFMap map[string]interface{}) (result tableInfo, indexInfos map[string]map[string]*indexInfo, engine string, err error) {
	result = tableInfo{
		columnsMap: map[string]tableColumn{},
	}
	indexInfos = map[string]map[string]*indexInfo{}

	if nameIF, exist := tableIFMap["name"]; exist {
		result.name = nameIF.(string)
	} else {
		err = errors.New("require table.name")
		return
	}
	indexInfos[result.name] = map[string]*indexInfo{}

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
			if _, exist := indexInfos[result.name]["PRIMARY"]; !exist {
				indexInfos[result.name]["PRIMARY"] = &indexInfo{tableName: result.name, unique: true, indexName: "PRIMARY", indexType: "BTREE", columns: []string{}}
			}
			indexInfos[result.name]["PRIMARY"].columns = append(indexInfos[result.name]["PRIMARY"].columns, primariesSlice...)
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
			if _, exist := indexInfos[result.name][indexName]; !exist {
				indexInfos[result.name][indexName] = &indexInfo{tableName: result.name, indexName: indexName, indexType: "BTREE", columns: []string{}}
			}
			indexInfos[result.name][indexName].columns = append(indexInfos[result.name][indexName].columns, indexesSlice...)
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
			if _, exist := indexInfos[result.name][indexName]; !exist {
				indexInfos[result.name][indexName] = &indexInfo{tableName: result.name, unique: true, indexName: indexName, indexType: "BTREE", columns: []string{}}
			}
			if _, exist := indexInfos[result.name]["PRIMARY"]; !exist {
				for _, idxColumnName := range indexesSlice {
					if !result.columnsMap[idxColumnName].null {
						// uniqueでnot nullかつprimaryを明示していない場合、最初にnot null uniqueを指定したカラムが勝手にprimaryになるので
						// 既にprimary keyが存在する場合のみnot null uniqueを許可する
						err = errors.New(fmt.Sprintf("table: %v column: %v For those that are NOT NULL and UNIQUE, please specify primary", result.name, idxColumnName))
						return
					}
				}
			}
			indexInfos[result.name][indexName].columns = append(indexInfos[result.name][indexName].columns, indexesSlice...)
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
			if _, exist := indexInfos[result.name][indexName]; !exist {
				indexInfos[result.name][indexName] = &indexInfo{tableName: result.name, indexName: indexName, indexType: "FULLTEXT", columns: []string{}}
				indexInfos[result.name][indexName].comment = `'tokenizer "TokenBigramSplitSymbolAlphaDigit"'`
			}
			indexInfos[result.name][indexName].columns = append(indexInfos[result.name][indexName].columns, indexesSlice...)
		}
	}
	if engineIF, exist := tableIFMap["engine"]; exist {
		engine = engineIF.(string)
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
