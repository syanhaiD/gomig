package proc

import (
	"fmt"
	"strings"
)

func ExportToml(tomlPath, env, settingTomlPath string) (err error) {
	fromToml, err := parseToml(tomlPath, env, settingTomlPath)
	if err != nil {
		return
	}
	connect(fromToml.database)
	defer dbConn.Close()
	fromDB, err := parseDB(fromToml.database.Name)
	if err != nil {
		return
	}

	pKeysByTableNameMap := map[string][]string{}
	indexesByTableNameMap := map[string][]string{}
	fulltextIDXByTableNameMap := map[string][]string{}
	uniqIndexesByTableNameMap := map[string][]string{}
	for tableName, sortedIDXes := range fromDB.indexInfosSlice {
		for _, idxName := range sortedIDXes {
			ii := fromDB.indexInfosMap[tableName][idxName]
			columnsString := strings.Join(ii.columns, ",")
			if idxName == "PRIMARY" {
				if _, exist := pKeysByTableNameMap[ii.tableName]; !exist {
					pKeysByTableNameMap[ii.tableName] = []string{}
				}
				pKeysByTableNameMap[ii.tableName] = append(pKeysByTableNameMap[ii.tableName], columnsString)
				continue
			}
			if ii.unique {
				if _, exist := uniqIndexesByTableNameMap[ii.tableName]; !exist {
					uniqIndexesByTableNameMap[ii.tableName] = []string{}
				}
				uniqIndexesByTableNameMap[ii.tableName] = append(uniqIndexesByTableNameMap[ii.tableName], columnsString)
			} else if ii.indexType == "FULLTEXT" {
				if _, exist := fulltextIDXByTableNameMap[ii.tableName]; !exist {
					fulltextIDXByTableNameMap[ii.tableName] = []string{}
				}
				fulltextIDXByTableNameMap[ii.tableName] = append(fulltextIDXByTableNameMap[ii.tableName], columnsString)
			} else {
				if _, exist := indexesByTableNameMap[ii.tableName]; !exist {
					indexesByTableNameMap[ii.tableName] = []string{}
				}
				indexesByTableNameMap[ii.tableName] = append(indexesByTableNameMap[ii.tableName], columnsString)
			}
		}
	}

	var result []string
	for _, ti := range fromDB.tables {
		result = append(result, `[[tables]]`, fmt.Sprintf(`name = "%v"`, ti.name), `columns = [`)
		columnLines := []string{}
		for _, col := range ti.columns {
			columnLine := fmt.Sprintf(`    {name = "%v", type = "%v"`, col.name, col.columnType)
			if shouldAddColumnSize(col.columnType, col.size, col.unsigned) {
				columnLine += fmt.Sprintf(`, size = "%v"`, col.size)
			}
			if col.unsigned {
				columnLine += fmt.Sprintf(`, unsigned = true`)
			}
			if col.null {
				columnLine += fmt.Sprintf(`, null = true`)
			} else {
				columnLine += fmt.Sprintf(`, null = false`)
			}
			if col.autoInc {
				columnLine += fmt.Sprintf(`, autoinc = true`)
			}
			if col.defaultValue.need {
				columnLine += fmt.Sprintf(`, default = "%v"`, col.defaultValue.value)
			}
			columnLine += `},`
			columnLines = append(columnLines, columnLine)
		}
		columnLines[len(columnLines) - 1] = strings.TrimRight(columnLines[len(columnLines) - 1], ",")
		result = append(result, columnLines...)
		result = append(result, `]`)
		if pKeys, exist := pKeysByTableNameMap[ti.name]; exist {
			result = append(result, fmt.Sprintf(`primary = ["%v"]`, strings.Join(pKeys, ``)))
		}
		if idxColumns, exist := indexesByTableNameMap[ti.name]; exist {
			var idxesString string
			for _, column := range idxColumns {
				idxesString += fmt.Sprintf(`"%v",`, column)
			}
			idxesString = strings.TrimRight(idxesString, ",")
			result = append(result, fmt.Sprintf(`index = [%v]`, idxesString))
		}
		if uniqIdxColumns, exist := uniqIndexesByTableNameMap[ti.name]; exist {
			var idxesString string
			for _, column := range uniqIdxColumns {
				idxesString += fmt.Sprintf(`"%v",`, column)
			}
			idxesString = strings.TrimRight(idxesString, ",")
			result = append(result, fmt.Sprintf(`unique_index = [%v]`, idxesString))
		}
		if idxColumns, exist := fulltextIDXByTableNameMap[ti.name]; exist {
			var idxesString string
			for _, column := range idxColumns {
				idxesString += fmt.Sprintf(`"%v",`, column)
			}
			idxesString = strings.TrimRight(idxesString, ",")
			result = append(result, fmt.Sprintf(`fulltext_index = [%v]`, idxesString))
		}
		if ti.partition.partitionType != "" {
			result = append(result, fmt.Sprintf(`partition = {type = "%v", key = "%v", basename = "%v", start = "%v", end = "%v", each = "%v"}`,
				ti.partition.partitionType, ti.partition.keyColumn, ti.partition.baseName, ti.partition.startNum, ti.partition.endNum, ti.partition.eachRow))
		}
		if ti.engine != "" {
			result = append(result, fmt.Sprintf(`engine = "%v"`, ti.engine))
		}
		result[len(result)-1] += "\n"
	}
	fmt.Println(strings.Join(result, "\n"))

	return
}

func shouldAddColumnSize(columnType, size string, unsigned bool) (result bool) {
	switch columnType {
	case "varchar":
		result = true
	case "int":
		if unsigned && size != "10" {
			result = true
		} else if !unsigned && size != "11" {
			result = true
		}
	case "bigint":
		if size != "20" {
			result = true
		}
	case "tinyint":
		if unsigned && size != "3" {
			result = true
		} else if !unsigned && size != "4" {
			result = true
		}
	case "smallint":
		if unsigned && size != "5" {
			result = true
		} else if !unsigned && size != "6" {
			result = true
		}
	case "mediumint":
		if unsigned && size != "8" {
			result = true
		} else if !unsigned && size != "9" {
			result = true
		}
	}

	return
}
