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

	indexesByTableNameMap := map[string][]string{}
	uniqIndexesByTableNameMap := map[string][]string{}
	for _, ii := range fromDB.indexInfosMap {
		columnsString := strings.Join(ii.columns, ",")
		if ii.unique {
			if _, exist := uniqIndexesByTableNameMap[ii.tableName]; !exist {
				uniqIndexesByTableNameMap[ii.tableName] = []string{}
			}
			uniqIndexesByTableNameMap[ii.tableName] = append(uniqIndexesByTableNameMap[ii.tableName], columnsString)
		} else {
			if _, exist := indexesByTableNameMap[ii.tableName]; !exist {
				indexesByTableNameMap[ii.tableName] = []string{}
			}
			indexesByTableNameMap[ii.tableName] = append(indexesByTableNameMap[ii.tableName], columnsString)
		}
	}

	var result []string
	for _, ti := range fromDB.tables {
		result = append(result, `[[tables]]`, fmt.Sprintf(`name = "%v"`, ti.name), `columns = [`)
		columnLines := []string{}
		for _, col := range ti.columns {
			columnLine := fmt.Sprintf(`    {name = "%v", type = "%v"`, col.name, col.columnType)
			if col.unsigned {
				columnLine += fmt.Sprintf(`, unsigned = true`)
			}
			if !col.null {
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
		if pKeys, exist := fromDB.primaryKeysMap[ti.name]; exist {
			result = append(result, fmt.Sprintf(`primary = ["%v"]`, strings.Join(pKeys, `,`)))
		}
		if idxColumns, exist := indexesByTableNameMap[ti.name]; exist {
			result = append(result, fmt.Sprintf(`index = ["%v"]`, strings.Join(idxColumns, `,`)))
		}
		if uniqIdxColumns, exist := uniqIndexesByTableNameMap[ti.name]; exist {
			result = append(result, fmt.Sprintf(`unique_index = ["%v"]`, strings.Join(uniqIdxColumns, `,`)))
		}
		result[len(result)-1] += "\n"
	}
	fmt.Println(strings.Join(result, "\n"))

	return
}
