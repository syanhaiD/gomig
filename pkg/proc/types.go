package proc

import "database/sql"

type schema struct {
	database       databaseInfo
	tables         []tableInfo
	tablesMap      map[string]tableInfo  // map[tableName]
	indexInfosMap  map[string]*indexInfo // map[indexName]
	primaryKeysMap map[string][]string // map[tableName]
}

type databaseInfo struct {
	name      string
	user      string
	pass      string
	host      string
	port      string
	charset   string
	collation string
}

type tableInfo struct {
	name       string
	columns    []tableColumn
	columnsMap map[string]tableColumn
}

type tableColumn struct {
	name         string
	columnType   string
	size         string
	unsigned     bool
	autoInc      bool
	null         bool
	defaultValue defaultDetail
}

type defaultDetail struct {
	need  bool
	value string
}

type Queries struct {
	CreateTables  []string
	AddColumns    []string
	ModifyColumns []string
	DropTables    []string
	DropColumns   []string
	AddIndexes    []string
	DropIndexes   []string
}

type descColumns struct {
	field        string
	columnType   string
	null         string
	key          string
	defaultValue sql.NullString
	extra        string
}

type indexInfo struct {
	tableName string
	unique    bool
	indexName string
	columns   []string
}
