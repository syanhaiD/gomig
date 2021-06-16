package proc

import "database/sql"

type schema struct {
	database      DatabaseInfo
	tables        []tableInfo
	tablesMap     map[string]tableInfo             // map[tableName]
	indexInfosMap map[string]map[string]*indexInfo // map[tableName]map[indexName]
	engine        map[string]string                // map[tableName]engineName tomlからのCreate専用でALTER非対応
}

type DatabaseInfo struct {
	Name      string
	User      string
	Pass      string
	Host      string
	Port      string
	Charset   string
	Collation string
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
	indexType string // 2021-06-15 BTREE or FULLTEXT のみ
	columns   []string
	comment   string // 'tokenizer "TokenBigramSplitSymbolAlphaDigit"' のみ
}
