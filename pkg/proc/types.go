package proc

import "database/sql"

type schema struct {
	database        DatabaseInfo
	tables          []tableInfo
	tablesMap       map[string]tableInfo             // map[tableName]
	indexInfosSlice map[string][]string              // map[tableName][]indexName indexの順番保持用
	indexInfosMap   map[string]map[string]*indexInfo // map[tableName]map[indexName]
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
	partition  partitionInfo
	engine     string
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

// e.g. PARTITION BY partitionType (keyColumn) (PARTITION [[name]][[startNum]]...[[endNum]] VALUES LESS THAN (eachRow))
// endNumのときeachRowはMAXVALUE
type partitionInfo struct {
	partitionType string
	keyColumn     string
	baseName      string
	startNum      string // 2021-06-16 1で一旦固定
	endNum        string
	eachRow       string // 2021-06-16 10000区切りで一旦固定
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
