package proc

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

func Exec(tomlPath string, sqlOnly bool) (err error) {
	fromToml, err := parseToml(tomlPath)
	if err != nil {
		return
	}
	connect(fromToml.database)
	defer dbConn.Close()
	fromDB, err := parseDB(fromToml.database.name)
	if err != nil {
		return
	}
	queries := procDiff(fromToml, fromDB)
	if sqlOnly {
		printDDL(queries)
	} else {
		err = execDDL(queries)
		if err != nil {
			return
		}
	}

	return
}

var dbConn *sql.DB

func connect(dbInfo databaseInfo) {
	dsn := fmt.Sprintf(
		`%v:%v@tcp(%v:%v)/%v?parseTime=true&charset=%v&collation=%v`,
		dbInfo.user,
		dbInfo.pass,
		dbInfo.host,
		dbInfo.port,
		dbInfo.name,
		dbInfo.charset,
		dbInfo.collation,
	)

	var err error
	dbConn, err = sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
}

func execDDL(queries *Queries) (err error) {
	for _, query := range queries.DropTables {
		_, err = dbConn.Exec(query)
		if err != nil {
			return
		}
	}
	for _, query := range queries.CreateTables {
		_, err = dbConn.Exec(query)
		if err != nil {
			return
		}
	}
	for _, query := range queries.DropColumns {
		_, err = dbConn.Exec(query)
		if err != nil {
			return
		}
	}
	for _, query := range queries.AddColumns {
		_, err = dbConn.Exec(query)
		if err != nil {
			return
		}
	}
	for _, query := range queries.ModifyColumns {
		_, err = dbConn.Exec(query)
		if err != nil {
			return
		}
	}
	for _, query := range queries.DropIndexes {
		_, err = dbConn.Exec(query)
		if err != nil {
			return
		}
	}
	for _, query := range queries.AddIndexes {
		_, err = dbConn.Exec(query)
		if err != nil {
			return
		}
	}

	return
}

func printDDL(queries *Queries) {
	for _, query := range queries.DropTables {
		fmt.Println(query)
	}
	for _, query := range queries.CreateTables {
		fmt.Println(query)
	}
	for _, query := range queries.DropColumns {
		fmt.Println(query)
	}
	for _, query := range queries.AddColumns {
		fmt.Println(query)
	}
	for _, query := range queries.ModifyColumns {
		fmt.Println(query)
	}
	for _, query := range queries.DropIndexes {
		fmt.Println(query)
	}
	for _, query := range queries.AddIndexes {
		fmt.Println(query)
	}
}
