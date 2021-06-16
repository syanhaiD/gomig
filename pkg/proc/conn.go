package proc

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

var dbConn *sql.DB

func connect(dbInfo DatabaseInfo) {
	dsn := fmt.Sprintf(
		`%v:%v@tcp(%v:%v)/%v?parseTime=true&charset=%v&collation=%v`,
		dbInfo.User,
		dbInfo.Pass,
		dbInfo.Host,
		dbInfo.Port,
		dbInfo.Name,
		dbInfo.Charset,
		dbInfo.Collation,
	)

	var err error
	dbConn, err = sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
}
