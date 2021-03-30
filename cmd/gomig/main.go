package main

import (
	"flag"
	"fmt"
	"github.com/syanhaiD/gomig/pkg/proc"
	"os"
)

func main() {
	var tomlPath = flag.String("toml_path", "", "Path to the toml file containing the table definitions")
	var sqlOnly = flag.Bool("sql_only", false, "Output SQL without executing ddl.")
	var testEnv = flag.Bool("test", false, "If this is true, it will read the database_test settings.")
	flag.Parse()

	if *tomlPath == "" {
		fmt.Println("ERROR: toml_path is required")
		os.Exit(0)
	}

	err := proc.Exec(*tomlPath, *sqlOnly, *testEnv)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	os.Exit(0)
}
