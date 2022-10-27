# gomig

もともとローカル開発用のテキトーなDB(mariadb) migration tool  
btree以外とかカラムごとのcollationとかそういうのには非対応  
外部キーは気が向いたらやる

DB接続でエラった場合panicします

カラムの存在チェックとかもしてないので記載にミスがあった場合割と容赦なくSQLエラーなります  
PRIMARYキーの付替えは現状できません  

mysql8系とmariadbのみ対応

## usage(executable)
toml_pathは必須  
-sql_onlyつけるとクエリ実行せずにSQLを標準出力に吐き捨てます

```
./gomig toml_path="" -sql_only
```

## usage(library)
pkg/procインポートしてExec実行すれば良いです  
tomlPath(string)とsql_only(bool)を渡してあげてください

## toml
charsetとcollationは指定しない場合utf8mb4とutf8mb4_general_ciになります

有効なオプションは

name(require)  
type(require)  
size(optional)  
unsigned(optional)  
autoinc(optional)  
null(optional  
default(optional)  

uniqはunique_indexで指定すること  
カラム名をカンマ区切りの文字列で指定すると複合indexになります

auto_inc指定すると内部で自動で単一のprimary keyにしちゃいます  

primary keyが指定されていないテーブルでunique_index指定されていてかつnot nullが指定されているカラムがある場合エラーとしています  
primaryに設定するか、別にprimary keyを設定してください

```
[database]
name = "test"
user = "root"
pass = ""
host = "127.0.0.1"
port = "3306"
charset = "utf8mb4"
collation = "utf8mb4_general_ci"

[[tables]]
name = "exmaple"
columns = [
  {name = "id", type = "int", unsigned = true, null = false, autoinc = true},
  {name = "sub_id", type = "int", unsigned = true},
  {name = "name", type = "varchar", size = "255", null = false},
  {name = "age", type = "int", unsigned = true, null = false},
  {name = "birth", type = "int", unsigned = true, null = false, default = "5"},
]
primary = ["id"]
index = ["sub_id","name"]
unique_index = ["age,birth"]
```