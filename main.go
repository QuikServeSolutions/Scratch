package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	//	"fmt"
	"io/ioutil"
	"log"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Wise struct {
	wisetailid         string
	user_id            string
	content_id         string
	program_session_id string
	created            string
	created_by         string
	last_name          string
	first_name         string
	title              string
	Store              string
	employee_id        string
	points             string
}

func main() {
	//db, err := sql.Open("mysql", "user:password@/dbname")
	db, err := sql.Open("mysql", "tj_remote:1234@tcp(104.130.251.178)/tacojohns")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT distinct t4.id, t1.id as user_id, t4.content_id, t4.program_session_id, cast(max(t4.created) as date) as created , t4.created_by, t1.last_name, t1.first_name, t5.title, t3.code as Store, t6.employee_id as employee_id, sum(t1.points) as points
		FROM tacojohns.tb_eco_v_people t1 
		join tacojohns.user_profiles t2 on t1.id=t2.user_id
		join tacojohns.user_profile_field_values t3 on t2.value_id=t3.id 
		left outer join tacojohns.user_completed_content t4 on t1.id=t4.user_id
		left outer join tacojohns.content t5 on t4.content_id=t5.id
		join tacojohns.tb_eco_users t6 on t1.id=t6.id
		WHERE t3.key in ('new_location') 
		GROUP BY t4.id, t1.id, t4.content_id, t4.program_session_id, t4.created_by, t1.last_name, t1.first_name, t5.title, t3.code, t6.employee_id
		`)

	if err != nil {
		panic(err.Error())
	}

	columns, err := rows.Columns()
	if err != nil {
		panic(err.Error())
	}

	count := len(columns)
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	for rows.Next() {
		for i := 0; i < count; i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		tableData = append(tableData, entry)
	}
	jsonData, err := json.Marshal(tableData)
	if err != nil {
		log.Println(err)
	}

	ioutil.WriteFile("wisetail.json", jsonData, 0644)
	//fmt.Println(string(jsonData))
	PostIt(jsonData)
}

//PostIt : PostIt is a function that accepts a JSON string and passes it as an arguement to SQL Sever import procedure.
func PostIt(jx []byte) {

	//fmt.Println(string(jx))

	wise := []Wise{}

	jid := string(jx)
	sql := `exec crm.dbo.import_wisetail $1`
	err := DB().Select(&wise, sql, jid)

	if err != nil {
		log.Println(err)
	}
}

//DB : DB is a function that connects to SQL server.
func DB() *sqlx.DB {
	serv := os.Getenv("DB_SERVER")
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASS")
	database := os.Getenv("DB_DATABASE")

	db, err := sqlx.Connect("mssql", fmt.Sprintf(`server=%s;user id=%s;password=%s;database=%s;log64;encrypt=disable`, serv, user, pass, database))

	if err != nil {
		log.Println(err)
	}
	return db
}

/*ctrl + ~
cd c:\work\src\github.com\terryberlin\wisetail
go run main.go
go install to create wisetail executable in ..\bin\ folder
*/
