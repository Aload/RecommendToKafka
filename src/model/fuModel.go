package model

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strings"
	"sync"
	"kafka"
)

const (
	DB_BA       = "ba"
	BA_PARAMTER = "root:hadoop@tcp(192.168.236.11:3306)/ba?charset=utf8"
	DB_FU       = "fu"
	FU_PARAMTER = "root:hadoop@tcp(192.168.236.11:3306)/fu?charset=utf8"
	FILE_PATH_1 = "E:/DataSource/data1/data"
	FILE_PATH_2 = "E:/DataSource/data2/data"
)

//into parameter
func QueryData(wait sync.WaitGroup, table_name string, code int, parameter string) {
	wait.Add(1)
	defer wait.Done()
	db, err := sql.Open("mysql", parameter)
	fmt.Println(db)
	if err != nil {
		panic(err)
		return
	}
	defer db.Close()
	rows, _ := db.Query("SELECT * FROM " + table_name)
	columns, err := rows.Columns()
	if err != nil {
		panic(err.Error())
	}
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error())
		}
		var value string
		var kafkaValues = ""
		if code == 1 {
			kafkaValues = DB_BA
		} else {
			kafkaValues = DB_FU
		}
		for _, col := range values {
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			kafkaValues = strings.Join([]string{kafkaValues, value}, ",")
		}
		fmt.Println(kafkaValues + "-----------------------------------")
		kafka.SendMsgIntoKafka(wait, "", kafkaValues)
	}
	if err = rows.Err(); err != nil {
		panic(err.Error())
	}

}
