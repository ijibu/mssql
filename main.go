package main

import (
	_ "code.google.com/p/odbc"
	"database/sql"
	"fmt"
)

func main() {
	//conn, err := sql.Open("odbc", "driver={sql server};server=192.168.1.12;port=1433;uid=sa;pwd=password;database=QunInfo11")
	conn, err := sql.Open("odbc", "driver={sql server};server=localhost;port=1433;uid=sa;pwd=password;database=QunInfo11")

	if err != nil {
		fmt.Println("Connecting Error")
		return
	}
	defer conn.Close()

	names := getTableNames(conn)
	//getTableNames(conn)

	//f, err := os.OpenFile("GroupData1.csv", os.O_RDWR|os.O_CREATE, 0666) //其实这里的 O_RDWR应该是 O_RDWR|O_CREATE，也就是文件不存在的情况下就建一个空文件，但是因为windows下还有BUG，如果使用这个O_CREATE，就会直接清空文件，所以这里就不用了这个标志，你自己事先建立好文件。
	//if err != nil {
	//	panic(err)
	//}

	//defer f.Close()

	for _, tableName := range names {
		//rowCount := getRowCount(conn, tableName)
		if tableName != "" {
			getRowCount(conn, tableName)
		}
	}

	fmt.Printf("%s\n", "finish")
	return
}

/**
 * 获取数据库所有的表
 */
func getTableNames(conn *sql.DB) []string {
	names := make([]string, 101) //不能用切片，你实际上是想返回100个元素的数组，但是最后会返回200个元素，其中100个元素为空。
	stmt, err := conn.Prepare("select name from sys.objects where type='U'")
	if err != nil {
		fmt.Println("Query Error", err)
		return names
	}
	defer stmt.Close()
	row, err := stmt.Query()
	if err != nil {
		fmt.Println("Query Error", err)
		return names
	}
	defer row.Close()
	for row.Next() {
		var name string
		if err := row.Scan(&name); err == nil {
			//fmt.Println(name)
			names = append(names, name)
		}
	}
	return names
}

/**
 * 获取表的记录数
 */
func getRowCount(conn *sql.DB, tableName string) int {
	fmt.Println(tableName)
	var cnt int = 0
	row, err := conn.Query("select count(*) as cnt from " + tableName)
	if err != nil {
		fmt.Println("Query Error", err)
		return cnt
	}
	defer row.Close()
	for row.Next() {
		if err := row.Scan(&cnt); err == nil {
			//fmt.Println(cnt)
			return cnt
		}
	}
	return cnt
}
