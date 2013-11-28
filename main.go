package main

import (
	_ "code.google.com/p/odbc"
	"database/sql"
	"fmt"
	"strconv"
)

//var dbNames = [11]string{"QunInfo1", "QunInfo2", "QunInfo3", "QunInfo4", "QunInfo5", "QunInfo6", "QunInfo7", "QunInfo8", "QunInfo9", "QunInfo10", "QunInfo11"}
//var dbNames = [11]string{"GroupData1", "GroupData2", "GroupData3", "GroupData4", "GroupData5", "GroupData6", "GroupData7", "GroupData8", "GroupData9", "GroupData10", "GroupData11"}
var dbNames = [1]string{"GroupData11"}

func main() {
	for _, dbName := range dbNames {

		conn, err := db(dbName)

		if err != nil {
			fmt.Println("Connecting Error")
			return
		}

		names := getTableNames(conn)
		//getTableNames(conn)

		//f, err := os.OpenFile("GroupData1.csv", os.O_RDWR|os.O_CREATE, 0666) //其实这里的 O_RDWR应该是 O_RDWR|O_CREATE，也就是文件不存在的情况下就建一个空文件，但是因为windows下还有BUG，如果使用这个O_CREATE，就会直接清空文件，所以这里就不用了这个标志，你自己事先建立好文件。
		//if err != nil {
		//	panic(err)
		//}

		//defer f.Close()

		for _, tableName := range names {
			//rowCount := getRowCount(conn, tableName)
			if tableName != "" && tableName != "dtproperties" {
				rowCount := getRowCount(conn, tableName)
				max, min := getMaxMinField(conn, tableName, "QQNum")
				fmt.Println(tableName + "," + strconv.Itoa(rowCount) + "," + strconv.Itoa(max) + "," + strconv.Itoa(min))
			}
		}

		fmt.Printf("%s\n", "finish")
		conn.Close()
	}

	return
}

/**
 * 数据库连接
 */
func db(dbName string) (*sql.DB, error) {
	return sql.Open("odbc", "driver={sql server};server=localhost;port=1433;uid=sa;pwd=liuawen99;database="+dbName)
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
 * 获取所有数据库的名字
 */
func getAllDataBase(conn *sql.DB) []string {
	names := make([]string, 12) //不能用切片，你实际上是想返回100个元素的数组，但是最后会返回200个元素，其中100个元素为空。
	stmt, err := conn.Prepare("select name from sys.databases where name like 'GroupData%'")
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
	//fmt.Println(tableName)
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

/**
 * 获取表某个字段的最大和最小值。
 */
func getMaxMinField(conn *sql.DB, tableName string, fieldName string) (int, int) {
	var maxField, minField int = 0, 0
	row, err := conn.Query("select max(" + fieldName + ") as maxField, min(" + fieldName + ") as minField from " + tableName)
	if err != nil {
		fmt.Println("Query Error", err)
		return maxField, minField
	}
	defer row.Close()
	for row.Next() {
		if err := row.Scan(&maxField, &minField); err == nil {
			//fmt.Println(maxField)
			return maxField, minField
		}
	}
	return maxField, minField
}
