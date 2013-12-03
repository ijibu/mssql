package main

import (
	_ "code.google.com/p/odbc"
	"database/sql"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
)

var QunNum *int = flag.Int("q", 0, "please input a QunNum like 69699987")
var QQNum *int = flag.Int("i", 0, "please input a QQNum like 43546936")

func main() {
	flag.Usage = show_usage
	flag.Parse()

	if *QunNum == 0 && *QQNum == 0 {
		show_usage()
		return
	}
	//fmt.Println(*QunNum)
	//fmt.Println(*QQNum)
	//return
	if *QunNum != 0 {
		dbName, tableName := getDbAndTable(*QunNum)

		conn, err := db(dbName)

		if err != nil {
			fmt.Println("Connecting Error")
			return
		}

		queryQunNum(conn, tableName, *QunNum)
		fmt.Printf("%s\n", "finish")
		conn.Close()
	}

	if *QQNum != 0 {
		conn, _ := db("GroupData1")
		dbNames := getAllDataBase(conn)

		for _, dbName := range dbNames {
			if dbName == "" {
				continue
			}
			fmt.Println("searching database:		" + dbName)
			conn, err := db(dbName)

			if err != nil {
				fmt.Println("Connecting Error")
				return
			}

			names := getTableNames(conn)

			for _, tableName := range names {
				if tableName != "" && tableName != "dtproperties" {
					fmt.Println("searching table:		" + tableName)
					queryQQNum(conn, tableName, *QQNum)
				}
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
 * 查询某个QQ加了那些群
 */
func queryQQNum(conn *sql.DB, tableName string, QQNum int) {
	rows, err := conn.Query("select * from " + tableName + " where QQNum=" + strconv.Itoa(QQNum))
	if err != nil {
		fmt.Println("Query Error", err)
		return
	}
	defer rows.Close()

	var (
		id     int
		qq     int
		nick   string
		age    int
		gender int
		auth   int
		qun    int
	)

	for rows.Next() {
		if err := rows.Scan(&id, &qq, &nick, &age, &gender, &auth, &qun); err == nil {
			fmt.Print(id)
			fmt.Print("\t")
			fmt.Print(qq)
			fmt.Print("\t")
			fmt.Print(nick)
			fmt.Print("\t")
			fmt.Print(age)
			fmt.Print("\t")
			fmt.Print(gender)
			fmt.Print("\t")
			fmt.Print(auth)
			fmt.Print("\t")
			fmt.Print(qun)
			fmt.Print("\t\r\n")
		}
	}

	return
}

/**
 * 查询某个QQ群的成员
 */
func queryQunNum(conn *sql.DB, tableName string, QunNum int) {
	rows, err := conn.Query("select * from " + tableName + " where QunNum=" + strconv.Itoa(QunNum))
	if err != nil {
		fmt.Println("Query Error", err)
		return
	}
	defer rows.Close()

	fmt.Println("")
	cols, _ := rows.Columns()
	for i := range cols {
		fmt.Print(cols[i])
		fmt.Print("\t")
	}
	fmt.Println("")

	var (
		id     int
		qq     int
		nick   string
		age    int
		gender int
		auth   int
		qun    int
	)

	for rows.Next() {
		if err := rows.Scan(&id, &qq, &nick, &age, &gender, &auth, &qun); err == nil {
			fmt.Print(id)
			fmt.Print("\t")
			fmt.Print(qq)
			fmt.Print("\t")
			fmt.Print(nick)
			fmt.Print("\t")
			fmt.Print(age)
			fmt.Print("\t")
			fmt.Print(gender)
			fmt.Print("\t")
			fmt.Print(auth)
			fmt.Print("\t")
			fmt.Print(qun)
			fmt.Print("\t\r\n")
		}
	}

	return
}

/**
 * 根据QQ群号码获取所在的数据库和表
 */
func getDbAndTable(QunNum int) (string, string) {
	var dbStep float64 = 10000000
	var tableStep float64 = 100000
	QunNum1 := float64(QunNum)

	dbIndex := math.Ceil(QunNum1 / dbStep)       //向上取整
	tableIndex := math.Ceil(QunNum1 / tableStep) //向上取整

	dbName, tableName := "GroupData"+strconv.Itoa(int(dbIndex)), "Group"+strconv.Itoa(int(tableIndex))
	return dbName, tableName
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

func show_usage() {
	fmt.Fprintf(os.Stderr,
		"Usage: %s [-q=<QunNum>]\n"+" [-i=<QQNum>]\n"+
			"       <command> [<args>]\n\n",
		os.Args[0])
	fmt.Fprintf(os.Stderr,
		"Flags:\n")
	flag.PrintDefaults()
}
