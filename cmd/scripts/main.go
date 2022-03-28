package main

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	defaultMaxOpenConns = 16
	defaultMaxIdleConns = 16
	defaultAddr         = "127.0.0.1"
	defaultPort         = "4000"
	defaultDatabase     = "test"
)

// 定义一个全局对象db
var db *sql.DB

// 定义一个初始化数据库的函数
func initDB(addr, port, database string, openConns, idleConns int) (err error) {
	// DSN:Data Source Name
	dsn := fmt.Sprintf("root@tcp(%v:%v)/%v", addr, port, database)
	// 不会校验账号密码是否正确
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return errors.Wrap(err, "")
	}
	db.SetMaxOpenConns(openConns)
	db.SetMaxIdleConns(idleConns)
	// 尝试与数据库建立连接（校验dsn是否正确）
	for i := 0; i < defaultMaxIdleConns; i++ {
		err = db.Ping()
		if err != nil {
			return errors.Wrap(err, "")
		}
	}
	return nil
}

// 本地测试
func initDefault() {
	err := initDB(defaultAddr, defaultPort, defaultDatabase,
		defaultMaxOpenConns, defaultMaxIdleConns)
	if err != nil {
		log.Panic("init db failed, err:%v\n", zap.Error(err))
		return
	}
	fmt.Println("init db succ")

	countRows(db, "sbtest1")
}

func getMinMaxID(db *sql.DB, table, mod string) int {
	sqlStr := fmt.Sprintf("SELECT MAX(id) FROM %v", table)
	if mod == "min" {
		sqlStr = fmt.Sprintf("SELECT MIN(id) FROM %v", table)
	}
	var cnt int
	err := db.QueryRow(sqlStr).Scan(&cnt)
	if err != nil {
		log.Panic("scan failed, err:%v\n", zap.Error(err))
	}
	return cnt
}

// 统计数据总行数
func countRows(db *sql.DB, table string) {
	sqlStr := fmt.Sprintf("select count(*) from %v", table)
	var cnt int
	err := db.QueryRow(sqlStr).Scan(&cnt)
	if err != nil {
		log.Error("scan failed, err:%v\n", zap.Error(err))
		return
	}
	fmt.Printf("Rows of %v: %v\n", table, cnt)

	// s := ""
	// err = db.QueryRow(fmt.Sprintf("show index from %v;", table)).Scan(&s)
	// if err != nil {
	// 	log.Error("scan failed, err:%v\n", zap.Error(err))
	// 	return
	// }
	// fmt.Printf("Index of %v: %v\n", table, s)
}

func initUpstream(database string) {
	err := initDB("172.16.102.51", defaultPort, database,
		defaultMaxOpenConns, defaultMaxIdleConns)
	if err != nil {
		log.Error("init upstream failed, err:%v\n", zap.Error(err))
		return
	}
	fmt.Println("init upstream succ")

	_, _ = db.Exec("ALTER TABLE sbtest1 DROP INDEX k_1;")
	_, _ = db.Exec("ALTER TABLE sbtest2 DROP INDEX k_1;")
	_, _ = db.Exec("TRUNCATE TABLE sbtest2;")

	fmt.Println("======Databases Info=======")
	fmt.Println("++++++++++sbtest1++++++++++")
	countRows(db, "sbtest1")
	fmt.Println("\n++++++++++sbtest2++++++++++")
	countRows(db, "sbtest2")
	fmt.Println("===========================")
	fmt.Println()
}

func execInsert(concurrency int) {
	sqlStr := "INSERT INTO sbtest2 SELECT * FROM sbtest1 WHERE id>=? AND id<?;"
	min, max := getMinMaxID(db, "sbtest1", "min"), getMinMaxID(db, "sbtest1", "max")

	batch := make([]int, concurrency+1)
	batch[0] = min
	for i, r := 1, (max-min)/concurrency; i <= concurrency; i++ {
		if i == concurrency {
			batch[i] = max + 1
		} else {
			batch[i] = batch[i-1] + r
		}
	}
	time.Sleep(time.Second * 3)

	// Exec Insert
	wg := sync.WaitGroup{}
	start := time.Now()
	for i := 1; i <= concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			log.Info("Exec sql: " + sqlStr + " Args: " +
				fmt.Sprintf("%v, %v", batch[i-1], batch[i]))

			_, err := db.Exec(sqlStr, batch[i-1], batch[i])
			if err != nil {
				log.Panic("", zap.Error(err))
			}
		}(i)
	}
	wg.Wait()
	log.Info("Total exec time: " + time.Since(start).String())
}

func close() {
	db.Close()
}

func batchExecInsert(batchNum int) {
	sqlStr := "INSERT INTO sbtest2 SELECT * FROM sbtest1 WHERE id>=? AND id<?;"
	min, max := getMinMaxID(db, "sbtest1", "min"), getMinMaxID(db, "sbtest1", "max")

	batch := make([]int, batchNum+1)
	batch[0] = min
	for i, r := 1, (max-min)/batchNum; i <= batchNum; i++ {
		if i == batchNum {
			batch[i] = max + 1
		} else {
			batch[i] = batch[i-1] + r
		}
	}

	_, _ = db.Exec("TRUNCATE TABLE sbtest2;")
	time.Sleep(time.Second * 3)

	// Exec Insert
	for i := 1; i <= batchNum; i++ {
		log.Info("Exec sql: " + sqlStr + " Args: " +
			fmt.Sprintf("%v, %v", batch[i-1], batch[i]))

		res, err := db.Exec(sqlStr, batch[i-1], batch[i])
		log.Info(fmt.Sprintln(res))
		if err != nil {
			log.Panic("", zap.Error(err))
		}
	}
}
