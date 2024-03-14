package dbs4ruts

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	utils "github.com/ruts48code/utils4ruts"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func OpenDBS(dbs []string) (*sql.DB, string, error) {
	dbN := utils.RandomArrayString(dbs)
	dbConnect := false
	dbtypex := ""
	var db *sql.DB
	var err error
	for i := range dbN {
		dbtype, _, _, host, _, _ := ExtractDBparameter(dbN[i])
		if dbtype == "" {
			return nil, "", errors.New("host uri error")
		}
		dbx := dbN[i]
		switch dbtype {
		case "mysql":
			ex := strings.SplitN(dbN[i], "://", 2)
			dbx = ex[1]
		}
		db, err = sql.Open(dbtype, dbx)
		if err != nil {
			log.Printf("Error: Fail to open db %s:%s - %v\n", dbtype, host, err)
			continue
		}

		err = db.Ping()
		if err != nil {
			log.Printf("Error: Fail to ping db %s:%s - %v\n", dbtype, host, err)
			db.Close()
			continue
		}
		log.Printf("Log: Connect to db %s:%s\n", dbtype, host)
		dbConnect = true
		dbtypex = dbtype
		break
	}
	if !dbConnect {
		log.Printf("Error: Cannot connect to all db\n")
		return nil, "", errors.New("cannot connect to all db")
	}
	return db, dbtypex, nil
}

func Q(dbtype, query string) (output string) {
	output = ""
	count := 1
	for _, char := range query {
		if char == '?' {
			switch dbtype {
			case "mysql":
				output += string(char)
			case "postgres":
				output += fmt.Sprintf("$%d", count)
			case "sqlserver":
				output += fmt.Sprintf("@p%d", count)
			default:
				output += string(char)
			}
			count++
		} else {
			output += string(char)
		}
	}
	return
}

func ExtractDBparameter(h string) (dbtype string, username string, password string, host string, db string, param string) {
	dbtypex := strings.SplitN(h, "://", 2)
	if len(dbtypex) != 2 {
		return
	}
	ex := strings.SplitN(dbtypex[1], "@", 2)
	if len(ex) != 2 {
		return
	}
	dbtype = dbtypex[0]
	u := strings.SplitN(ex[0], ":", 2)
	username = u[0]
	if len(u) == 2 {
		password = u[1]
	}
	path := strings.SplitN(ex[1], "/", 2)
	host = path[0]
	if len(path) == 2 {
		dbx := strings.SplitN(path[1], "?", 2)
		db = dbx[0]
		if len(dbx) == 2 {
			param = dbx[1]
		}
	}
	return
}
