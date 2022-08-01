package models

import (
	"database/sql"
	"fmt"
	"github.com/paul-at-nangalan/db-util/migrator"
	"log"
	"time"
)
import "github.com/paul-at-nangalan/errorhandler/handlers"

type LastModifiedModel struct{
	get *sql.Stmt
	update *sql.Stmt
}

func NewLastModifiedModel(db *sql.DB, lastmodtable, datatablename string)*LastModifiedModel{
	lastmodfield := "lastread"
	tablenamefield := "tablename"
	mig := migrator.NewMigrator(db, migrator.DBTYPE_POSTGRES)
	cols := map[string]string{
		lastmodfield: "timestamp",
		tablenamefield: "text",
	}
	indx := []string{tablenamefield}
	primekey := []string{tablenamefield}
	mig.Migrate("create-last-mod-table", lastmodtable, cols, indx, primekey)

	getqry := `SELECT ` + lastmodfield + ` FROM ` + lastmodtable +
		` WHERE tablename='` + datatablename + `'`
	fmt.Println("Qry for last read ", getqry)
	getstmt, err := db.Prepare(getqry)
	handlers.PanicOnError(err)

	setqry := `INSERT INTO ` + lastmodtable + ` (tablename, ` + lastmodfield + `)` +
		` VALUES('` + datatablename + `', $1)`  +
		` ON CONFLICT (tablename) DO UPDATE SET ` + lastmodfield + `=EXCLUDED.` + lastmodfield
	fmt.Println("Set last mod ", setqry)
	setstmt, err := db.Prepare(setqry)
	handlers.PanicOnError(err)

	return &LastModifiedModel{
		get: getstmt,
		update: setstmt,
	}
}

func (p *LastModifiedModel)Get()time.Time{
	res, err := p.get.Query()
	handlers.PanicOnError(err)
	defer res.Close()

	if !res.Next(){
		///First time, return a zero time
		return time.Time{}
	}
	var lastmodtime sql.NullTime
	err = res.Scan(&lastmodtime)
	handlers.PanicOnError(err)

	if !lastmodtime.Valid{
		///How've we got a nil time....
		log.Panicln("Nil time returned for last read time")
	}
	return lastmodtime.Time
}

func (p *LastModifiedModel)Set(lastreadtime time.Time){
	_, err := p.update.Exec(lastreadtime)
	handlers.PanicOnError(err)
}