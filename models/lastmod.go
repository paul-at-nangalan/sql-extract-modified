package models

import (
	"database/sql"
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
	getqry := `SELECT ` + lastmodfield + ` FROM ` + lastmodtable +
		` WHERE tablename=` + datatablename

	getstmt, err := db.Prepare(getqry)
	handlers.PanicOnError(err)

	setqry := `INSERT INTO ` + lastmodtable + ` (` + lastmodfield + `)` +
		` VALUES($1) WHERE tablename=` + datatablename +
		` ON CONFICT (tablename) SET ` + lastmodfield + `=` + lastmodfield + `.EXCLUDED`
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