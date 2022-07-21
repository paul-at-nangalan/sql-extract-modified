package extractor

import (
	"database/sql"
	"fmt"
	"github.com/paul-at-nangalan/errorhandler/handlers"
	"log"
	"sql-extract-modified/models"
	"time"
)

/// For testing
type LastModifiedModel interface {
	Get() time.Time
	Set(lastreadtime time.Time)
}

/// Sub in any type of writer
type Writer interface {
	Write([]string)error
}

type Extractor struct{
	lastmodifiedmodel LastModifiedModel
	sqlstmt *sql.Stmt
	batchsize int
	lastmodfield string
	tablename string
	db *sql.DB
}

//// The last mod where clause must always take $1 as the last read time parameter
func NewExtractor(db *sql.DB, qry string, lastmodfield, lastmodtable, tablename string)*Extractor{
	lastmodmodel := models.NewLastModifiedModel(db, lastmodtable, lastmodtable)
	return newExtractor(db, lastmodmodel, qry,lastmodfield, tablename, 500)
}
/////For testing we can inject a mock last modified model
func newExtractor(db *sql.DB, lastmodmodel LastModifiedModel,
	qry string, lastmodfield string, tablename string, limit int)*Extractor {

	fullquery := qry + fmt.Sprintf(` LIMIT %d OFFSET $2`, limit)
	stmt, err := db.Prepare(fullquery)
	handlers.PanicOnError(err)

	return &Extractor{
		lastmodifiedmodel: lastmodmodel,
		sqlstmt: stmt,
		batchsize: limit, ///make sure this matches the statement
		lastmodfield: lastmodfield,
		tablename: tablename,
		db: db,
	}

}

func (p *Extractor)getMaxLastModTime()time.Time{
	res, err := p.db.Query(`SELECT MAX(` + p.lastmodfield + `) FROM ` + p.tablename )
	handlers.PanicOnError(err)
	if !res.Next(){
		log.Panicln("Unable to get a last modified time, no results returned")
	}
	defer res.Close()
	lastmod := time.Time{}
	err = res.Scan(&lastmod)
	handlers.PanicOnError(err)

	return lastmod
}

func (p *Extractor)Extract(writer Writer){
	lastreadtime := p.lastmodifiedmodel.Get()
	maxlastmod := p.getMaxLastModTime() /// only set this if everything completes successfully

	numcols := 0
	havemore := true
	for i := 0; havemore ; i += p.batchsize{
		func() {
			res, err := p.sqlstmt.Query(lastreadtime, i)
			handlers.PanicOnError(err)
			defer res.Close()

			if i == 0{
				////get the header and write it
				cols, err := res.Columns()
				handlers.PanicOnError(err)

				writer.Write(cols)
				numcols = len(cols)
			}
			havemore = false ///if there's nothing in this batch, we've reached the end
			for res.Next(){
				havemore = true

				cols := make([]string, numcols)
				scans := make([]any, numcols)
				for x := 0; x < len(cols); x++{
					scans[x] = &cols[x]
				}

				err := res.Scan(scans...)
				handlers.PanicOnError(err)

				writer.Write(cols)
			}
		}()
	}
	p.lastmodifiedmodel.Set(maxlastmod)
}

