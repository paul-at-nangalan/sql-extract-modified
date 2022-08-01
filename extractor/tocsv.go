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
	Flush()
}

type Extractor struct{
	lastmodifiedmodel LastModifiedModel
	sqlstmt *sql.Stmt
	batchsize int
	lastmodfield string
	tablename string

	goodbeforequery string
	db *sql.DB
}

//// The last mod where clause must always take $1 as the last read time parameter
func NewExtractor(db *sql.DB, qry string, lastmodfield, lastmodtable, tablename, goodbeforeqry string)*Extractor{
	lastmodmodel := models.NewLastModifiedModel(db, lastmodtable, tablename)
	return newExtractor(db, lastmodmodel, qry,lastmodfield, tablename, goodbeforeqry, 500)
}
/////For testing we can inject a mock last modified model
func newExtractor(db *sql.DB, lastmodmodel LastModifiedModel,
	qry string, lastmodfield string, tablename, goodbeforequery string, limit int)*Extractor {

	fullquery := qry + fmt.Sprintf(` LIMIT %d OFFSET $2`, limit)
	fmt.Println("Extract qry ", fullquery)
	stmt, err := db.Prepare(fullquery)
	handlers.PanicOnError(err)

	return &Extractor{
		lastmodifiedmodel: lastmodmodel,
		sqlstmt: stmt,
		batchsize: limit, ///make sure this matches the statement
		lastmodfield: lastmodfield,
		tablename: tablename,
		db: db,
		goodbeforequery: goodbeforequery,
	}

}

func (p *Extractor)getMaxLastModTime(goodbefore time.Time)time.Time{
	params := make([]any, 0)

	qry := `SELECT MAX(` + p.lastmodfield + `) FROM ` + p.tablename
	if p.goodbeforequery != ""{
		qry += ` WHERE ` + p.lastmodfield + `< $1`
		params = append(params, goodbefore)
	}
	res, err := p.db.Query(qry, params...)
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

func (p *Extractor)getGoodBefore()time.Time{
	//// Because we use limit and offset, we should get the good before time upfront
	///  to avoid some change to the good before time resulting in a change to the result set
	goodbefore := time.Time{}
	if p.goodbeforequery != ""{
		res, err := p.db.Query(p.goodbeforequery)
		handlers.PanicOnError(err)
		defer res.Close()
		if !res.Next(){
			log.Panicln("No data from the good before query ", p.goodbeforequery)
		}
		err = res.Scan(&goodbefore)
		handlers.PanicOnError(err)
	}
	return goodbefore
}

func (p *Extractor)Extract(writer Writer){
	lastreadtime := p.lastmodifiedmodel.Get()
	goodbefore := p.getGoodBefore()
	maxlastmod := p.getMaxLastModTime(goodbefore) /// only set this if everything completes successfully

	numcols := 0
	havemore := true
	var res *sql.Rows
	var err error
	for i := 0; havemore ; i += p.batchsize{
		func() {
			if p.goodbeforequery != ""{
				res, err = p.sqlstmt.Query(lastreadtime, i, goodbefore)
			}else {
				res, err = p.sqlstmt.Query(lastreadtime, i)
			}
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
				writer.Flush()
			}
		}()
	}
	p.lastmodifiedmodel.Set(maxlastmod)
}

