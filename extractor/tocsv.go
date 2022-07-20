package extractor

import (
	"database/sql"
	"github.com/paul-at-nangalan/errorhandler/handlers"
	"sql-extract-modified/models"
)

type Writer interface {
	Write([]string)error
}

type Extractor struct{
	lastmodifiedmodel *models.LastModifiedModel
	sqlstmt *sql.Stmt
	batchsize int
	lastmodfield string
}

//// The last mod where clause must always take $1 as the parameter
func NewExtractor(db *sql.DB, qry string, lastmodfield, lastmodtable, tablename string)*Extractor{
	lastmodmodel := models.NewLastModifiedModel(db, lastmodtable, lastmodtable)

	fullquery := qry + ` LIMIT 500 OFFSET $2`
	stmt, err := db.Prepare(fullquery)
	handlers.PanicOnError(err)

	return &Extractor{
		lastmodifiedmodel: lastmodmodel,
		sqlstmt: stmt,
		batchsize: 500, ///make sure this matches the statement
		lastmodfield: lastmodfield,
	}
}

func (p *Extractor)Extract(writer Writer){
	lastreadtime := p.lastmodifiedmodel.Get()
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
}
