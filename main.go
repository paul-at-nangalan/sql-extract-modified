package main

import (
	"encoding/csv"
	"flag"
	"github.com/paul-at-nangalan/db-util/connect"
	"github.com/paul-at-nangalan/errorhandler/handlers"
	"github.com/paul-at-nangalan/json-config/cfg"
	"os"
	"sql-extract-modified/extractor"
)

func main(){
	defer handlers.HandlePanic()

	cfgdir := ""
	lastreadtable := ""
	lastmodfield := ""
	tablename := ""
	sqlqry := ""
	goobeforesubqry := ""
	outputfile := ""

	flag.StringVar(&cfgdir, "cfg", "./cfg", "The config directory")
	flag.StringVar(&lastreadtable, "last-read-table", "last_read",
		"The table to store the last value of last modified")
	flag.StringVar(&lastmodfield, "last-mod-field", "lastmod",
		"The field that holds the last modified value in the main data table")
	flag.StringVar(&tablename, "data-table-name", "",
		"The table that contains the actual data")
	flag.StringVar(&sqlqry, "query", "",
		"The query to use to query the data. $1 must be the last modified value")
	flag.StringVar(&outputfile, "outfile", "",
		"The output file name")
	flag.StringVar(&goobeforesubqry, "good-before-sub-qry", "",
		"[optional] If there is a good before marker, to prevent data being read that maybe only partially written, e.g. " +
		" SELECT good_before FROM data_status WHERE tablename=my_data_table")
	flag.Parse()

	cfg.Setup(cfgdir)

	db := connect.Connect()
	extproc := extractor.NewExtractor(db, sqlqry, lastmodfield, lastreadtable,
		tablename, goobeforesubqry)

	f, err := os.Create(outputfile)
	handlers.PanicOnError(err)

	csvwriter := csv.NewWriter(f)

	extproc.Extract(csvwriter)
}
