package extractor

import (
	"database/sql"
	"flag"
	"github.com/paul-at-nangalan/db-util/connect"
	"github.com/paul-at-nangalan/db-util/migrator"
	"github.com/paul-at-nangalan/errorhandler/handlers"
	"github.com/paul-at-nangalan/json-config/cfg"
	"os"
	"testing"
	"time"
)

type TestStuff struct{
	db *sql.DB
	testtablename string
	testdataindex int
}
var teststuff TestStuff

func setup(){
	teststuff.db = connect.Connect()
	teststuff.testtablename = "test_extractor"

	///create a test table in the DB - this should be a staging or test DB
	cols := map[string]string{
		"field1": "text",
		"field2": "text",
		"field3": "text",
		"lastmod": "timestamp",
	}
	primes := []string{"field1"}
	indexs := []string{"field1"}
	mig := migrator.NewMigrator(teststuff.db, migrator.DBTYPE_POSTGRES)
	mig.Migrate("create-test-table", teststuff.testtablename, cols, indexs, primes)

	_, err := teststuff.db.Exec(`DELETE FROM ` + teststuff.testtablename)
	handlers.PanicOnError(err)

}

func getDataByOffset(testdata string, index int, offset int)(data string, newindex int){
	start := index % len(testdata)
	end := (start + offset) % len(testdata)
	data = testdata[start: end]
	return data, end
}

func fillTestTable(mockwriter *MockWriter){
	testdata := `qwertyuiop[]asdfghjkl;'zxcvbnm,./QWERTYUIOP{}ASDFGHJKL:"ZXCVBNM<>?.`
	testdataindex := teststuff.testdataindex
	offsets := []int{6, 5, 10}
	t := time.Now().Add(-1153 * time.Second)

	stmt, err := teststuff.db.Prepare(`INSERT INTO ` + teststuff.testtablename +
		` (field1. field2, field3, lastmod) VALUES ($1, $2, $3, $4)`)
	handlers.PanicOnError(err)

	////put the header into mock writer
	mockwriter.expect = append(mockwriter.expect, []string{"field1", "field2", "field3"})

	data := make([]string, len(offsets))
	for i := 0; i < 1153; i++{
		expect := make([]string, len(offsets))
		for x := 0; x < len(offsets); x++{
			data[x], testdataindex = getDataByOffset(testdata, testdataindex, offsets[x])
			expect[x] = data[x]
		}
		_, err := stmt.Exec(data[0], data[1], data[2], t)
		handlers.PanicOnError(err)
		t.Add(time.Second)
		mockwriter.expect = append(mockwriter.expect, expect)
	}
	teststuff.testdataindex = testdataindex
}

func Test_Extractor(t *testing.T){
	mocklastmod := &MockLastmodmodel{}
	mockwriter := NewMockWriter(t)

	qry := `SELECT field1, field2, field3 FROM ` + teststuff.testtablename +
		`WHERE lastmod >= $1`
	extractor := newExtractor(teststuff.db, mocklastmod, qry, "lastmod")

	fillTestTable(mockwriter)
	extractor.Extract(mockwriter)

	///there should be nothing left in the expect list
	if len(mockwriter.expect) > 0{
		t.Error("Have unread data, num lines ", len(mockwriter.expect))
	}

	////clear out the old stuff
	mockwriter = NewMockWriter(t)
	fillTestTable(mockwriter)
	extractor.Extract(mockwriter)

}

func TestMain(m *testing.M) {

	cfgdir := ""
	flag.StringVar(&cfgdir, "cfg", "../ut-cfg", "The cfg directory")
	flag.Parse()
	cfg.Setup(cfgdir)
	setup()

	os.Exit(m.Run())
}
