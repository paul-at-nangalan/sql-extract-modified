package extractor

import (
	"database/sql"
	"flag"
	"fmt"
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
	limit int
	t time.Time
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

	///create a test-good-before table
	cols = map[string]string{
		"name": "text",
		"time": "timestamp",
	}
	primes = []string{"name"}
	indexs = []string{"name"}
	mig.Migrate("create-test-good-before-table", "test_good_before",
		cols, indexs, primes)
	teststuff.limit = 23

	_, err := teststuff.db.Exec(`DELETE FROM ` + teststuff.testtablename)
	handlers.PanicOnError(err)

	t := time.Now().Add(time.Duration(-3 * teststuff.limit) * time.Second)
	teststuff.t = t
}

func getDataByOffset(testdata string, index int, offset int)(data string, newindex int){
	start := index % len(testdata)
	end := (start + offset) % len(testdata)
	if end > start {
		data = testdata[start:end]
	}else{
		data = testdata[start : ]
	}
	return data, end
}

func fillTestTable(mockwriter *MockWriter, inchdr bool){
	testdata := `qwertyuiop[]asdfghjkl;'zxcvbnm,./QWERTYUIOP{}ASDFGHJKL:"ZXCVBNM<>?.`
	testdataindex := teststuff.testdataindex
	offsets := []int{6, 5, 10}

	stmt, err := teststuff.db.Prepare(`INSERT INTO ` + teststuff.testtablename +
		` (field1, field2, field3, lastmod) VALUES ($1, $2, $3, $4)`)
	handlers.PanicOnError(err)

	////put the header into mock writer
	mockwriter.expect = append(mockwriter.expect, []string{"field1", "field2", "field3"})

	data := make([]string, len(offsets))
	for i := 0; i < (teststuff.limit + 7) * 2 ; i++{
		expect := make([]string, len(offsets))
		for x := 0; x < len(offsets); x++{
			data[x], testdataindex = getDataByOffset(testdata, testdataindex, offsets[x])
			if x == 0{
				data[x] += fmt.Sprintf("%d", i) //make sure it's unique
			}
			expect[x] = data[x]
		}
		_, err := stmt.Exec(data[0], data[1], data[2], teststuff.t)
		handlers.PanicOnError(err)
		teststuff.t = teststuff.t.Add(time.Second)
		mockwriter.expect = append(mockwriter.expect, expect)
	}
	teststuff.testdataindex = testdataindex
}

func Test_Extractor(t *testing.T){
	mocklastmod := &MockLastmodmodel{}
	mockwriter := NewMockWriter(t)

	qry := `SELECT field1, field2, field3 FROM ` + teststuff.testtablename +
		` WHERE lastmod > $1 
		 ORDER BY lastmod`
	extractor := newExtractor(teststuff.db, mocklastmod, qry, "lastmod",
		teststuff.testtablename, "", teststuff.limit)

	fillTestTable(mockwriter, true)
	extractor.Extract(mockwriter)

	///there should be nothing left in the expect list
	if len(mockwriter.expect) > 0{
		t.Error("Have unread data, num lines ", len(mockwriter.expect))
	}

	fmt.Println("Run 2")
	////clear out the old stuff
	mockwriter = NewMockWriter(t)
	fillTestTable(mockwriter, true)
	extractor.Extract(mockwriter)

	///there should be nothing left in the expect list
	if len(mockwriter.expect) > 0{
		t.Error("Have unread data (2), num lines ", len(mockwriter.expect))
	}

	////Test with a good before
	qry = `SELECT field1, field2, field3 FROM ` + teststuff.testtablename +
		` WHERE lastmod > $1 AND lastmod < $3 
		 ORDER BY lastmod`
	extractor = newExtractor(teststuff.db, mocklastmod, qry, "lastmod",
		teststuff.testtablename,
		`SELECT time FROM test_good_before WHERE name='test'`, teststuff.limit)
	mockwriter = NewMockWriter(t)
	fillTestTable(mockwriter, true)
	////set the good before time to a few ms after the last entry
	goodbeforetime := teststuff.t.Add(2 * time.Millisecond)
	_, err := teststuff.db.Exec(`INSERT INTO test_good_before (name, time)
						VALUES($1, $2) ON CONFLICT (name)
						DO UPDATE SET time=excluded.time `, "test", goodbeforetime)
	handlers.PanicOnError(err)

	////Make sure the next fill starts after the good before time. Also, gives a clear break in the
	////test data if we need to look at it in the DB
	teststuff.t = teststuff.t.Add(5 * time.Second)
	mockwriter2 := NewMockWriter(t)
	fillTestTable(mockwriter2, true)

	fmt.Println("Run 3")
	////this should be limited by the good before time
	extractor.Extract(mockwriter)

	///there should be nothing left in the expect list
	if len(mockwriter.expect) > 0{
		t.Error("Have unread data (3), num lines ", len(mockwriter.expect))
	}

	goodbeforetime = teststuff.t.Add(2 * time.Second)
	_, err = teststuff.db.Exec(`INSERT INTO test_good_before (name, time)
						VALUES($1, $2) ON CONFLICT (name)
						DO UPDATE SET time=excluded.time `, "test", goodbeforetime)

	fmt.Println("Run 4")
	///this should not be limited and should read the remaining data
	extractor.Extract(mockwriter2)

	///there should be nothing left in the expect list
	if len(mockwriter2.expect) > 0{
		t.Error("Have unread data (4), num lines ", len(mockwriter.expect))
	}
}

func TestMain(m *testing.M) {

	cfgdir := ""
	flag.StringVar(&cfgdir, "cfg", "../ut-cfg", "The cfg directory")
	flag.Parse()
	cfg.Setup(cfgdir)
	setup()

	os.Exit(m.Run())
}
