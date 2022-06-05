package source

import (
	"compress/gzip"
	"encoding/csv"
	"goflow/flow"
	"io"
	"log"
	"os"
	"strings"
)

// CsvReader reads a CSV file and provides Read() method that can be used as produce function in goflow Source
type CsvReader struct {
	csvReader *csv.Reader
	f *os.File
	current_row int
}

func NewCsvReader(filePath string) *CsvReader {
	p := CsvReader{}
	var fReader io.Reader
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	if strings.HasSuffix(filePath, ".gz") {
		fReader, err = gzip.NewReader(f)
		if err != nil {
			log.Fatal("Unable to read gzipped file")
		}
	} else {
		fReader = f
	}
	csvReader := csv.NewReader(fReader)
	p.f = f
	p.csvReader = csvReader
	return &p
}
func (p *CsvReader) WithDelimiter(comma rune) {
	p.csvReader.Comma = comma
}
func (p *CsvReader) WithReuseRecord() {
	p.csvReader.ReuseRecord = false
}
func (p *CsvReader) SkipHeader() {
	if p.current_row == 0 {
		p.Read(nil)
	}
}
// Read next row in the csv file and returns it as a slice of strings
func (p *CsvReader) Read(ctx *flow.WorkerContext) ([]string, bool) {
	record, err := p.csvReader.Read()
	if err == io.EOF {
		p.f.Close()
		return nil, false
	}
	if err != nil {
		log.Fatal(err)
	}
	p.current_row++
	return record, true
}