package output

import (
	"compress/gzip"
	"encoding/csv"
	"goflow/flow"
	"log"
	"os"
	"strings"
	"path/filepath"
)

// CsvWriter reads a CSV file and provides Read() method that can be used as produce function in goflow Source
type CsvWriter struct {
	csvWriter *csv.Writer
	zipWriter *gzip.Writer
	f *os.File
}

func NewCsvWriter(filePath string) *CsvWriter {
	p := CsvWriter{}
	if err := os.MkdirAll(filepath.Dir(filePath), 0770); err != nil {
        log.Fatal(err)
    }
	f, err := os.Create(filePath)
	if err != nil {
		log.Fatal(err)
	}
	if strings.HasSuffix(filePath, ".gz") {
		p.zipWriter = gzip.NewWriter(f)
		p.csvWriter = csv.NewWriter(p.zipWriter)
	} else {
		p.csvWriter = csv.NewWriter(f)
	}
	p.f = f
	return &p
}
func (p *CsvWriter) WithDelimiter(comma rune) {
	p.csvWriter.Comma = comma
}
func (p *CsvWriter) WithHeader(header []string) {
	if err := p.csvWriter.Write(header); err != nil {
		p.f.Close()
		log.Fatal(err)
	}
}
func (p *CsvWriter) Close() {
	p.csvWriter.Flush()
	if p.zipWriter != nil {
		p.zipWriter.Flush()
		p.zipWriter.Close()
	}
	p.f.Close()
}
// Writes row in the csv file
func (p *CsvWriter) Write(record []string, ctx *flow.Context) {
	if err := p.csvWriter.Write(record); err != nil {
		p.f.Close()
		log.Fatal(err)
	}
}