package main

import (
	"fmt"
	"goflow/flow"
	"goflow/flow/source"
	"log"
	"time"
)

func main() {
	readerFactory := func(path string) flow.Reader[[]string]{
		csvIn := source.NewCsvReader(path)
		csvIn.SkipHeader()
		return csvIn.Reader()
	}
	fileMonitor := source.NewFileMonitor[[]string]("test/data/csvs", "*.csv", true, time.Second *5, readerFactory)


	s1 := flow.NewSource[[]string]("CSV_file_monitor", 1, fileMonitor)
	// task functions
	printRow := func (input []string, ctx *flow.Context) ([]string, bool) {
		fmt.Println(">", input)
		return input, true
	}

	t1 := flow.NewTask("Print", 1, printRow)
	o1 := flow.NewOutput("CSV_output", 1, flow.NewDummyWriter[[]string]())

	// connect Nodes to build a pipeline
	flow.SendsTo[[]string](s1, t1, 10)
	flow.SendsTo[[]string](t1, o1, 10)

	flow.Pipeline.Print()

	flow.Pipeline.Run()
	log.Println("Waiting")
	flow.Pipeline.Wait()
	log.Println("DONE")
}
