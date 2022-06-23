package main

import (
	"goflow/flow"
	"goflow/flow/output"
	"goflow/flow/source"
	"log"
	"time"
)

func main() {
	// producer (closure)
	// task functions
	filterFields := func (input []string, ctx *flow.Context) ([]string, bool) {
		ret := []string{input[1], input[3], input[5]}
		time.Sleep(50 * time.Millisecond)
		return ret, true
	}

	fName := "../../test/data/simple.csv"
	csvIn := source.NewCsvReader(fName)
	csvIn.SkipHeader()


	csvFactory := func(path string) flow.Writer[[]string] {
		csvOut := output.NewCsvWriter(path)
		csvOut.WithHeader([]string{"integer", "float", "string"})
		return csvOut
	}
	bucketWriter := output.NewBucketWriter("output", "prefix", ".csv.gz", 100, -1, csvFactory, time.Second)
	bucketWriter.RolloverPolicy = bucketWriter.MaxRecordsRolloverPolicy

	// create Nodes
	s1 := flow.NewSource("CSV_source", 1, csvIn.Reader())
	t1 := flow.NewTask("FilterFields_task", 1, filterFields)
	o1 := flow.NewOutput("CSV_output", 1, bucketWriter.Writer())

	// connect Nodes to build a pipeline
	flow.SendsTo[[]string](s1, t1, 10)
	flow.SendsTo[[]string](t1, o1, 10)

	flow.Pipeline.Print()

	flow.Pipeline.Run()
	log.Println("Waiting")
	flow.Pipeline.Wait()
	log.Println("DONE")
}