package main

import (
	"goflow/flow"
	"goflow/flow/source"
	"goflow/flow/output"
	"log"
)

func main() {
	// producer (closure)
	// task functions
	filterFields := func (input []string, ctx *flow.WorkerContext) ([]string, bool) {
		ret := []string{input[1], input[3], input[5]}
		return ret, true
	}

	fName := "../../test/data/simple.csv"
	csvIn := source.NewCsvReader(fName)
	csvIn.SkipHeader()

	csvOut := output.NewCsvWriter("test.csv.gz")
	csvOut.WithHeader([]string{"integer", "float", "string"})

	// create Nodes
	s1 := flow.NewSource("CSV_source", 1, csvIn.Read)
	t1 := flow.NewTask("FilterFields_task", 1, filterFields)
	o1 := flow.NewOutputWithClose("CSV_output", 1, csvOut.Write, csvOut.Close)

	// connect Nodes to build a pipeline
	flow.SendsTo[[]string](s1, t1, 10)
	flow.SendsTo[[]string](t1, o1, 10)

	flow.Pipeline.Print()

	flow.Pipeline.Run()
	log.Println("Waiting")
	flow.Pipeline.Wait()
	log.Println("DONE")
}