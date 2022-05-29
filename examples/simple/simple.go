package main

import (
	"fmt"
	"goflow/flow"
	"log"
)

func main() {
	// producer (closure)
	i := 0
	string_producer := func(id *string) (string, bool) {
		if i < 5 {
			i++
			return fmt.Sprintf("src_%s_%d", *id, i), true
		} else {
			return "", false
		}
	}
	j := 0
	int_producer := func(id *string) (int, bool) {
		if j < 5 {
			j++
			return j, true
		} else {
			return -1, false
		}
	}
	// task functions
	string_processor := func (input string, t_id *string) (string, bool) {
		fmt.Println(*t_id, "input:", input)
		output := fmt.Sprintf("%v_after_%s", input, *t_id)
		return output, true
	}
	string_to_int_processor := func (input string, t_id *string) (int, bool) {
		fmt.Println(*t_id, "input:", input)
		output := len(input)
		return output, true
	}
	// this one changes the type
	int_to_string_processor := func (input int, t_id *string) (string, bool) {
		fmt.Println(*t_id, "input:", input)
		output := fmt.Sprintf("%v_after_%s", input, *t_id)
		return output, true
	}

	// output functions
	string_output := func (data string, t_id *string) {
		fmt.Println(*t_id, "output:", data)
	}
	int_output := func (data int, t_id *string) {
		fmt.Println(*t_id, "output:", data)
	}
	
	// create Nodes
	s1 := flow.NewSource("string_source", 2, string_producer)
	s2 := flow.NewSource("int_source", 1, int_producer)
	t1 := flow.NewTask("string_processor", 1, string_processor)
	t2 := flow.NewTask("string_to_int", 1, string_to_int_processor)
	t3 := flow.NewTask("int_to_string", 1, int_to_string_processor)
	o1 := flow.NewOutput("string_out", 1, string_output)
	o2 := flow.NewOutput("int_out", 1, int_output)

	// connect Nodes to build a pipeline
	flow.SendsTo[string](s1, t1, 0)
	flow.SendsTo[string](s1, t2, 0)
    // NOTE:
	// flow.SendsTo[string](s, t, ...) can't infer type from t Task (it's both Sender and Receiver) so the type 
	// argument [string] must be provided explicitly.
	// That's actually not a bad thing since it adds a bit of clarity of what type of data is being sent on this
	// particular connection.
	// But if the type is not relevant (or too long/complex) - there's a workaround:
	flow.SendsTo(s2, t3.R(), 0) // this connection sends ints - but this fact is hidden by inference
	// Task has two additional methods: R() and S() that return Receiver[T] and Sender[T] respectively
	// so the compiler can infere types properly.
	flow.SendsTo(t1.S(), t2.R(), 0)
	// Anyway, explicit type argument is highly recommended.
	flow.SendsTo[string](t1, o1, 0)
	flow.SendsTo[int](t2, o2, 0)
	flow.SendsTo[string](t3, o1, 0)

	flow.Pipeline.Print()

	flow.Pipeline.Run()
	log.Println("Waiting")
	flow.Pipeline.Wait()
	log.Println("DONE")
}
