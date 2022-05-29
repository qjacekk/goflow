package main

import (
	"fmt"
	"goflow/flow"
	"log"
	"time"
)

// Timestamped event
type TSEvent[T any] struct {
	event T
	ts time.Time
}

// This is an example of how to build a pipeline with timestamped events
// This also shows how to measure end-to-end processing time.
func main() {
	// producer (closure)
	i := 0
	string_producer := func(id *string) (*TSEvent[string], bool) {
		if i < 10 {
			i++
			return &TSEvent[string]{fmt.Sprintf("src_%s_%d", *id, i), time.Now()}, true
		} else {
			return nil, false
		}
	}
	// task functions
	event_processor := func(input *TSEvent[string], t_id *string) (*TSEvent[string], bool) {
		input.event = fmt.Sprintf("%v_after_%s", input.event, *t_id)
		time.Sleep(1)
		return input, true
	}

	// output
	string_output := func (input *TSEvent[string], t_id *string) {
		delay := time.Now().Sub(input.ts)
		fmt.Println("> ",*t_id, ":", input.event, "processing time:", delay.Microseconds(), "us")
	}
	
	s := flow.NewSource("Event_Source", 1, string_producer)
	t := flow.NewTask("Event_Processor", 2, event_processor)
	o := flow.NewOutput("Event_Output", 1, string_output)
	
	//flow.SendsTo[*TSEvent[string]](s, t, 2)
	flow.SendsTo(s, t.R(), 2)
	flow.SendsTo(t.S(), o, 2)

	flow.Pipeline.Print()

	flow.Pipeline.Run()
	log.Println("Waiting")
	flow.Pipeline.Wait()
	log.Println("DONE")
}
