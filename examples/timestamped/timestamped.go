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
	string_producer := func(ctx *flow.Context) (*TSEvent[string], bool) {
		if i < 10 {
			i++
			return &TSEvent[string]{fmt.Sprintf("src_%s_%d", *ctx.Id, i), time.Now()}, true
		} else {
			return nil, false
		}
	}
	// task functions
	event_processor := func(input *TSEvent[string], ctx *flow.Context) (*TSEvent[string], bool) {
		input.event = fmt.Sprintf("%v_after_%s", input.event, *ctx.Id)
		time.Sleep(1)
		return input, true
	}

	// output
	string_output := func (input *TSEvent[string], ctx *flow.Context) {
		delay := time.Now().Sub(input.ts)
		fmt.Println("> ",*ctx.Id, ":", input.event, "processing time:", delay.Microseconds(), "us")
	}
	
	s := flow.NewSource("Event_Source", 1, string_producer)
	t := flow.NewTask("Event_Processor", 2, event_processor)
	o := flow.NewOutput("Event_Output", 1, string_output)

	flow.SendsTo[*TSEvent[string]](s, t, 2)
	flow.SendsTo[*TSEvent[string]](t, o, 2)

	flow.Pipeline.Print()

	flow.Pipeline.Run()
	log.Println("Waiting")
	flow.Pipeline.Wait()
	log.Println("DONE")
}
