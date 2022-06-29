package main

import (
	"fmt"
	"goflow/flow"
	"math/rand"
	"log"
)

func main() {
	// producer (closure)
	i := 0
	rand_int_producer := func(ctx *flow.Context) (int, bool) {
		if i < 100 {
			i++
			r := rand.Intn(10)
			return r, true
		} else {
			return -1, false
		}
	}
	// task functions
	stateful_event_processor := func(input int, ctx *flow.Context) (string, bool) {
		if ctx.State == nil {
			// Init state - NOTE: this creates separate state (a map[int]bool in this case) per each thread of the processor!
			// This map doesn't have to be synchronized so it's fast but it can be used only if:
			// - concurrency of the task is 1
			// - conc > 1 but the state doesn't have to be shared between threads (e.g. each thread handles disjoint subset of events)
			// For shared state example see: stateful_shared.go
			m := make(map[int]bool)
			ctx.State = m
		}
		m := ctx.State.(map[int]bool)
		if m[input] {
			fmt.Printf("thread_%s_value %d --> already seen\n", *ctx.Id, input)
			return "", false
		} else {
			fmt.Printf("thread_%s got new value %d\n", *ctx.Id, input)
			m[input] = true
			return fmt.Sprintf("thread_%s_value_%d", *ctx.Id, input), true
		}
	}

	// output
	int_output := func (input string, ctx *flow.Context) {
		fmt.Println("out> ",*ctx.Id, ":", input)
	}
	
	s := flow.NewSource("Event_Source", 1, flow.NewSimpleReader(rand_int_producer))
	t := flow.NewTask("Event_Processor", 2, stateful_event_processor)
	o := flow.NewOutput("Event_Output", 1, flow.NewSimpleWriter(int_output))

	

	flow.SendsTo[int](s, t, 2)
	flow.SendsTo[string](t, o, 2)

	flow.Pipeline.Print()

	flow.Pipeline.Run()
	log.Println("Waiting")
	flow.Pipeline.Wait()
	log.Println("DONE")
}
