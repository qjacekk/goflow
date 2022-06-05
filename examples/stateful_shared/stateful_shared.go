package main

import (
	"fmt"
	"goflow/flow"
	"math/rand"
	"log"
	"sync"
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
	global_map := make(map[int]bool)
	mutex := sync.Mutex{}
	// task functions
	stateful_event_processor := func(input int, ctx *flow.Context) (string, bool) {
		if ctx.State == nil {
			// Init state - NOTE: this inits each thread context with common state (a map[int]bool in this case)
			// shared by all threads. This kind of stateful processing requires synchronization!
			ctx.State = global_map
		}
		m := ctx.State.(map[int]bool)
		mutex.Lock()
		defer mutex.Unlock()
		found := m[input]
		if found {
			fmt.Printf("= thread_%s_value %d --> already seen\n", *ctx.Id, input)
			return "", false
		} else {
			fmt.Printf("+ thread_%s got new value %d\n", *ctx.Id, input)

			m[input] = true
			return fmt.Sprintf("thread_%s_value_%d", *ctx.Id, input), true
		}
	}

	// output
	int_output := func (input string, ctx *flow.Context) {
		fmt.Println("out> ",*ctx.Id, ":", input)
	}
	
	s := flow.NewSource("Event_Source", 1, rand_int_producer)
	t := flow.NewTask("Event_Processor", 4, stateful_event_processor)
	o := flow.NewOutput("Event_Output", 1, int_output)

	

	flow.SendsTo[int](s, t, 2)
	flow.SendsTo[string](t, o, 2)

	flow.Pipeline.Print()

	flow.Pipeline.Run()
	log.Println("Waiting")
	flow.Pipeline.Wait()
	log.Println("DONE")
}
