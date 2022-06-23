package flow

// This package provides implementation of a simple not-distributed **concurrent event processing framework**.
// The data processing is controlled by a **Pipeline** which consists of a number of interconnected Nodes that form a directed graph.

// The framework implements the _event stream processing_ model i.e. the data is exchanged between Nodes as unbounded continuous stream of granular pieces of data (aka _events_). An _event_ can be any golang value of any type that can be transported through channels. Both the content of an _event_ as well as its type can change as the event passes through Tasks.
// Connections between the Nodes (graph edges) are implemented with channles (buffered or unbuffered).

// There are several kinds of Nodes, but they all belong to three main categories: **Sources**, **Tasks** and **Outputs**. 
// The data flows through the pipeline starting from one or more **Sources** that are responsible for extracting the data from external resources (e.g. by reading files or network sockets), then passed through zero or more **Tasks** that perform some kind of transformation on the data to finally reach **Outputs** responsible for exposing the results to some external systems (e.g. loading into a database). So, the most common use case of this framework is a small ETL (Extract-Transform-Load) pipeline.

// The framework laverages golang built-in concurrency support - each Node can be executed in several parallel instances.

//  Another golang feature - channels - allow for simple and reliable data exchange between Tasks and also provide buffering, backpressure, synchronus or asynchronous processing (with buffered or unbuffered channels).

// The framework is generic i.e. Nodes and processing functions are generic so it's easy to modify existing pipeline while preserving the strongly typed nature of golang. Thanks to type inference it's usually not necessary to provide type parameters explicitly (as long as they can be infered from the processing
// functions or constructors).

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Package level variables

// Pipeline is a package level singleton that controls the pipeline
var Pipeline pipeline = pipeline{graph: make(map[string][]Node), sources: make(map[string]Node), others: make(map[string]Node)} 
var PReg *prometheus.Registry = prometheus.NewRegistry()

// wg provides synchronization of Nodes. Pipleline can be shut down only after all Nodes have finished their job.
var wg sync.WaitGroup  // WaitGroup to synchronize all nodes


/*   NODE   */

// Enum that specifies the kind of a Node
// It's needed because there can be different types that provide the same kind of functionality.
// Type checking seems more complex than this.
type NodeKind int 
const(
	source NodeKind = iota
	task
	output
	// TODO: add some predefined stuff like filter, mapper, stateful, window, timeWindow etc.
)

// Node interace has a set of methods that are common to all kinds of Nodes.
// run() method is called when starting pipleline execution.
type Node interface {
	Name() string
	Conc() int
	Kind() NodeKind
	Run()
}

// Base struct for all kinds of Nodes. Each specific node structure must embed this to inherit Node interface.
type node struct {
	name string
	conc int
	kind NodeKind
}
// Returns the name of a Node
func (n node) Name() string {
	return n.name
}
// Returns 
func (n node) Conc() int {
	return n.conc
}
func (n node) Kind() NodeKind {
	return n.kind
}

/* CONTEXT */

// NodeContext is a structure passed to a worker thread in the processing function.
// WorkerContext contains thread-local parameters and variables.
type Context struct {
	Id *string  // workerId, unique for each thread (goroutine)
	NodeInstance Node
	State any
}


/*   MQ (Message Queue)   */

// MQ is wrapper over a channel which acts as a message queue for passing data between Nodes
// that supports multi-producer and multi-consumer pattern.
// MQ is always controlled by the producer (channel writer) by calling AddProducer(numOfWorkers int)
// and close() to close the channel. 
// close() can be safely called by each producer worker gorutine (sync.Once takes care of closing 
// a channel exactly once).
type MQ[T any] struct {
	queue chan T
	qwg  sync.WaitGroup  // This WaitGroup syncs all possible concurrent writer threads
	once sync.Once
}
// Creates new buffered (if queueSize > 0) or unbuffered (queueSize == 0) MQ
func NewMQ[T any](queueSize int) *MQ[T] {
	return &MQ[T]{queue: make(chan T, queueSize)}
}
// Closes MQ channel as soon as all workers have finished (using sync.Once)
// This looks weird but the point is:
// - there can be multiple go rutines writing to this channel (conc > 1)
// - the channel must be closed only after ALL producer's go rutines return (to finish all subsequent 
//   tasks that use range operator to read from the channel) so close() can't be called directly from 
//   producer's gorutine (this would cause "send on closed channel" panic in other threads). 
// - the solution is to use WaitGroup to wait for all producers to finish and then close the channel
//   but the question is where and when to call wg.Wait()
// - simple solution would be to start one more "closer" gorutine just after dispatching producers but
//   that would be a waste of resources (especially since the pipeline is supposed to run infinitely) 
// - so the better solution is to use sync.Once to start this "closer" gorutine only after the first
//   producer has finished.
func (mq *MQ[T]) close() {
	mq.once.Do(func() {
		go func() {
			mq.qwg.Wait()
			close(mq.queue)
		}()
	})
	mq.qwg.Done()

}
func (mq *MQ[T]) AddProducer(numOfWorkers int) {
	mq.qwg.Add(numOfWorkers)
}


/*   SENDER   */

// Sender is an interface for nodes that send data out.
type Sender[OUT any] interface {
	Node
	sendTo(Receiver[OUT], int)
	close()
}

// Base sender structure contains outgoing queues and can be embedded in nodes that send data.
// Sender handles one or more outgoing queues i.e. can send to one or more different nodes.
type sender[OUT any] struct {
	out []*MQ[OUT]  // a slice of MQs, Sender can send to more than one Receiver
}
// Closes all outgoing queues
func (s *sender[OUT]) close() {
	for _, mq := range s.out {
		mq.close()
	}
	wg.Done()
}
func (s *sender[OUT]) sendTo(rec Receiver[OUT], queueSize int) {
	mq := rec.getInputMQ(queueSize)
	for _,v := range s.out {
		if v == mq {
			log.Panicf("source already sends to %s", rec.Name()) 
		}
	}
	s.out = append(s.out, mq)
	// NOTE: can't call Pipeline.RegisterEdge(s, rec) here because s in not a Node
	// sender struct can not be a node because Task would have node twice
}

/*   RECEIVER   */

// Receiver is an interface for nodes that receive data.
type Receiver[IN any] interface {
	Node
	getInputMQ(queueSize int) *MQ[IN]
}

// Base receiver struct handles incoming queue and can be embedded in nodes that receive data out.
// Receiver uses a single queue i.e. it can receive from one node only.
type receiver[IN any] struct {
	in *MQ[IN]
}
func (r *receiver[IN]) getInputMQ(queueSize int) *MQ[IN] {
	if r.in == nil {
		r.in = &MQ[IN]{queue: make(chan IN, queueSize)}
	}
	return r.in	
}

/*   PIPELINE   */

// Pipeline is the main controller of the processing pipeline, it keeps the graph of nodes, 
// starts and waits utile the processing is completed.
// The pipeline is private and is not supposed to be instantiated outside of the package,
// instead the package provides a singleton Pipeline instance (as package level variable).
type pipeline struct {
	graph map[string][]Node
	sources map[string]Node  
	others map[string]Node // need to separate source from other nodes
}

// Adds a new node to the pipeline.
func (p *pipeline) RegisterNode(n Node) {
	name := n.Name()
	if n.Kind() == source {
		if _,ok := p.sources[name]; ok {
			// already registered
			return
		}
		p.sources[name] = n
	} else {
		if _,ok := p.others[name]; ok {
			return
		}
		p.others[name] = n
	}
}

// Adds a new edge to the graph (i.e. connection from sending Node to receiving Node)
func (p *pipeline) RegisterEdge(snd Node, rec Node) {
	// TODO: add safety checks for cycles (make it DAG directed acyclic graph)
	p.graph[snd.Name()] = append(p.graph[snd.Name()], rec)
}

// Run method starts the processing. A call to Pipeline.Run() must be followed by Pipeline.Wait()
func (p *pipeline) Run() {
	// start processing
	for _, node:= range p.others {
		node.Run()
	}
	// start sources last
	for _, src:= range p.sources {
		src.Run()
	}
}

// Waits until the processing is completed and all channels safely shut down.
func (p *pipeline) Wait() {
	wg.Wait()
}

// Waits until the processing is completed and all channels safely shut down.
// Starts Prometheus metrics server
func (p *pipeline) WaitWithMetrics(hostAndPort string) {
	go func(hp string) {
		if hp == "" {
			hp = ":2112"
		}
		log.Printf("Starting Prometheus metrics server at: %s/metrics\n", hp)
		// TODO: add option to enable built-in go metrics
		//PReg.MustRegister(collectors.NewGoCollector(
		//	collectors.WithGoCollections(collectors.GoRuntimeMemStatsCollection | collectors.GoRuntimeMetricsCollection),
		//	))

		handler := promhttp.HandlerFor(PReg, promhttp.HandlerOpts{})
		http.Handle("/metrics", handler)
		http.ListenAndServe(":2112", nil)
	}(hostAndPort)
	wg.Wait()
}

// Prints out the pipeline as a simple tree, e.g.:
//
// string_source (c 2)
//  +--> string_processor (c 1)
//  |     +--> string_to_int (c 1)
//  |     |     +--> int_out (c 1)
//  |     +--> string_out (c 1)
//  +--> string_to_int (c 1)
//        +--> int_out (c 1)
//
// int_source (c 1)
//  +--> int_to_string (c 1)
//        +--> string_out (c 1)
// 
// where (c x) is the concurrency of that node.
func (p pipeline) Print() {
	for _, src := range p.sources {
		p.printPath(src, "")
		fmt.Println()
	}
}

// (private) recurrently print all paths starting from n until a path reaches output node
func (p pipeline) printPath(n Node, padding string) {
	fmt.Printf("%s (c %d)\n", n.Name(), n.Conc())
	nsub := len(p.graph[n.Name()])
	var pad string
	for i, next := range p.graph[n.Name()] {
		if nsub > 1 && i<(nsub-1) {
			pad = " |    "
		} else {
			pad = "      "
		}
		fmt.Printf("%s +--> ", padding)
		if next.Kind() == output {
			fmt.Printf("%s (c %d)\n", next.Name(), next.Conc())
		} else {
			p.printPath(next, padding + pad)
		}
	} 
}

// SendsTo is a function that connects Sender with Receiver
// and also adds the connection (edge) to the Pipeline graph
func SendsTo[T any](snd Sender[T], rec Receiver[T], queueSize int) {
	snd.sendTo(rec, queueSize)
	Pipeline.RegisterEdge(snd, rec)
}


/*    SOURCE    */
type Reader[R any] interface {
	Read(ctx *Context) (R, bool)
	Close()
}
type SimpleReader[R any] struct {
	Produce func(ctx *Context) (R, bool)
}
func (sr *SimpleReader[R]) Read(ctx *Context) (R, bool) {
	return sr.Produce(ctx)
}
func (sr *SimpleReader[R]) Close() { }
func NewSimpleReader[R any](produce func(ctx *Context)(R, bool)) Reader[R] {
	return &SimpleReader[R]{Produce: produce}
}


// Source is a node that produces events by extracing data from some external resource (e.g. file,
// network socket, database etc.) by using Rader instance.
// Source calls Reader.Read(...) (R, bool) function repetedely to get next piece of data (event of type OUT) and sends it down the pipleline.
// The second return value - the boolean flag indicates if the event is valid.
// If the flag is false then source ends processing and shuts down the pipeline (i.e. closes all downstream channels).
type Source[OUT any] struct {
	node
	sender[OUT] 
	reader Reader[OUT]
	// instrumentation
	outCnt prometheus.Counter
	produceTimeHist prometheus.Histogram
}

// Source constructor that creates a new instance and registers it in the Pipeline.
func NewSource[OUT any](name string, conc int, reader Reader[OUT]) Sender[OUT] {
	if conc < 1 {
		log.Panic("concurrency must be >= 1")
	}
	outCnt := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: name + "_events_out_count",
			Help: "No of events produced",
		},
	)
	produceTimeHist := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: name + "_produce_time",
			Help: "Producer time (millis)",
			Buckets: prometheus.LinearBuckets(1, 1000, 5),
		},
	)
	PReg.MustRegister(outCnt)
	PReg.MustRegister(produceTimeHist)
	src := Source[OUT]{node: node{name: name, conc: conc, kind: source}, reader: reader, outCnt: outCnt, produceTimeHist: produceTimeHist}
	Pipeline.RegisterNode(&src)
	return &src
}

// Starts processing
func (s *Source[OUT]) Run() {
	for _, mq := range s.out {
		mq.AddProducer(s.conc)
	}
	for id := 0; id < s.Conc(); id++ {
		wg.Add(1)
		go func(name string, id int) {
			defer s.close()
			s_id := fmt.Sprintf("%s_%d", s.Name(), id)
			workerCtx := Context{Id: &s_id, NodeInstance: s}
			log.Printf("Starting source %s\n", s_id)
			for {
				start := time.Now()
				result, ok := s.reader.Read(&workerCtx)
				if !ok {
					break
				}
				s.produceTimeHist.Observe(float64(time.Since(start).Milliseconds()))
				for _, mq := range s.out {
					mq.queue <- result
				}
				s.outCnt.Inc()
			}
			log.Printf("Finishing source %s\n", s_id)
			
		}(s.Name(), id)
	}
}


/*    TASK    */

// Task is a Node that performs some transformation on a stream of data by executing user-defined execute function 
// A task has one input queue and possibly many output queue (when a task sends to more than one task/output).
// The execute function receives an event (of type [IN]) and a task id (for debugging purposes mostly) and
// returns transformed value (of type [OUT]) and a boolean flag indicating if the retured value should be sent
// downstream (returning false act as a filter - the value will be simply ignored).
// Execute function may transform value or type or both of the input event or act as a filter.
// Execute can be a statefull (closure) method or a stateless function.
type Task[IN any, OUT any] struct {
	node
	receiver[IN]
	sender[OUT]
	execute     func(input IN, taskCtx *Context) (OUT, bool)
	// instrumentation
	inCnt prometheus.Counter
	outCnt prometheus.Counter
	runTimeHist prometheus.Histogram
}

// Task constructor that creates a new instance and registers it in the Pipeline.
func NewTask[IN any, OUT any](name string, conc int, execute func(IN, *Context) (OUT, bool)) *Task[IN, OUT] {
	if conc < 1 {
		log.Panic("task concurrency must be >= 1")
	}
	if execute == nil {
		log.Panic("execute function must not be nil")
	}
	inCnt := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: name + "_events_in_count",
			Help: "No of events received",
		},
	)
	outCnt := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: name + "_events_out_count",
			Help: "No of events sent",
		},
	)
	runTimeHist := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: name + "_run_time",
			Help: "Task time (millis)",
			Buckets: prometheus.LinearBuckets(1, 1000, 5),
		},
	)
	PReg.MustRegister(inCnt)
	PReg.MustRegister(outCnt)
	PReg.MustRegister(runTimeHist)
	t := Task[IN, OUT]{node: node{name: name, conc: conc, kind: task}, execute: execute, inCnt: inCnt, outCnt: outCnt, runTimeHist: runTimeHist}
	Pipeline.RegisterNode(&t)
	return &t
}
// R() returns Receiver interface of Task. This is a trick to force type inference.
func (t *Task[IN, OUT]) R() Receiver[IN] { return t}
// S() returns Sender interface of Task. This is a trick to force type inference.
func (t *Task[IN, OUT]) S() Sender[OUT] { return t}

// starts processing
func (t *Task[IN, OUT]) Run() {
	for _, mq := range t.out {
		mq.AddProducer(t.conc)
	}
	for id := 0; id < t.conc; id++ {
		wg.Add(1)
		go func(name string, id int) {
			defer t.close()
			t_id := fmt.Sprintf("%s_%d", t.name, id)
			workerCtx := Context{Id: &t_id, NodeInstance: t}
			log.Printf("Starting task %s\n", t_id)
			for input := range t.in.queue {
				t.inCnt.Inc()
				start := time.Now()
				result, ok := t.execute(input, &workerCtx)
				t.runTimeHist.Observe(float64(time.Since(start).Milliseconds()))
				if ok {
					for _, mq := range t.out {
						mq.queue <- result
					}
					t.outCnt.Inc()
				}
			}
			log.Printf("Finishing task %s_%d\n", t.name, id)
		}(t.name, id)
	}
}


/*    OUTPUT    */
type Writer[R any] interface {
	Write(record R, ctx *Context)
	Close()
}
type SimpleWriter[R any] struct {
	Consume func(record R, ctx *Context)
}
func (sw *SimpleWriter[R]) Write(record R, ctx *Context) {
	sw.Consume(record, ctx)
}
func (sw *SimpleWriter[R]) Close() {}
func NewSimpleWriter[R any](consume func(record R, ctx *Context)) Writer[R] {
	return &SimpleWriter[R]{consume}
}

// Output receives events and generates some kind of output to external world (e.g. writes to file
// or database).
type Output[IN any] struct {
	node
	receiver[IN]
	writer Writer[IN]

	// instrumentation
	inCnt prometheus.Counter
	writeTimeHist prometheus.Histogram
}

func newOutput[IN any](name string, conc int, writer Writer[IN]) *Output[IN] {
	if conc < 1 {
		log.Panic("concurrency must be >= 1")
	}
	if writer == nil {
		log.Panic("writer must not be nil")
	}
	inCnt := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: name + "_events_in_count",
			Help: "No of events received",
		},
	)
	writeTimeHist := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: name + "_write_time",
			Help: "Output time (millis)",
			Buckets: prometheus.LinearBuckets(1, 1000, 5),
		},
	)
	PReg.MustRegister(inCnt)
	PReg.MustRegister(writeTimeHist)

	o := Output[IN]{node: node{name: name, conc: conc, kind: output}, writer: writer, inCnt: inCnt, writeTimeHist: writeTimeHist}
	Pipeline.RegisterNode(&o)
	return &o
}

// Output constructor that creates a new instance and registers it in the Pipeline.
func NewOutput[IN any](name string, conc int, writer Writer[IN]) Receiver[IN] {
	o := newOutput(name, conc, writer)
	return o
}

// starts processing
func (o *Output[IN]) Run() {
	for id := 0; id < o.conc; id++ {
		wg.Add(1)
		go func(name string, id int) {
			defer wg.Done()
			o_id := fmt.Sprintf("%s_%d", o.name, id)
			workerCtx := Context{Id: &o_id, NodeInstance: o}
			log.Printf("Starting output %s\n", o_id)
			for input := range o.in.queue {
				o.inCnt.Inc()
				start := time.Now()
				o.writer.Write(input, &workerCtx)
				o.writeTimeHist.Observe(float64(time.Since(start).Milliseconds()))
			}
			log.Printf("Finishing output %s_%d\n", o.name, id)
			o.writer.Close()
		}(o.name, id)
	}
}