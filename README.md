# GoFlow

This package provides implementation of a simple not-distributed **concurrent event processing framework**.

The data processing is controlled by a **Pipeline** which consists of a number of interconnected Nodes that form a directed graph. 

The framework implements the _event stream processing_ model i.e. the data is exchanged between Nodes as unbounded continuous stream of granular pieces of data (aka _events_). An _event_ can be any golang value of any type that can be transported through channels. Both the content of an _event_ as well as its type can change as the event passes through Tasks.
Connections between the Nodes (graph edges) are implemented with channles (buffered or unbuffered).

There are several kinds of Nodes, but they all belong to three main categories: **Sources**, **Tasks** and **Outputs**. The data flows through the pipeline starting from one or more **Sources** that are responsible for extracting the data from external resources (e.g. by reading files or network sockets), then passed through zero or more **Tasks** that perform some kind of transformation on the data to finally reach **Outputs** responsible for exposing the results to some external systems (e.g. loading into a database). So, the most common use case of this framework is a small ETL (Extract-Transform-Load) pipeline.


The framework laverages golang built-in concurrency support - each Node can be executed in several parallel instances.

 Another golang feature - channels - allow for simple and reliable data exchange between Tasks and also provide buffering, backpressure, synchronus or asynchronous processing (with buffered or unbuffered channels).

The framework is generic i.e. Nodes and processing functions are generic so it's easy to modify existing pipeline while preserving the strongly typed nature of golang. Thanks to type inference it's usually not necessary to provide type parameters explicitly (as long as they can be infered from the processing 
functions or constructors).

The following is an example of a simple pipeline with two Sources and 3 Outputs:

```
string_source (c 2)
 +--> string_processor (c 1)
 |     +--> string_to_int (c 1)
 |     |     +--> int_out (c 1)
 |     +--> string_out (c 1)
 +--> string_to_int (c 1)
       +--> int_out (c 1)

int_source (c 1)
 +--> int_to_string (c 1)
       +--> string_out (c 1)
```

The pipeline consists of 2 sources _string_source_ and _int_source_ (these sources are independent but they share the same output _string_out_ i.e. they write to the same place).
The _string_source_ has concurrency level 2 which means it runs in two separate threads. It produces events of type string. Those events are then sent down to task _string_processor_ (which performs some kind of operation on the input), and output _string_out_ (which accepts string input). The output of _string_processor_ (a string) is passed to _string_to_int_ which performs some kind of operation of the string (e.g. returns its length) which yeilds an int value sent down to _int_out_ (which accepts int as an input). 
And so on (for the implementation details see examples/simple/simple.go).

## Why?

- I need a fast lightweight API for this kind of processing that can be quickly deployed in a docker.
- I need something that is as versatile as Apache Flink/Spark but without the Java development and cluster management overhead.
- Python is too slow.

## TODO list

- [ ] [Prometheus instrumentation](https://github.com/prometheus/client_golang). Add basic built-in metrics (counters of events in/out, errors, channel buffer len, processing delays etc.).
- [ ] Add basic file sources: csv, avro, parquet, json
- [ ] Add basic file outputs: csv, avro, parquet, json with partitioning and bucketing.
- [ ] Kafka Source & Output
- [ ] Add basic stateful tasks: keyed stateful mapping
- [ ] Add fault tollerance: checkpointing
- [ ] Add examples/templates
