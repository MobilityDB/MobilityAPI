# Kafka Streams streaming engine — bridge job

`MfStreamKafkaBridgeJob.java` is the Kafka-engine adapter the MobilityAPI
`kafkaEngine` spawns. It is a Kafka Streams topology
(`stream → mapValues(lift) → to`) that reads one float per line from stdin,
applies a lifted MEOS operation through JMEOS `GeneratedFunctions`, and writes the
transformed float per line to stdout. Arguments: `<op> [arg]`.

The topology runs broker-free through `TopologyTestDriver` (in-process,
parallelism 1, ordered) — the same execution the BerlinMOD Kafka Streams parity
suite uses — so the engine needs no cluster. The MobilityAPI tier holds no MEOS:
the `kafkaEngine` (pure Go) pipes the source instants through this job, where MEOS
runs as a JMEOS UDF — the streaming analogue of the request-response tier issuing
a named function to a database backend.

## Build the classpath

The job compiles and runs against Kafka Streams (with `kafka-streams-test-utils`
for `TopologyTestDriver`) and the JMEOS jar:

```
KS=/path/to/kafka-streams-app
CP="$KS/jar/JMEOS.jar:$(cd $KS && mvn -o -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout)"
javac -cp "$CP" -d . MfStreamKafkaBridgeJob.java
```

## Wire it into the tier

Select the Kafka engine and point it at the job. MEOS runs inside the job, so the
tier itself is the default (cgo-free) build:

```
OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
       --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED \
       --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
       --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED"

MFAPI_STREAM_ENGINE=kafka \
MFAPI_KAFKA_CMD="java $OPENS -cp $CP:. MfStreamKafkaBridgeJob" \
MFAPI_KAFKA_LIBPATH=/path/to/libmeos-dir \
MFAPI_DSN=<dsn> ./mfapi
```

A `POST …/queries` then runs the continuous transform on Kafka Streams and
delivers the result over the same Server-Sent Events stream as the in-process and
Flink engines. The `--add-opens` flags are the Java 17+ requirement for the JNR
FFI the JMEOS jar uses.

## Engine contract

The `kafkaEngine` and the job communicate by the same line protocol as the Flink
engine: one input float per line in, one transformed float per line out, in order
(the topology runs at parallelism 1). The tier pairs each output with its source
instant's timestamp. The same tutorial drives the Flink and Kafka engines by
switching `MFAPI_STREAM_ENGINE`; both serve the identical lifted-operation set
(`ln, exp, log10, ceil, floor, abs, degrees, radians, add, sub, mul, div`) and
delegate windowed aggregation to the in-process engine.
