# Flink streaming engine — bridge job

`MfStreamBridgeJob.java` is the Flink-engine adapter the MobilityAPI `flinkEngine`
spawns. It is a continuous Flink DataStream that reads one float per line from
stdin, applies a lifted MEOS operation through MobilityFlink's `MeosStatelessMap`
wiring (which initialises MEOS per task thread), and writes the transformed float
per line to stdout. Arguments: `<op> [arg]`.

The MobilityAPI tier holds no MEOS: the `flinkEngine` (pure Go) pipes the source
instants through this job, where MEOS runs as a JMEOS UDF — the streaming
analogue of the request-response tier issuing a named function to a database
backend.

## Build the classpath

The job compiles and runs against MobilityFlink's wirings, the JMEOS jar, and the
Flink runtime:

```
FP=/path/to/MobilityFlink/flink-processor
CP="$FP/target/classes:$FP/jar/JMEOS.jar:$(cd $FP && mvn -o -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout)"
javac -cp "$CP" -d . MfStreamBridgeJob.java
```

## Wire it into the tier

Select the Flink engine and point it at the job. MEOS runs inside the job, so the
tier itself is the default (cgo-free) build:

```
OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
       --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED \
       --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
       --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED"

MFAPI_STREAM_ENGINE=flink \
MFAPI_FLINK_CMD="java $OPENS -cp $CP:. MfStreamBridgeJob" \
MFAPI_FLINK_LIBPATH=/path/to/libmeos-dir \
MFAPI_DSN=<dsn> ./mfapi
```

A `POST …/queries` then runs the continuous transform on Flink and delivers the
result over the same Server-Sent Events stream as the in-process engine. The
`--add-opens` flags are Flink's requirement on Java 17+.

## Engine contract

The `flinkEngine` and the job communicate by a line protocol: one input float per
line in, one transformed float per line out, in order (the job runs at
parallelism 1). The tier pairs each output with its source instant's timestamp.
Any engine that honours this contract (a Kafka Streams or Spark Structured
Streaming job) plugs in through `MFAPI_FLINK_CMD` without a tier change.
