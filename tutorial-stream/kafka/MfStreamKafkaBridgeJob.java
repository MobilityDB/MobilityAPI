import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import functions.GeneratedFunctions;
import jnr.ffi.Pointer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

// The Kafka-engine adapter the MobilityAPI kafkaEngine spawns: a Kafka Streams
// topology that reads one float per line from stdin, applies a lifted MEOS
// operation through JMEOS GeneratedFunctions, and writes the transformed float
// per line to stdout. The control plane pairs each output with its source
// instant's timestamp by order, exactly as it does for the Flink bridge job, so
// the same tutorial drives either engine. The topology runs broker-free through
// TopologyTestDriver (in-process, parallelism 1, ordered) — the same execution
// the BerlinMOD Kafka Streams parity suite uses. Args: <op> [arg].
public class MfStreamKafkaBridgeJob {

    static final String IN = "mf-stream-in";
    static final String OUT = "mf-stream-out";

    public static void main(String[] args) throws Exception {
        final String op = args[0];
        final double arg = args.length > 1 ? Double.parseDouble(args[1]) : 0.0;

        GeneratedFunctions.meos_initialize_error_handler((level, code, message) -> { });
        GeneratedFunctions.meos_initialize();

        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(IN, Consumed.with(Serdes.String(), Serdes.String()))
            .mapValues(line -> lift(op, arg, line))
            .to(OUT, Produced.with(Serdes.String(), Serdes.String()));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mf-stream-bridge");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            TestInputTopic<String, String> in =
                driver.createInputTopic(IN, Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> out =
                driver.createOutputTopic(OUT, Serdes.String().deserializer(), Serdes.String().deserializer());
            BufferedReader r = new BufferedReader(new InputStreamReader(System.in));
            String line;
            while ((line = r.readLine()) != null) {
                if (line.isEmpty()) continue;
                in.pipeInput(line);
                System.out.println(out.readValue());
                System.out.flush();
            }
        }
    }

    // Apply one lifted operation to a single float, carried as a one-instant
    // tfloat at a fixed canonical time; return the transformed value.
    static String lift(String op, double arg, String floatLine) {
        Pointer t = GeneratedFunctions.tfloat_in(floatLine.trim() + "@2000-01-01 00:00:00+00");
        Pointer r;
        switch (op) {
            case "ln":      r = GeneratedFunctions.tfloat_ln(t); break;
            case "exp":     r = GeneratedFunctions.tfloat_exp(t); break;
            case "log10":   r = GeneratedFunctions.tfloat_log10(t); break;
            case "ceil":    r = GeneratedFunctions.tfloat_ceil(t); break;
            case "floor":   r = GeneratedFunctions.tfloat_floor(t); break;
            case "abs":     r = GeneratedFunctions.tnumber_abs(t); break;
            case "degrees": r = GeneratedFunctions.tfloat_degrees(t, false); break;
            case "radians": r = GeneratedFunctions.tfloat_radians(t); break;
            case "add":     r = GeneratedFunctions.add_tfloat_float(t, arg); break;
            case "sub":     r = GeneratedFunctions.sub_tfloat_float(t, arg); break;
            case "mul":     r = GeneratedFunctions.mul_tfloat_float(t, arg); break;
            case "div":     r = GeneratedFunctions.div_tfloat_float(t, arg); break;
            default: throw new IllegalArgumentException("unknown operation: " + op);
        }
        String s = GeneratedFunctions.tfloat_out(r, 15);
        int at = s.indexOf('@');
        return at >= 0 ? s.substring(0, at) : s;
    }
}
