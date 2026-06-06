import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.mobilitydb.flink.meos.wirings.MeosStatelessMap;
import functions.GeneratedFunctions;
import jnr.ffi.Pointer;

import java.io.BufferedReader;
import java.io.InputStreamReader;

// The Flink-engine adapter the MobilityAPI flinkEngine spawns: a continuous
// Flink DataStream that reads one float per line from stdin, applies a lifted
// MEOS operation through MobilityFlink's MeosStatelessMap wiring (per-thread
// MEOS init), and writes the transformed float per line to stdout. The control
// plane pairs each output with its source instant's timestamp by order.
// Args: <op> [arg].
public class MfStreamBridgeJob {

    public static void main(String[] args) throws Exception {
        final String op = args[0];
        final double arg = args.length > 1 ? Double.parseDouble(args[1]) : 0.0;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> out = env
            .addSource(new StdinSource())
            .map(new MeosStatelessMap<String, String>(
                (MeosStatelessMap.MeosCall<String, String>) (line) -> lift(op, arg, line)))
            .returns(Types.STRING);
        out.addSink(new StdoutSink());

        env.execute("mf-stream-bridge");
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

    // Unbounded source: one input line per stream record, until stdin closes.
    public static final class StdinSource implements SourceFunction<String> {
        private volatile boolean running = true;
        @Override public void run(SourceContext<String> ctx) throws Exception {
            BufferedReader r = new BufferedReader(new InputStreamReader(System.in));
            String line;
            while (running && (line = r.readLine()) != null) {
                if (!line.isEmpty()) ctx.collect(line);
            }
        }
        @Override public void cancel() { running = false; }
    }

    // Sink: one transformed value per line on stdout, flushed for streaming.
    public static final class StdoutSink implements SinkFunction<String> {
        @Override public void invoke(String value, Context ctx) {
            System.out.println(value);
            System.out.flush();
        }
    }
}
