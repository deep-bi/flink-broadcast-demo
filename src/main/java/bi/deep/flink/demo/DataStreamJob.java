package bi.deep.flink.demo;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class DataStreamJob {

    private static final Lookup[] lookupStream = {
            new Lookup("name:1", "A"),
            new Lookup("name:2", "B"),
            new Lookup("org:1", "X"),
            new Lookup("org:2", "Y"),
    };

    public static void main(String[] args) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1)) {
            // define source streams
            var source = env.fromCollection(new EventGenerator(), TypeInformation.of(InputEvent.class));
            var lookups = env.fromCollection(Arrays.asList(lookupStream)).setParallelism(1);


            // define lookup schema
            final var lookupDescriptor = new MapStateDescriptor<>(
                    "Lookup", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
            );
            // connect the input stream with a broadcasted lookup
            source.connect(lookups.broadcast(lookupDescriptor))
                    .process(new BroadcastProcessFunction<InputEvent, Lookup, OutputEvent>() {

                        @Override
                        public void processElement(InputEvent e, BroadcastProcessFunction<InputEvent, Lookup,
                                OutputEvent>.ReadOnlyContext ctx, Collector<OutputEvent> out) throws Exception {
                            // enrich input event with a lookup
                            var state = ctx.getBroadcastState(lookupDescriptor);
                            out.collect(new OutputEvent(
                                    e.uid,
                                    state.get("name:" + e.nameId),
                                    state.get("org:" + e.orgId))
                            );
                        }

                        @Override
                        public void processBroadcastElement(Lookup e, BroadcastProcessFunction<InputEvent, Lookup,
                                OutputEvent>.Context ctx, Collector<OutputEvent> out) throws Exception {
                            // update the lookup table
                            var state = ctx.getBroadcastState(lookupDescriptor);
                            state.put(e.id, e.label);
                            // TODO: Note that we just put the entries here, not remove. In prod you would probably have to remove not used entries
                            // state.remove();
                        }
                    })
                    .print();

            env.execute("Broadcast Demo");
        }
    }
}
