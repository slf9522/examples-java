/*
 * Copyright 2015 Fabian Hueske / Vasia Kalavri
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.streamingwithflink.chapter6;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

public class CustomWindow {

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkpoint every 10 seconds
        env.getCheckpointConfig().setCheckpointInterval(10_000);

        // use event time for the application
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // configure watermark interval
        env.getConfig().setAutoWatermarkInterval(1000L);

        // ingest sensor stream
        DataStream<SensorReading> sensorData = env
                // SensorSource generates random temperature readings
                .addSource(new SensorSource())
                // assign timestamps and watermarks which are required for event time
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<Tuple4<String, Long, Long, Integer>> countsPerThirtySecs = sensorData
                .keyBy(r -> r.id)
                // a custom window assigner for 30 seconds tumbling windows
                .window(new ThirtySecondsWindows())
                // a custom trigger that fires early (at most) every second
                .trigger(new OneSecondIntervalTrigger())
                // count readings per window
                .process(new CountFunction());

        countsPerThirtySecs.print();

        DataStream<MinMaxTemp> minMaxTempPerWindow =
                sensorData.keyBy(r -> r.id).timeWindow(Time.seconds(5)).process(new HighAndLowTempProcessFunction());
        minMaxTempPerWindow.print();

        DataStream<MinMaxTemp> minMaxTempDataStream =
                sensorData
                        .map(r -> new Tuple3<>(r.id, r.temperature, r.temperature))
                        .keyBy(r -> r.f0)
                        .timeWindow(Time.seconds(5))
                        .reduce(
                                (Tuple3<String, Double, Double> r1, Tuple3<String, Double, Double> r2) -> {
                                    return new Tuple3<String, Double, Double>(
                                            r1.f0, Math.min(r1.f1, r2.f1), Math.max(r1.f2, r2.f2)
                                    );
                                },
                                new AssignWindowEndProcessFunction()
                        );
        minMaxTempDataStream.print();

        env.execute("Run custom window example");
    }

    /**
     * A custom window that groups events in to 30 second tumbling windows.
     */
    public static class ThirtySecondsWindows extends WindowAssigner<Object, TimeWindow> {

        long windowSize = 30_000L;

        @Override
        public Collection<TimeWindow> assignWindows(Object e, long ts, WindowAssignerContext ctx) {

            // rounding down by 30 seconds
            long startTime = ts - (ts % windowSize);
            long endTime = startTime + windowSize;
            // emitting the corresponding time window
            return Collections.singletonList(new TimeWindow(startTime, endTime));
        }

        @Override
        public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
            return EventTimeTrigger.create();
        }

        @Override
        public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
            return new TimeWindow.Serializer();
        }

        @Override
        public boolean isEventTime() {
            return true;
        }
    }

    /**
     * A trigger thet fires early. The trigger fires at most every second. 可提前触发的计时器，触发周期不小于1s
     */
    public static class OneSecondIntervalTrigger extends Trigger<SensorReading, TimeWindow> {

        @Override
        public TriggerResult onElement(SensorReading r, long ts, TimeWindow w, TriggerContext ctx) throws Exception {
            // firstSeen will be false if not set yet
            ValueState<Boolean> firstSeen = ctx.getPartitionedState(
                    new ValueStateDescriptor<>("firstSeen", Types.BOOLEAN));

            // register initial timer only for first element
            if (firstSeen.value() == null) {
                // compute time for next early firing by rounding watermark to second
                long t = ctx.getCurrentWatermark() + (1000 - (ctx.getCurrentWatermark() % 1000));
                ctx.registerEventTimeTimer(t);
                // register timer for the end of the window
                ctx.registerEventTimeTimer(w.getEnd());
                firstSeen.update(true);
            }
            // Continue. Do not evaluate window per element
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long ts, TimeWindow w, TriggerContext ctx) throws Exception {
            if (ts == w.getEnd()) {
                // final evaluation and purge window state
                return TriggerResult.FIRE_AND_PURGE;
            } else {
                // register next early firing timer
                long t = ctx.getCurrentWatermark() + (1000 - (ctx.getCurrentWatermark() % 1000));
                if (t < w.getEnd()) {
                    ctx.registerEventTimeTimer(t);
                }
                // fire trigger to early evaluate window
                return TriggerResult.FIRE;
            }
        }

        @Override
        public TriggerResult onProcessingTime(long ts, TimeWindow w, TriggerContext ctx) throws Exception {
            // Continue. We don't use processing time timers
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow w, TriggerContext ctx) throws Exception {
            // clear trigger state
            ValueState<Boolean> firstSeen = ctx.getPartitionedState(
                    new ValueStateDescriptor<>("firstSeen", Types.BOOLEAN));
            firstSeen.clear();
        }
    }

    /**
     * A window function that counts the readings per sensor and window.
     * The function emits the sensor id, window end, tiem of function evaluation, and count.
     */
//    计算每个窗口内的最低和最高温，每个窗口都会发出一条记录，包含了窗口的开始、技术时间以及窗口内的最低、最高温度
    public static class CountFunction
            extends ProcessWindowFunction<SensorReading, Tuple4<String, Long, Long, Integer>, String, TimeWindow> {

        //        对窗口执行计算
        @Override
        public void process(
                String id,
                Context ctx,
                Iterable<SensorReading> readings,
                Collector<Tuple4<String, Long, Long, Integer>> out) throws Exception {
            // count readings
            int cnt = 0;
            for (SensorReading r : readings) {
                cnt++;
            }
            // get current watermark
            long evalTime = ctx.currentWatermark();
            // emit result
            out.collect(Tuple4.of(id, ctx.window().getEnd(), evalTime, cnt));
        }
    }

    // 输入， 累加器， 输出
    public static class AvgTempFunction implements AggregateFunction<Tuple2<String, Double>, Tuple3<String, Double,
            Integer>, Tuple2<String, Double>> {
        @Override
        public Tuple3<String, Double, Integer> createAccumulator() {
            return new Tuple3<>("", 0.0, 0);
        }

        // 温度总和，事件次数
        @Override
        public Tuple3<String, Double, Integer> add(Tuple2<String, Double> input, Tuple3<String, Double,
                Integer> agg) {
            return new Tuple3<>(input.f0, input.f1 + agg.f1, 1 + agg.f2);
        }

        // 根据累加器计算并返回结果
        @Override
        public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> agg) {
            return new Tuple2<>(agg.f0, agg.f1 / agg.f2);
        }

        @Override
        public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> agg1,
                                                     Tuple3<String, Double, Integer> agg2) {
            return new Tuple3<>(agg1.f0, agg1.f1 + agg2.f1, agg1.f2 + agg2.f2);
        }
    }

    public static class MinMaxTemp {
        String id;
        Double min;
        Double max;
        Long endTs;

        public MinMaxTemp() {
        }

        public MinMaxTemp(String id, Double min, Double max, Long endTs) {
            this.id = id;
            this.min = min;
            this.max = max;
            this.endTs = endTs;
        }

        public String toString() {
            return "(" + this.id + ", " + this.min + ", " + this.max + ", " + this.endTs + ")";
        }
    }

    public static class HighAndLowTempProcessFunction extends ProcessWindowFunction<SensorReading, MinMaxTemp, String
            , TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<SensorReading> elements, Collector<MinMaxTemp> out) throws Exception {
            double min = Double.MAX_VALUE, max = Double.MIN_VALUE;
            Iterator<SensorReading> iterator = elements.iterator();
            while (iterator.hasNext()) {
                Double next = iterator.next().temperature;
                min = Math.min(min, next);
                max = Math.max(max, next);
            }
            out.collect(new MinMaxTemp(key, min, max, context.window().getEnd()));
        }
    }

    public static class AssignWindowEndProcessFunction extends ProcessWindowFunction<Tuple3<String, Double, Double>,
            MinMaxTemp, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Tuple3<String, Double, Double>> elements,
                            Collector<MinMaxTemp> out) throws Exception {
            Tuple3<String, Double, Double> minMax = elements.iterator().next();
            out.collect(new MinMaxTemp(s, minMax.f1, minMax.f2, context.window().getEnd()));
        }
    }
}
