package io.github.streamingwithflink.chapter6;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @ClassName WaterMarkGeneration
 * @Description //TODO
 * @Author fangjiaxin
 * @Date 2021/8/9
 */
public class WaterMarkGeneration {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use event time for the application
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // configure watermark interval 周期性水位线分配器 1000ms
        env.getConfig().setAutoWatermarkInterval(1000L);

        // ingest sensor stream
        DataStream<SensorReading> readingsWithPeriodicWMs = env
                // SensorSource generates random temperature readings
                .addSource(new SensorSource())
                // assign timestamps and watermarks which are required for event time
                .assignTimestampsAndWatermarks(new PeriodicAssigner());
//        怎么观测水位线的推移
        readingsWithPeriodicWMs.print();
        env.execute("Run custom window example");
    }

    private static class PeriodicAssigner implements AssignerWithPeriodicWatermarks<SensorReading> {

        Long bound = 60 * 1000L;
        // todo: 该状态存储在哪？会不会有不一致的现象？不会，每个并行任务处理自己的
        Long maxTs = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTs - bound);
        }

        @Override
        public long extractTimestamp(SensorReading element, long previousElementTimestamp) {
            maxTs = Math.max(maxTs, element.timestamp);
            return element.timestamp;
        }
    }

    private static class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<SensorReading> {
        Long bound = 60 * 1000L;

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(SensorReading lastElement, long extractedTimestamp) {
            if (lastElement.id == "sensor_1") {
                return new Watermark(extractedTimestamp - bound);
            } else {
                return null;
            }
        }

        @Override
        public long extractTimestamp(SensorReading element, long previousElementTimestamp) {
            return element.timestamp;
        }
    }
}
