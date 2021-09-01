package io.github.streamingwithflink.chapter6;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName ProcessFunctionTimers
 * @Description //TODO
 * @Author fangjiaxin
 * @Date 2021/8/10
 */
public class ProcessFunctionTimers {
    //     保存两个状态
//    1. 上一个时间 2.开始递增的传感器 timer：时间
//    FreezingMonitor 控传 器读数流，它会在遇到读数温度低于32度时向副输出发送警告。
    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use event time for the application
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        // switch messages disable filtering of sensor readings for a specific amount of time
        DataStream<SensorReading> readings =
                env.addSource(new SensorSource()).assignTimestampsAndWatermarks(new SensorTimeAssigner());

        SingleOutputStreamOperator<SensorReading> monitoredReadings = readings.process(new FreezingMonitor());
        monitoredReadings.getSideOutput(new OutputTag<String>("freezing-alarms"){}).print();

        env.execute("Filter sensor readings");
    }

    public static class FreezingMonitor extends ProcessFunction<SensorReading, SensorReading> {

        OutputTag<String> freezingAlarmOutput = new OutputTag<String>("freezing-alarms") {
        };

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            if (value.temperature < 32.0) {
                ctx.output(freezingAlarmOutput, String.format("Freezing Alarm for %s", value.id));
            }
            // 将数据发送到常规输出
            out.collect(value);
        }
    }

}
