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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * This example shows how to use a CoProcessFunction and Timers.
 * 使用场景参考：https://www.hnbian.cn/posts/7d5aa59b.html
 */
public class CoProcessFunctionTimers {

    //     保存两个状态
//    1. 上一个时间 2.开始递增的传感器 timer：时间
    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use event time for the application
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // switch messages disable filtering of sensor readings for a specific amount of time
        DataStream<SensorReading> readings = env.addSource(new SensorSource());

        readings.keyBy(r -> r.id)
                .process(new TempIncreaseAlertFunction()).print();

        env.execute("Filter sensor readings");
    }

    public static class TempIncreaseAlertFunction extends KeyedProcessFunction<String, SensorReading, String> {

        // 上一条记录温度
        ValueState<Double> lastTemp;
        // 上一个timer， timer是key级别的共享变量嘛？
        ValueState<Long> timer;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTemp",
                    Types.DOUBLE));
            timer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time", Types.LONG));

        }

        // 回调函数
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("传感器的问题" + ctx.getCurrentKey() + "单调增加了一分钟，时间为" + timer.value());
            // 清空，准备下一个告警
            timer.clear();

        }

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<String> collector) throws Exception {
            // 更新最新温度
            Double pre = lastTemp.value();
            lastTemp.update(sensorReading.temperature);

            Long curTimerStamp = timer.value();
            if (pre == null) {

            } else if (sensorReading.temperature < pre && curTimerStamp != null) {
                // todo：取消注册， 为什么入参是时间戳？
                context.timerService().deleteProcessingTimeTimer(curTimerStamp);
                // 清空状态
                timer.clear();
            } else if (sensorReading.temperature > pre && curTimerStamp == null) {
                // 没注册过
                Long timerTs = context.timerService().currentProcessingTime() + 1000L;
                context.timerService().registerProcessingTimeTimer(timerTs);
                timer.update(timerTs);
            }
        }
    }
}


