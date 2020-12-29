package com.claroja.window;

import com.claroja.model.SensorReading;
import com.claroja.readwrite.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class AggOperatorWholeWin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        stream
                .keyBy(r -> r.id)
                .timeWindow(Time.seconds(5))
                .process(new AvgTempPerWindow())
                .print();

        env.execute();
    }
    //全窗口函数相比累加窗口函数好的优势是可以获得窗口的上下文信息,比如窗口的开始时间和结束时间
    public static class AvgTempPerWindow extends ProcessWindowFunction<SensorReading, String, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<SensorReading> iterable, Collector<String> collector) throws Exception {
            Double sum = 0.0;
            Long count = 0L;
            for (SensorReading r : iterable) {
                sum += r.temperature;
                count += 1L;
            }
            collector.collect("传感器为：" + key + " 窗口结束时间为：" + new Timestamp(ctx.window().getEnd()) + " 的平均值是：" + sum / count);
        }
    }
}
