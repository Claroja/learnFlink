package com.claroja.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class CusWaterMark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
//        env.getConfig().setAutoWatermarkInterval(60 * 1000L); //默认200ms更新水位线,每进来一个event都记录最新的时间,但200ms才会更新

        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);
        stream
                .map(new MapFunction<String, Tuple2<String, Long>>() {// Tuple2<key, timestamp>，事件事件必须是毫秒时间戳
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] arr = s.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(// 水位线必须在keyby之前
                        new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
                            //设置最大延迟时间
                            final long bound = 5 * 1000L;
                            //系统观察到的元素包含的最大时间戳
                            long maxTs = Long.MIN_VALUE + bound + 1;

                            // 每来一条数据执行一次
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                maxTs = Math.max(maxTs, stringLongTuple2.f1); // 更新观察到的最大的事件时间
                                return stringLongTuple2.f1; // 告诉系统哪一个字段是事件时间
                            }
                            // 系统在流中插入水位线时执行，默认200ms执行一次
                            @Override
                            public Watermark getCurrentWatermark() {
                                return new Watermark(maxTs - bound - 1);
                            }
                        }
                )
                .keyBy(r -> r.f0)
                .timeWindow(Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {
                        long count = 0L;
                        for (Tuple2<String, Long> i : iterable) {
                            count += 1;
                        }
                        collector.collect("窗口中共有 " + count + " 条元素");
                    }
                })
                .print();

        env.execute();
    }
}
