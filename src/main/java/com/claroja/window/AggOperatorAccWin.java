package com.claroja.window;

import com.claroja.model.SensorReading;
import com.claroja.readwrite.SensorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AggOperatorAccWin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());
        stream
            .keyBy(r -> r.id)
            .timeWindow(Time.seconds(5))
            .aggregate(new AvgAgg())
            .print();
        env.execute();
    }

    public static class AvgAgg implements AggregateFunction<SensorReading, Tuple3<String, Double, Long>, Tuple2<String, Double>> {
        //创建空累加器,每次数据进来后,更新累加器
        @Override
        public Tuple3<String, Double, Long> createAccumulator() {
            return Tuple3.of("", 0.0, 0L);
        }
        //累加器的更新逻辑,温度每进来一个就加一个,f2进行计数
        @Override
        public Tuple3<String, Double, Long> add(SensorReading r, Tuple3<String, Double, Long> acc) {
            return Tuple3.of(r.id, acc.f1 + r.temperature, acc.f2 + 1L);
        }
        //在窗口结束时计算最终输出的结果
        @Override
        public Tuple2<String, Double> getResult(Tuple3<String, Double, Long> acc) {
            return Tuple2.of(acc.f0, acc.f1 / acc.f2);
        }
        //在eventtime和sessionwindow才用到
        @Override
        public Tuple3<String, Double, Long> merge(Tuple3<String, Double, Long> stringDoubleLongTuple3, Tuple3<String, Double, Long> acc1) {
            return null;
        }
    }
}



