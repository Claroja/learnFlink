package com.claroja.window;

import com.claroja.model.HighLowTemp;
import com.claroja.model.SensorReading;
import com.claroja.readwrite.SensorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AccAndWholeWin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());
        stream
                .keyBy(r -> r.id)
                .timeWindow(Time.seconds(5))
                .aggregate(new Agg(), new WindowResult())
                .print();
        env.execute();
    }

    //Agg没有改变
    public static class Agg implements AggregateFunction<SensorReading, Tuple3<String, Double, Double>, Tuple3<String, Double, Double>> {
        @Override
        public Tuple3<String, Double, Double> createAccumulator() {
            return Tuple3.of("", Double.MAX_VALUE, Double.MIN_VALUE);
        }

        @Override
        public Tuple3<String, Double, Double> add(SensorReading r, Tuple3<String, Double, Double> agg) {
            return Tuple3.of(r.id, Math.min(r.temperature, agg.f1), Math.max(r.temperature, agg.f2));
        }

        @Override
        public Tuple3<String, Double, Double> getResult(Tuple3<String, Double, Double> agg) {
            return agg;
        }

        @Override
        public Tuple3<String, Double, Double> merge(Tuple3<String, Double, Double> stringDoubleDoubleTuple3, Tuple3<String, Double, Double> acc1) {
            return null;
        }
    }

    //每次Agg都经过wholeWin,读取上下文信息,然后再通过wholeWin发送出去
    public static class WindowResult extends ProcessWindowFunction<Tuple3<String, Double, Double>, HighLowTemp, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Tuple3<String, Double, Double>> iterable, Collector<HighLowTemp> collector) throws Exception {
            Tuple3<String, Double, Double> iter = iterable.iterator().next();
            collector.collect(new HighLowTemp(key, iter.f1, iter.f2, context.window().getStart(), context.window().getEnd()));
        }
    }


}
