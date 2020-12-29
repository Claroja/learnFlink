package com.claroja.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class FlatMapOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.fromElements("white","black","gray");
        stream.flatMap(new MyFlatMapOperator2()).print();
        env.execute();
    }
    //'flatMap'针对流中的每一个元素生成，0个，1个或多个元素，可以实现map filter
    static class MyFlatMapOperator implements FlatMapFunction<String,String>{
        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            if(s.equals("white")){
                collector.collect(s);
            } else if(s.equals("black")){
                collector.collect(s);
                collector.collect(s);
            }
        }
    }

    //'flatMap'实现map功能
    static class MyFlatMapOperator1 implements FlatMapFunction<String,String>{
        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
                collector.collect(s+"1");
        }
    }
    //'flatMap'实现filter功能
    static class MyFlatMapOperator2 implements FlatMapFunction<String,String>{
        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            if(s.equals("white")){
                collector.collect(s);
            }
        }
    }
}


/*
1> white

 */