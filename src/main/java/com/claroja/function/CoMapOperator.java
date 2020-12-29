package com.claroja.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;


public class CoMapOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String,Integer>> stream1 = env.fromElements(new Tuple2("wang",120),new Tuple2("wei",130));
        DataStream<Tuple2<String,Integer>> stream2 = env.fromElements(new Tuple2("wang",16),new Tuple2("wei",18));
        //两种不同的keyby方式
//        ConnectedStreams<Tuple2<String,Integer>,Tuple2<String,Integer>> connected = stream1.keyBy(0).connect(stream2.keyBy(0));
        ConnectedStreams<Tuple2<String,Integer>,Tuple2<String,Integer>> connected = stream1.connect(stream2).keyBy(0,0);
        connected.map(new MyCoMapFunction()).print();
        env.execute();

    }

    static class MyCoMapFunction implements CoMapFunction<Tuple2<String,Integer>,Tuple2<String,Integer>,String>{
        //map1来自第一条流的元素
        @Override
        public String map1(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
            return stringIntegerTuple2.getField(0) +"\t"+stringIntegerTuple2.getField(1);
        }
        //map2来自第二条流的元素
        @Override
        public String map2(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
            return stringIntegerTuple2.getField(0) +"\t"+stringIntegerTuple2.getField(1);
        }
    }
}

/*
6> wei	130
6> wei	18
1> wang	120
1> wang	16
 */