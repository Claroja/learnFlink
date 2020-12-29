package com.claroja;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Parallelism {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println(env.getParallelism());
        DataStream<String> stream = env.fromElements("white","black","gray").setParallelism(1);
        stream.print();
        env.execute();
    }
}
