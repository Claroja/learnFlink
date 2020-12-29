package com.claroja.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichMapOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.fromElements("hello");
        stream.map(new MyRichMap()).print();
        env.execute();
    }

    static class MyRichMap extends RichMapFunction<String,String>{
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("生命周期开始");
        }

        @Override
        public void close() throws Exception {
            System.out.println("生命周期开始");
        }

        @Override
        public String map(String s) throws Exception {
            RuntimeContext context = getRuntimeContext();
            return String.valueOf(context);
        }
    }
}
/*
生命周期开始
生命周期开始
生命周期开始
生命周期开始
生命周期开始
生命周期开始
3> org.apache.flink.streaming.api.operators.StreamingRuntimeContext@370b7ea9
生命周期开始
生命周期开始
生命周期开始
生命周期开始
生命周期开始
生命周期开始
 */