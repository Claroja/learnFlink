package com.claroja.function;

import com.claroja.model.SensorReading;
import com.claroja.readwrite.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> stream = env.addSource(new SensorSource());
//        stream.map(x->x.id).print();
        stream.map(new MyMapOperator()).print(); //使用接口形式
        env.execute();
    }


    static class MyMapOperator implements MapFunction<SensorReading, String> {
        @Override
        public String map(SensorReading sensorReading) throws Exception {
            return sensorReading.id;
        }
    }


}
/*
2> sensor_0
2> sensor_1
5> sensor_0
4> sensor_0
4> sensor_1
6> sensor_0
3> sensor_0
1> sensor_0
 */