package com.claroja.function;

import com.claroja.model.SensorReading;
import com.claroja.readwrite.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedStreamOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> stream = env.addSource(new SensorSource()).filter(x->x.id.equals("sensor_1"));
        KeyedStream<SensorReading,String> keyedStream = stream.keyBy(sensorReading -> sensorReading.id);
        //会保持最小值的状态
        keyedStream.min("temperature").print();
        env.execute();
    }
}

/*
4> SensorReading{id='sensor_1', timestamp=1607781524965, temperature=-2.3132350358009575}
4> SensorReading{id='sensor_1', timestamp=1607781524965, temperature=-2.3132350358009575}
4> SensorReading{id='sensor_1', timestamp=1607781524965, temperature=-2.3132350358009575}
4> SensorReading{id='sensor_1', timestamp=1607781524965, temperature=-2.3132350358009575}
4> SensorReading{id='sensor_1', timestamp=1607781524965, temperature=-2.3132350358009575}
4> SensorReading{id='sensor_1', timestamp=1607781524965, temperature=-2.3132350358009575}
4> SensorReading{id='sensor_1', timestamp=1607781524965, temperature=-2.3132350358009575}
4> SensorReading{id='sensor_1', timestamp=1607781524965, temperature=-2.3132350358009575}
 */