package com.claroja.readwrite;

import com.claroja.model.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ConsumerFromSensorSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<SensorReading> stream = env.addSource(new SensorSource());
        stream.print();
        env.execute();
    }
}

/*
SensorReading{id='sensor_0', timestamp=1607780932062, temperature=1.308847778859833}
SensorReading{id='sensor_1', timestamp=1607780932062, temperature=1.308847778859833}
SensorReading{id='sensor_2', timestamp=1607780932062, temperature=1.308847778859833}
SensorReading{id='sensor_3', timestamp=1607780932062, temperature=1.308847778859833}
SensorReading{id='sensor_4', timestamp=1607780932062, temperature=1.308847778859833}
SensorReading{id='sensor_5', timestamp=1607780932062, temperature=1.308847778859833}
SensorReading{id='sensor_6', timestamp=1607780932062, temperature=1.308847778859833}
SensorReading{id='sensor_7', timestamp=1607780932062, temperature=1.308847778859833}
SensorReading{id='sensor_8', timestamp=1607780932062, temperature=1.308847778859833}
SensorReading{id='sensor_9', timestamp=1607780932062, temperature=1.308847778859833}
SensorReading{id='sensor_0', timestamp=1607780932172, temperature=2.190754491899719}
 */