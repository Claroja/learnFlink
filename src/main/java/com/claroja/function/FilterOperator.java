package com.claroja.function;

import com.claroja.model.SensorReading;
import com.claroja.readwrite.SensorSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class FilterOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> stream = env.addSource(new SensorSource());
//        stream.filter(x -> x.id.equals("sensor_1")).print();
        stream.filter(new MyFilterOperator()).print();
        env.execute();
    }


    static class MyFilterOperator implements FilterFunction<SensorReading>{
        @Override
        public boolean filter(SensorReading sensorReading) throws Exception {
            return sensorReading.id.equals("sensor_1");
        }
    }

}
/*
1> SensorReading{id='sensor_1', timestamp=1607781342028, temperature=1.3163956493709634}
2> SensorReading{id='sensor_1', timestamp=1607781342028, temperature=1.573019159755242}
3> SensorReading{id='sensor_1', timestamp=1607781342028, temperature=-0.5209101598821565}
4> SensorReading{id='sensor_1', timestamp=1607781342028, temperature=0.9852670133814783}
5> SensorReading{id='sensor_1', timestamp=1607781342028, temperature=-1.378381863903357}
6> SensorReading{id='sensor_1', timestamp=1607781342028, temperature=-1.0826006361332303}
4> SensorReading{id='sensor_1', timestamp=1607781342138, temperature=-1.859706722586013}
2> SensorReading{id='sensor_1', timestamp=1607781342138, temperature=-1.0048111307861023}
 */