package com.claroja.function;

import com.claroja.model.SensorReading;
import com.claroja.readwrite.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> stream = env.addSource(new SensorSource());
        DataStream<SensorReading> BJ = env.addSource(new SensorSource()).filter(x->x.id.equals("sensor_1"));
        DataStream<SensorReading> SH = env.addSource(new SensorSource()).filter(x->x.id.equals("sensor_2"));
        DataStream<SensorReading> union = BJ.union(SH);
        union.print();
        env.execute();
    }
}

/*
2> SensorReading{id='sensor_1', timestamp=1607762743631, temperature=0.6597404450484071}
4> SensorReading{id='sensor_1', timestamp=1607762743631, temperature=0.10573279789826416}
6> SensorReading{id='sensor_1', timestamp=1607762743628, temperature=-0.7438768226576186}
1> SensorReading{id='sensor_1', timestamp=1607762743630, temperature=1.3444626601351608}
6> SensorReading{id='sensor_2', timestamp=1607762743628, temperature=-0.31310672347396085}
1> SensorReading{id='sensor_2', timestamp=1607762743630, temperature=-0.1394104754634639}
5> SensorReading{id='sensor_2', timestamp=1607762743628, temperature=-0.43607070251378344}
3> SensorReading{id='sensor_2', timestamp=1607762743630, temperature=0.9027633464017715}
 */