package com.claroja.window;

import com.claroja.model.SensorReading;
import com.claroja.readwrite.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class TimeWinOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> stream = env.addSource(new SensorSource());
        KeyedStream<SensorReading,String> keyedStream = stream.keyBy(x->x.id);
        //TimeWindow是重载的,可以一个参数翻滚窗口,第二个加上可以变为滑动窗口
        WindowedStream<SensorReading,String, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(2));
        DataStream<SensorReading> reduce = windowedStream.reduce((x,y)-> new SensorReading(x.id,0L,Double.min(x.temperature,y.temperature)));
        reduce.print();
        env.execute();

    }
}
/*
1> SensorReading{id='sensor_0', timestamp=0, temperature=-1.68399461733873}
4> SensorReading{id='sensor_1', timestamp=0, temperature=-1.386289104562929}
1> SensorReading{id='sensor_3', timestamp=0, temperature=-1.7662465721727982}
4> SensorReading{id='sensor_6', timestamp=0, temperature=-2.553708752406045}
4> SensorReading{id='sensor_5', timestamp=0, temperature=-2.1957939834589055}
5> SensorReading{id='sensor_4', timestamp=0, temperature=-2.042676873361992}
5> SensorReading{id='sensor_7', timestamp=0, temperature=-1.879369583431785}
6> SensorReading{id='sensor_9', timestamp=0, temperature=-1.8012082068879722}
2> SensorReading{id='sensor_2', timestamp=0, temperature=-1.9590283231638568}
3> SensorReading{id='sensor_8', timestamp=0, temperature=-2.1126834371548715}
1> SensorReading{id='sensor_0', timestamp=0, temperature=-2.166066950308937}
6> SensorReading{id='sensor_9', timestamp=0, temperature=-2.0830876562468013}
2> SensorReading{id='sensor_2', timestamp=0, temperature=-2.7231539921766887}
4> SensorReading{id='sensor_1', timestamp=0, temperature=-2.4982491519571632}
5> SensorReading{id='sensor_4', timestamp=0, temperature=-1.9212064066975152}
 */