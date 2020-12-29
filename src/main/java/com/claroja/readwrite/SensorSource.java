package com.claroja.readwrite;

import com.claroja.model.SensorReading;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class SensorSource extends RichParallelSourceFunction<SensorReading> {
    Boolean running = true;
    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {
        Random rand = new Random();
        while(running){
            Long curTime = System.currentTimeMillis();
            for (int i = 0; i < 10; i++){
                Double curTemp = rand.nextGaussian();
                SensorReading sensorReading = new SensorReading("sensor_"+i,curTime,curTemp);
                sourceContext.collect(sensorReading);
            }
            Thread.sleep(100);
        }
    }
    @Override
    public void cancel() {
        running = false;
    }
}
