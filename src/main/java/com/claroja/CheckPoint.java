package com.claroja;

import com.claroja.model.SensorReading;
import com.claroja.readwrite.SensorSource;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CheckPoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 每隔10s做一次保存检查点操作
        env.enableCheckpointing(10 * 1000L);
        // 设置状态后端
        env.setStateBackend(new FsStateBackend("file:///E:\\"));
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());
        stream.print();
        env.execute();
    }
}
