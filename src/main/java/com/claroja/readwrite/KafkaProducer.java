package com.claroja.readwrite;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

public class KafkaProducer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //创建消费者
        DataStreamSource<String> stream = env.fromElements("test");
        Properties PropertiesProducer = new Properties();
        PropertiesProducer.put("bootstrap.servers", "localhost:9092");
        stream.addSink(new FlinkKafkaProducer011<String>(
                "test",
                new SimpleStringSchema(),
                PropertiesProducer
        ));

        stream.print();

        env.execute();
    }
}
