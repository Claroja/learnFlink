package com.claroja.cep;

import com.claroja.model.CEPLoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class LoginFailDetect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<CEPLoginEvent> stream = env
                .fromElements(
                        new CEPLoginEvent("user_1", "0.0.0.0", "fail", 2000L),
                        new CEPLoginEvent("user_1", "0.0.0.1", "fail", 3000L),
                        new CEPLoginEvent("user_1", "0.0.0.2", "fail", 4000L),
                        new CEPLoginEvent("user_1", "0.0.0.3", "fail", 4500L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<CEPLoginEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<CEPLoginEvent>() {
                                    @Override
                                    public long extractTimestamp(CEPLoginEvent element, long recordTimestamp) {
                                        return element.eventTime;
                                    }
                                })
                );

        // 定义恶意登录的模板
        Pattern<CEPLoginEvent, CEPLoginEvent> pattern1 = Pattern
                .<CEPLoginEvent>begin("first")
                .where(new SimpleCondition<CEPLoginEvent>() {
                    @Override
                    public boolean filter(CEPLoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .next("second")
                .where(new SimpleCondition<CEPLoginEvent>() {
                    @Override
                    public boolean filter(CEPLoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .next("third")
                .where(new SimpleCondition<CEPLoginEvent>() {
                    @Override
                    public boolean filter(CEPLoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                // 5s以内连续三次登录失败
                .within(Time.seconds(5));

        // 连续三次登录失败模板的另一种定义方法
        Pattern<CEPLoginEvent, CEPLoginEvent> pattern2 = Pattern
                .<CEPLoginEvent>begin("first")
                .times(3)
                .where(new SimpleCondition<CEPLoginEvent>() {
                    @Override
                    public boolean filter(CEPLoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .within(Time.seconds(5));

        PatternStream<CEPLoginEvent> patternStream1 = CEP.pattern(stream.keyBy(r -> r.userId), pattern1);

        PatternStream<CEPLoginEvent> patternStream2 = CEP.pattern(stream.keyBy(r -> r.userId), pattern2);

        patternStream1
                .select(new PatternSelectFunction<CEPLoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<CEPLoginEvent>> map) throws Exception {
                        CEPLoginEvent first = map.get("first").iterator().next();
                        CEPLoginEvent second = map.get("second").iterator().next();
                        CEPLoginEvent thrid = map.get("third").iterator().next();

                        return first.ipAddr + "; " + second.ipAddr + "; " + thrid.ipAddr;
                    }
                })
                .print();

        patternStream2
                .select(new PatternSelectFunction<CEPLoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<CEPLoginEvent>> map) throws Exception {
                        for (CEPLoginEvent e : map.get("first")) {
                            System.out.println(e.ipAddr);
                        }
                        return "hello world";
                    }
                })
                .print();

        env.execute();
    }
}
