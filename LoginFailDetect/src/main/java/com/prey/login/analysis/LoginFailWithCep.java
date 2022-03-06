package com.prey.login.analysis;

import com.prey.login.bean.LoginEvent;
import com.prey.login.bean.LoginFailWarming;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;
import java.util.List;
import java.util.Map;

public class LoginFailWithCep {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        URL resource = LoginFail.class.getResource("/data.txt");
        SingleOutputStreamOperator<LoginEvent> loginEventStream = env.readTextFile(resource.getPath()).map(line -> {
            String[] fields = line.split(",");
            return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(LoginEvent loginEvent) {
                 return loginEvent.getTimestamp() * 1000L;
            }

        });
        // 定义匹配模式
        Pattern<LoginEvent, LoginEvent> loginFailPattern2 = Pattern.<LoginEvent>begin("failEvent")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getLoginState());
                    }
                }).times(3)
                .consecutive()
                .within(Time.seconds(5));
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream.keyBy(LoginEvent::getUserId), loginFailPattern2);

         patternStream.select(new LoginFailMatchDetectWarming()).print();

         env.execute("LoginFail With Cep");


    }


    public static class LoginFailMatchDetectWarming implements PatternSelectFunction<LoginEvent, LoginFailWarming>{

        @Override
        public LoginFailWarming select(Map<String, List<LoginEvent>> map) throws Exception {
            List<LoginEvent> events = map.get("failEvent");
            LoginEvent first = events.get(0);
            LoginEvent second = events.get(events.size());
            return new LoginFailWarming(first.getUserId(), first.getTimestamp().toString(),second.getTimestamp().toString(),"login fail two times");
        }
    }


}

