package com.prey.login.analysis;

import com.prey.login.bean.LoginEvent;
import com.prey.login.bean.LoginFailWarming;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.Iterator;

public class LoginFail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        URL resource = LoginFail.class.getResource("/data.txt");
        SingleOutputStreamOperator<LoginEvent> loginEventStream = env.readTextFile(resource.getPath()).map(line -> {
            String[] fields = line.split(",");
            return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LoginEvent>() {
            @Override
            public long extractAscendingTimestamp(LoginEvent data) {
                return data.getTimestamp() * 1000L;
            }
        });
        loginEventStream.keyBy(LoginEvent::getUserId).process(new LoginFailDetectWarming(2)).print();
        env.execute("LoginFail detect");
    }

    public static class LoginFailDetectWarming extends KeyedProcessFunction<Long,LoginEvent, LoginFailWarming>{
        private Integer maxFailTimes;
        public LoginFailDetectWarming(Integer max){
            this.maxFailTimes = max;
        }
        // 保存两秒内所有登录失败事件
        ListState<LoginEvent> loginEventListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list",LoginEvent.class));
        }

        @Override
        public void processElement(LoginEvent loginEvent, KeyedProcessFunction<Long, LoginEvent, LoginFailWarming>.Context context, Collector<LoginFailWarming> collector) throws Exception {
                if("fail".equals(loginEvent.getLoginState())){
                    // 获取当前登录失败事件继续判断是否有失败事件
                    Iterator<LoginEvent> iterator =loginEventListState.get().iterator();
                    if(iterator.hasNext()){
                        // 如果有登陆失败事件判断时间戳是否在两秒之内
                        LoginEvent pre = iterator.next();
                        if(loginEvent.getTimestamp() - pre.getTimestamp() <=2){
                            collector.collect(new LoginFailWarming(loginEvent.getUserId(),pre.getTimestamp().toString(),loginEvent.getTimestamp().toString(),"login fail  2 times"));
                        }
                        loginEventListState.clear();
                        loginEventListState.add(loginEvent);
                    }else{
                        loginEventListState.add(loginEvent);
                    }
                }else{
                    loginEventListState.clear();
                }
        }
    }
}
