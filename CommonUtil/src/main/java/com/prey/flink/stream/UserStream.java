package com.prey.flink.stream;

import com.alibaba.fastjson.JSONObject;
import com.prey.flink.source.PostgresSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.*;

public class UserStream {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty("group.id","user666");
        kafkaConfig.setProperty("bootstrap.server","localhost:10004");
        List<String> topicList = new ArrayList<>();
        topicList.add("user1");
        FlinkKafkaConsumer<String> userSource = new FlinkKafkaConsumer<>(topicList, new SimpleStringSchema(), kafkaConfig);
        DataStreamSource<String> userStream = env.addSource(userSource);

        Properties postgresConfig = new Properties();
        postgresConfig.setProperty("username","root");
        postgresConfig.setProperty("url","jdbc:postgresql://lcoalhost:5400/userdb?userUnicode=ture&amp;characterEncoding=UTF-8&amp;autoReconnect=true");
        postgresConfig.setProperty("password","123456");
        BroadcastStream<HashMap<String, JSONObject>> userBroadcast = env.addSource(new PostgresSource(postgresConfig)).broadcast(userBroadcastDesc);
        userStream.connect(userBroadcast).process(new UserBroadcastFunction());
    }

    private static final MapStateDescriptor<String, HashMap<String, JSONObject>> userBroadcastDesc = new MapStateDescriptor<>(
            "userBroadcast",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<HashMap<String, JSONObject>>() {
            }));

    public static class UserBroadcastFunction extends BroadcastProcessFunction<String, HashMap<String, JSONObject>, String> {

        @Override
        public void processElement(String s, BroadcastProcessFunction<String, HashMap<String, JSONObject>, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
            HashMap<String, JSONObject> userInfoMap = readOnlyContext.getBroadcastState(userBroadcastDesc).get("userInfo");
            JSONObject userInfo = userInfoMap.get(s);
            collector.collect(userInfo.toString());
        }

        @Override
        public void processBroadcastElement(HashMap<String, JSONObject> stringJSONObjectHashMap, BroadcastProcessFunction<String, HashMap<String, JSONObject>, String>.Context context, Collector<String> collector) throws Exception {
            BroadcastState<String, HashMap<String, JSONObject>> broadcastState = context.getBroadcastState(userBroadcastDesc);
            broadcastState.put("userInfo",stringJSONObjectHashMap);
        }
    }
}
