package com.prey.flow.analysis;


import com.prey.flow.bean.PageViewCount;
import com.prey.flow.bean.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.net.URL;

public class UniqueVisitor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        URL resource = PagesView.class.getResource("/data.log");
        SingleOutputStreamOperator<UserBehavior> dataStream = env.readTextFile(resource.getPath()).map(line -> {
            String[] fileds = line.split(",");
            return new UserBehavior(new Long(fileds[0]), new Long(fileds[1]), new Integer(fileds[2]), fileds[3], new Long(fileds[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });

        // 开窗统计uv值
            dataStream.filter(data -> "pv".equals(data.getBehavior()))
                    .timeWindowAll(Time.hours(1))
                    .trigger(new MyTrigger())
                    .process(new UvCountResultWithBloomFilter())
                    .print("unique visitor");
            env.execute("unique visitor job");
    }

    public static class UvCountResultWithBloomFilter extends ProcessAllWindowFunction<UserBehavior, PageViewCount,TimeWindow>{
         Jedis jedis;
         MyBloomFilter myBloomFilter;


        @Override
        public void process(ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow>.Context context, Iterable<UserBehavior> iterable, Collector<PageViewCount> collector) throws Exception {
            // 将位图和窗口count值存入redis，用windowEnd做key
            Long windowEnd = context.window().getEnd();
            String bitmapKey = windowEnd.toString();
            // 把count值存成一张hash表
            String countHashName = "un_count";
            String countKey = windowEnd.toString();
            Long userId  = iterable.iterator().next().getUserId();
            // 计算位图中的 offset
            Long offset = myBloomFilter.haseCode(userId.toString(),61);
            // redis getbit命令判断对于位置的值
            Boolean isExist = jedis.getbit(bitmapKey,offset);

            if(!isExist){
                // 如果不存在，对应位图位置 置为1
                jedis.setbit(bitmapKey,offset,true);
                Long uvCount = 0L;
                String uvCountStr = jedis.hget(countHashName,countKey);
                if( uvCountStr != null && !"".equals(uvCountStr)){
                    uvCount = Long.valueOf(uvCountStr);
                }
                jedis.hset(countHashName,countKey,String.valueOf(uvCount + 1));
                collector.collect(new PageViewCount("uv",windowEnd,uvCount));
            }

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("localhost",6379);
            // 如果处理数据一亿字节 换布隆过滤器大概只要64mb
            myBloomFilter = new MyBloomFilter(1 << 29);
        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }

    // 自定义一个布隆过滤器
    public static class MyBloomFilter{
        private Integer cap;

        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }
        // 实现一个hash函数
        public Long haseCode(String value,Integer seed){
            Long result = 0L;
            for(int i = 0; i <value.length(); i++){
                result = result * seed + value.charAt(i);
            }
            return result & (cap - 1);
        }

    }


    public static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {
        // 触发器 continue fire fire_and_purge purge
        @Override
        public TriggerResult onElement(UserBehavior userBehavior, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        }
    }

}
