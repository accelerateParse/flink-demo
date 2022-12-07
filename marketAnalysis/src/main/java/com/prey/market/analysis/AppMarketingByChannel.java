package com.prey.market.analysis;

import com.prey.market.bean.AdClickEvent;
import com.prey.market.bean.ChannelPromotionCount;
import com.prey.market.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.*;

public class AppMarketingByChannel {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<MarketingUserBehavior> dataStream = env.addSource(new SimulateMarketingUserBehaviorSource());
        SingleOutputStreamOperator<String> resultSteam = dataStream.filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .keyBy(MarketingUserBehavior::getChannel)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .process(new ProcessWindowFunction<MarketingUserBehavior, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<MarketingUserBehavior, String, String, TimeWindow>.Context context, Iterable<MarketingUserBehavior> iterable, Collector<String> collector) throws Exception {
                        HashMap<String, Integer> result = new HashMap<>();
                        Iterator<MarketingUserBehavior> iterator = iterable.iterator();

                        while (iterator.hasNext()) {
                            MarketingUserBehavior next = iterator.next();
                            Integer count = result.getOrDefault(next.getBehavior(), 0) + 1;
                            result.put(next.getBehavior(), count);
                        }
                        for (String key : result.keySet()) {
                            collector.collect("channel:" + s + " behavior:" + key + "count:" + result.get(key));
                        }
                    }
                });

        resultSteam.print();

        env.execute("AppMarketingByChannel");

    }

    public static class SimulateMarketingUserBehaviorSource implements SourceFunction<MarketingUserBehavior>{
        Boolean running = true;

        List<String> behaviorList = Arrays.asList("CLICK","DOWNLOAD","INSTALL","UNINSTALL");
        List<String> channelList = Arrays.asList("app store","wechat","weibo");
        Random random = new Random();

        @Override
        public void run(SourceContext<MarketingUserBehavior> sourceContext) throws Exception {
            while (running) {
                String behavior = behaviorList.get(random.nextInt(behaviorList.size()));
                String channel = channelList.get(random.nextInt(channelList.size()));
                Long timestamp = System.currentTimeMillis();
                Long id = random.nextLong();
                sourceContext.collect(new MarketingUserBehavior(id, behavior, channel, timestamp));
                Thread.sleep(100);
            }
        }
        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class MarketingCountResult extends ProcessWindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow>{

        @Override
        public void process(Tuple tuple, ProcessWindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow>.Context context, Iterable<Long> iterable, Collector<ChannelPromotionCount> collector) throws Exception {
            String channel = tuple.getField(0);
            String behavior = tuple.getField(1);
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            Long count = iterable.iterator().next();
            collector.collect(new ChannelPromotionCount(channel,behavior,windowEnd,count));
        }
    }

    public static class MarketingCountAgg implements AggregateFunction<MarketingUserBehavior,Long, Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketingUserBehavior marketingUserBehavior, Long a) {
            return a + 1;
        }

        @Override
        public Long getResult(Long a) {
            return a;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }
}
