package com.prey.market.analysis;

import com.prey.market.bean.ChannelPromotionCount;
import com.prey.market.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;


public class AppMarketingStatistics {
    public static void main(String[] args)  throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        SingleOutputStreamOperator<MarketingUserBehavior> dataStream = env.addSource(new AppMarketingByChannel.SimulateMarketingUserBehaviorSource()).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketingUserBehavior>() {
            @Override
            public long extractAscendingTimestamp(MarketingUserBehavior marketingUserBehavior) {
                return marketingUserBehavior.getTimestamp() * 1000;
            }
        });
        // 开窗统计总量
        dataStream.filter(data -> ! "UNINSTALL".equals(data.getBehavior()))
                        .map(new MapFunction<MarketingUserBehavior, Tuple2<String ,Long>>() {
                            @Override
                            public Tuple2<String ,Long> map(MarketingUserBehavior marketingUserBehavior) throws Exception {
                                return new Tuple2<String ,Long>("total",1L);
                            }
                        }).keyBy(0)
                        .timeWindow(Time.hours(1), Time.seconds(5))
                                .aggregate(new MarketingStatisticAgg(), new MarketingStatisticsResult());

        env.execute("AppMarketingStatistics");
    }

    public static class MarketingStatisticAgg implements AggregateFunction<Tuple2<String ,Long>,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> stringLongTuple2, Long a) {
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

    public static class MarketingStatisticsResult implements WindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ChannelPromotionCount> collector) throws Exception {
            String windowEnd = new Timestamp(timeWindow.getEnd()).toString();
            Long count = iterable.iterator().next();
            collector.collect(new ChannelPromotionCount("total","total",windowEnd,count));
        }
    }

}
