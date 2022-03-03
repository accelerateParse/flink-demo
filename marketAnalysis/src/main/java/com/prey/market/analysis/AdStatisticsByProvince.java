package com.prey.market.analysis;

import com.prey.market.bean.AdClickEvent;
import com.prey.market.bean.AdCountViewByProvince;
import com.prey.market.bean.BlackListUserWarming;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;

public class AdStatisticsByProvince {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        URL adResource = AdStatisticsByProvince.class.getResource("/data.txt");
        SingleOutputStreamOperator<AdClickEvent> filterBlackUserStream = env.readTextFile(adResource.getPath()).map(line -> {
                    String[] str = line.split(",");
                    return new AdClickEvent(new Long(str[0]), new Long(str[1]), str[2], str[3], new Long(str[4]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
                    @Override
                    public long extractAscendingTimestamp(AdClickEvent adClickEvent) {
                        return adClickEvent.getTimestamp() * 1000L;
                    }
                }).keyBy("userId", "adId")
                .process(new FilterBlackListUser(10));
        filterBlackUserStream
                .keyBy(AdClickEvent::getProvince)
                .timeWindow(Time.hours(1), Time.minutes(5))
                 .aggregate(new AdCountAgg(), new AdCountResult()).print("ad count by province");
        filterBlackUserStream.getSideOutput(new OutputTag<BlackListUserWarming>("blackList")).print("black list user");
        env.execute("ad count");


    }

    public static class AdCountResult implements WindowFunction<Long, AdCountViewByProvince,String , TimeWindow> {

        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<AdCountViewByProvince> collector) throws Exception {
            String windEnd = new Timestamp(timeWindow.getEnd()).toString();
            Long count = iterable.iterator().next();
            collector.collect(new AdCountViewByProvince(s, windEnd,count));
        }
    }

    public static class AdCountAgg implements AggregateFunction<AdClickEvent,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent adClickEvent, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class FilterBlackListUser extends KeyedProcessFunction<Tuple,AdClickEvent,AdClickEvent>{

        private Integer countUpperBound;
        private  FilterBlackListUser(Integer countUpperBound){
            this.countUpperBound = countUpperBound;
        }
        // 用户点击次数
        ValueState<Long> countState;
        // 是否已经在黑名单里
        ValueState<Boolean> isSentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ad-count",Long.class,0L));
            isSentState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-sent",Boolean.class,false));
        }

        @Override
        public void processElement(AdClickEvent adClickEvent, KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent>.Context context, Collector<AdClickEvent> collector) throws Exception {
            Long curCount = countState.value();
            // 判断是否第一个数据
            if(curCount == 0){
                Long ts = (context.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) +1) * (24 * 60 *60 *1000) - 8 * 60 * 60 * 1000;
                context.timerService().registerEventTimeTimer(ts);
            }
            // 判断是否报警
            if(curCount >= countUpperBound){
                // 判断是否输出到黑名单过，如果没有则输出到侧输出流
                if(!isSentState.value()){
                    isSentState.update(true);
                    context.output(new OutputTag<BlackListUserWarming>("blackList"),
                            new BlackListUserWarming(adClickEvent.getUserId(), adClickEvent.getAdId(), "click over" + countUpperBound));
                }
                return;
            }
            countState.update(curCount + 1);
            collector.collect(adClickEvent);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent>.OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            countState.clear();
            isSentState.clear();
        }
    }
}
