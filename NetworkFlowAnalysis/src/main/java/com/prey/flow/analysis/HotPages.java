package com.prey.flow.analysis;

import com.prey.flow.bean.LogEvent;
import com.prey.flow.bean.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class HotPages {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        URL resource = HotPages.class.getResource("/application.log");
        SingleOutputStreamOperator<LogEvent> dataStream = env.readTextFile(resource.getPath()).map(data -> {
            String[] fields = data.split(",");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
            return new LogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LogEvent>(Time.minutes(1)) {
            @Override
            public long extractTimestamp(LogEvent logEvent) {
                return logEvent.getTimestamp();
            }
        });
        OutputTag<LogEvent> lateTap = new OutputTag<>("late");
        SingleOutputStreamOperator<PageViewCount> aggregate = dataStream.filter(data -> "GET".equals(data.getMethod()))
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                }).keyBy(LogEvent::getUrl)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTap)
                .aggregate(new PageCountAgg(), new PageCountResult());
        DataStream<LogEvent> sideOutput = aggregate.getSideOutput(lateTap);
        aggregate.keyBy(PageViewCount::getWindowEnd).process(new TopHotPages(10)).print("top 10 pages");
    }

    public static class PageCountAgg implements AggregateFunction<LogEvent,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(LogEvent logEvent, Long a) {
            return a + 1;
        }

        @Override
        public Long getResult(Long a) {
            return a;
        }

        @Override
        public Long merge(Long b, Long a) {
            return a + b;
        }
    }

    public static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow>{

        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount(s,timeWindow.getEnd(),iterable.iterator().next()));
        }
    }


    public static class TopHotPages extends KeyedProcessFunction<Long,PageViewCount, String>{
        private Integer topSize;
        public TopHotPages(Integer size) {
            this.topSize = size;
        }

        ListState<PageViewCount> pageViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            pageViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("page-count-list",PageViewCount.class));
        }

        @Override
        public void processElement(PageViewCount pageViewCount, KeyedProcessFunction<Long, PageViewCount, String>.Context context, Collector<String> collector) throws Exception {
            pageViewCountListState.add(pageViewCount);
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd()  + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, PageViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<PageViewCount> pageViewCounts = Lists.newArrayList(pageViewCountListState.get().iterator());
            pageViewCounts.sort((v1,v2)->{
                if(v1.getCount() > v2.getCount()){
                    return -1;
                }else if( v1.getCount() < v2.getCount()){
                    return 1;
                }else {
                    return 0;
                }
            });

            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===============/n");
            resultBuilder.append("window endTime").append( new Timestamp(timestamp -1)).append("\n");
            for(int i = 0; i < Math.min(topSize, pageViewCounts.size() );i ++){
                PageViewCount pageViewCount = pageViewCounts.get(i);
                resultBuilder.append("no").append(i + 1).append(i)
                        .append(":").append("id =").append(pageViewCount.getUrl())
                        .append("hot:").append(pageViewCount.getCount()).append("/n");
            }
            resultBuilder.append("===========================/n/n");
            Thread.sleep(1000L);
            out.collect(resultBuilder.toString());

        }
    }


}
