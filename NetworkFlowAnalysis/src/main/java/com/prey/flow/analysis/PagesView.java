package com.prey.flow.analysis;

import com.prey.flow.bean.LogEvent;
import com.prey.flow.bean.PageViewCount;
import com.prey.flow.bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.Random;

public class PagesView {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        URL resource = PagesView.class.getResource("/data.log");
        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath());
        SingleOutputStreamOperator<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });


        SingleOutputStreamOperator<UserBehavior> filterStream = dataStream.filter(data -> "pv".equals(data.getBehavior()));
        /* 下列代码数据量大存在数据倾斜情况
        //filterStream.keyBy(0).timeWindow(Time.hours(1)).sum(1);
         并行任务改进，设计随机key进行分区，解决数据倾斜
         */
        SingleOutputStreamOperator<PageViewCount> aggregate = filterStream.keyBy(UserBehavior::getItemId)
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior userBehavior) throws Exception {
                        Random random = new Random();
                        return new Tuple2<>(random.nextInt(10), 1L);
                    }
                }).keyBy(data -> data.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new PageViewAgg(), new PageViewResult());




    }

    public static class PageViewAgg implements AggregateFunction<Tuple2<Integer, Long>,Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> t, Long a) {
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

    public static class PageViewResult implements WindowFunction<Long, PageViewCount, Integer, TimeWindow> {


        @Override
        public void apply(Integer integer, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount(integer.toString(),timeWindow.getEnd(),iterable.iterator().next()));
        }
    }
}
