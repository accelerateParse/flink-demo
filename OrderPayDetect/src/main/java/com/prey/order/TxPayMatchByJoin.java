package com.prey.order;

import com.prey.order.bean.OrderEvent;
import com.prey.order.bean.ReceiptEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;

public class TxPayMatchByJoin {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        URL orderResource = TxPayMatchByJoin.class.getResource("/order.txt");
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env.readTextFile(orderResource.getPath()).map(line -> {
            String[] str = line.split(",");
            return new OrderEvent(new Long(str[0]), str[1], str[2], new Long(str[3]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
            @Override
            public long extractAscendingTimestamp(OrderEvent orderEvent) {
                return orderEvent.getTimestamp() * 1000L;
            }
        }).filter( data -> !"".equals(data.getTxId()));

        URL receiptResource = TxPayMatchByJoin.class.getResource("/receipt.txt");
        SingleOutputStreamOperator<ReceiptEvent> receiptEventStream = env.readTextFile(orderResource.getPath()).map(line -> {
            String[] str = line.split(",");
            return new ReceiptEvent(str[0], str[1],new Long(str[2]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
            @Override
            public long extractAscendingTimestamp(ReceiptEvent event) {
                return event.getTimestamp() * 1000L;
            }
        });

        orderEventStream.keyBy(OrderEvent::getTxId)
                .intervalJoin(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .between(Time.seconds(-3),Time.seconds(5))
                .process(new TxPayMatchDetectByJoin())
                .print();
        env.execute();

    }

    public static class TxPayMatchDetectByJoin extends ProcessJoinFunction<OrderEvent,ReceiptEvent, Tuple2<OrderEvent,ReceiptEvent>>{

        @Override
        public void processElement(OrderEvent orderEvent, ReceiptEvent receiptEvent, ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>.Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            collector.collect(new Tuple2<>(orderEvent,receiptEvent));
        }
    }
}
