package com.prey.order;

import com.prey.order.bean.OrderEvent;
import com.prey.order.bean.OrderResult;
import com.prey.order.bean.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Tuple2;

import java.net.URL;

public class TxPayMatch {

    private final static OutputTag<OrderEvent> unmatchedPays = new OutputTag<OrderEvent>("unmatchedPays"){};
    private final static OutputTag<ReceiptEvent> unmatchedReceipts = new OutputTag<ReceiptEvent>("unmatchedReceipts"){};


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

        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderEventStream.keyBy(OrderEvent::getTxId).connect(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .process(new TxPayMatchDetect());

        resultStream.print("match-pays");
        resultStream.getSideOutput(unmatchedPays).print("unmatchedPays");
        resultStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts");
        env.execute("tx pay match");

    }

    public static class TxPayMatchDetect extends CoProcessFunction<OrderEvent,ReceiptEvent, Tuple2<OrderEvent,ReceiptEvent>>{
        ValueState<OrderEvent> payState;
        ValueState<ReceiptEvent> receiptState;


        @Override
        public void processElement1(OrderEvent orderEvent, CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>.Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            ReceiptEvent value = receiptState.value();
            if(value!= null){
                collector.collect(new Tuple2<>(orderEvent,value));
                payState.clear();
                receiptState.clear();
            }else {
                context.timerService().registerEventTimeTimer((value.getTimestamp() + 5) *1000);
                payState.update(orderEvent);
            }
        }

        @Override
        public void processElement2(ReceiptEvent receiptEvent, CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>.Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            OrderEvent value = payState.value();
            if(value!= null){
                collector.collect(new Tuple2<>(value,receiptEvent));
                payState.clear();
                receiptState.clear();
            }else {
                context.timerService().registerEventTimeTimer((value.getTimestamp() + 5) *1000);
                receiptState.update(receiptEvent);
            }
        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>.OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            if(payState.value() != null){
                ctx.output(unmatchedPays,payState.value());
            }
            if(receiptState.value() != null){
                ctx.output(unmatchedReceipts,receiptState.value());
            }
            payState.clear();
            receiptState.clear();
        }
    }
}
