package com.prey.order;

import com.prey.order.bean.OrderEvent;

import com.prey.order.bean.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.util.List;
import java.util.Map;

public class OrderPayTimeoutCep {
    public static void main(String[] args) throws Exception {
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
        });
        // 1 定义一个带时间限制的模式
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return "create".equals(orderEvent.getEventType());
                    }
                }).followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                           @Override
                           public boolean filter(OrderEvent orderEvent) throws Exception {
                               return "pay".equals(orderEvent.getEventType());
                           }
                       })
                .within(Time.minutes(15));
        //2 定义侧输出流标签表示超时时间
        OutputTag<OrderResult> orderTimeoutTag = new OutputTag<>("order-timeout");
        //3 将patter应用到数据流上，得到patter stream
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream.keyBy(OrderEvent::getOrderId), pattern);
        //4 调用select方法实现匹配辅助时间和超时复杂时间的提取和处理
        SingleOutputStreamOperator<OrderResult> resultStream = patternStream.select(orderTimeoutTag, new OrderTimeoutSelect(), new OrderPaySelect());
        resultStream.print("normal");
        resultStream.getSideOutput(orderTimeoutTag).print("timeout");

        env.execute("order pay timeout");
    }

    public static class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent, OrderResult> {

        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
            Long timeoutOrderId = map.get("create").iterator().next().getOrderId();
            return new OrderResult(timeoutOrderId,"timeout" + l );
        }
    }
    public static class OrderPaySelect implements PatternSelectFunction<OrderEvent, OrderResult>{

        @Override
        public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
            Long payedOrderId = map.get("create").iterator().next().getOrderId();
            return new OrderResult(payedOrderId,"payed" );
        }
    }
}
