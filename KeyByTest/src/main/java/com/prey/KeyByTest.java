package com.prey;

import com.prey.bean.Person;
import com.prey.function.PersonMapFunction;
import com.prey.function.TestSource;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author YaoJiangXiao
 * @Date 2022-11-16 10:31
 * @description:
 **/
public class KeyByTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Person> dataStream = env.addSource(new TestSource());
        DataStreamSink<String> print = dataStream.keyBy(Person::getAge)
                .process(new PersonMapFunction()).setParallelism(3).print();
        env.execute();

    }
}
