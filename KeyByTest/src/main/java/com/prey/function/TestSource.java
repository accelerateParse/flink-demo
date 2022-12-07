package com.prey.function;

import com.prey.bean.Person;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @Author YaoJiangXiao
 * @Date 2022-11-16 10:32
 * @description:
 **/
public class TestSource implements SourceFunction<Person> {

    Boolean running = true;


    List<String> name = Arrays.asList("zhangsan","lisi","wangwu");
    Random random = new Random();

    @Override
    public void run(SourceContext<Person> sourceContext) throws Exception {
        while (running) {
            int age =(int) (System.currentTimeMillis() / 10000) % 20;
            sourceContext.collect(new Person(age, name.get(random.nextInt(3))));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }
}
