package com.prey.function;

import com.prey.bean.Person;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author YaoJiangXiao
 * @Date 2022-11-16 11:11
 * @description:
 **/
public class PersonMapFunction  extends KeyedProcessFunction<Integer, Person, String> {

    MapState<Integer , Integer> ageCount;

    @Override
    public void open(Configuration parameters) throws Exception {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(15))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        MapStateDescriptor<Integer, Integer> stateDescriptor = new MapStateDescriptor<>("ageCount", Integer.class,Integer.class);
        stateDescriptor.enableTimeToLive(ttlConfig);
        ageCount = this.getRuntimeContext().getMapState(stateDescriptor);
    }


    @Override
    public void processElement(Person person, KeyedProcessFunction<Integer, Person, String>.Context context, Collector<String> collector) throws Exception {
        boolean contains = ageCount.contains(person.getAge());
        int count = 1;
        if(contains){
            count = ageCount.get(person.getAge()) + 1;
        }
        ageCount.put(person.getAge(), count);
        collector.collect( person.getAge() + "," + count);
    }
}
