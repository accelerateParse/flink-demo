package com.prey.hot;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class HotItemAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.execute("");

    }
}
