package com.prey.flink.mysql;


import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;


/**
 * @author ： YaoJiangXiao
 * @data 2022/06/15/17:04
 * @description:
 */
public class DirectionBroadcast extends RichSourceFunction<HashMap<String, Integer>> {


    private final boolean  isRunning = true;

    private static Logger log = LoggerFactory.getLogger(DirectionBroadcast.class);

    private final static String REFRESH_TIME_STR = "broadcast.refreshTime";

    @Override
    public void run(SourceContext<HashMap<String, Integer>> sourceContext) throws Exception {
        while (isRunning) {
            HashMap<String, Integer> data = getRoadBroadCast();
            if (data != null) {
                sourceContext.collect(data);
            }
            Thread.sleep(1000 * 30 * 60);
        }
    }


    private static final String SELECT_SECTION
            = "select id,name,score * from student where is_delete=0 ";

    private HashMap<String, Integer> getRoadBroadCast() throws SQLException, InterruptedException {

        HashMap<String, Integer> data = new HashMap();
        DBConnection connection = null;

        try {
            connection = BroadcastUtils.connect();
            ResultSet res = connection.getStatement().executeQuery(SELECT_SECTION);
            while (res.next()) {
                Long id = res.getLong(1);
                String name = res.getString(2);
                int score = res.getInt(3);
                data.put(name , score);
            }
            return data;
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
        return null;
    }


    @Override
    public void cancel() {

    }


}
