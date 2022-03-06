package com.prey.flink.sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


import java.sql.*;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SinkToPostgresql extends RichSinkFunction<ArrayList<JSONObject>> {


    private Connection connection;
    private PreparedStatement insertStatement;
    private PreparedStatement updateStatement;
    private PreparedStatement heartbeat;

    private String url;
    private String username;
    private String password;

    public SinkToPostgresql(String url,String username,String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(url, username,password);

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                   heartbeat = connection.prepareStatement("SELECT 1");
                   heartbeat.execute();
                } catch (SQLException e) {
                    e.printStackTrace();
                }finally {
                    // 数据库连接断开可以重连
                    try {
                        connection = DriverManager.getConnection(url, username, password);
                    }catch (SQLException e){
                        e.printStackTrace();
                    }
                }
            }
        };
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(runnable, 5 ,5 , TimeUnit.MINUTES);
    }

    @Override
    public void close() throws Exception {
        if(updateStatement != null){
            updateStatement.close();
        }
        if(connection != null){
            connection.close();
        }
        if(insertStatement != null){
            insertStatement.close();
        }
    }

    @Override
    public void invoke(ArrayList<JSONObject> value) throws Exception {
        String selectSql ="select * from userdb.user where name='%s";
        String insertSql = "insert into userdb.user(name,age) values(?,?) ";
        insertStatement.execute(insertSql);
        int batchSize = 0;
        for(int i = 0; i < value.size(); i++){
            JSONObject user = value.get(i);
            String execQuerySql = String.format(selectSql, user.getString("username"));
            ResultSet rs = connection.createStatement().executeQuery(execQuerySql);

            if(rs.next()){
                String update = "update userdb.user set user='s";
                String where = "where id ='%s";
                String sql = update + where;
                updateStatement = connection.prepareStatement(sql);
                updateStatement.setString(1,"name");
                updateStatement.execute();
            }else{
                insertStatement.setString(1,"name");
                insertStatement.setInt(2,20);
                insertStatement.addBatch();
                batchSize ++;
                if(batchSize > 50){
                    insertStatement.execute();
                    batchSize = 0;
                    insertStatement.clearBatch();
                }

            }

        }
    }
}
