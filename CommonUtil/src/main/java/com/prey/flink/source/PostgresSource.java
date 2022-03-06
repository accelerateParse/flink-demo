package com.prey.flink.source;

import com.alibaba.fastjson.JSONObject;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Properties;

/**
 *
 */
public class PostgresSource extends RichSourceFunction<HashMap<String,JSONObject>> {

    public PostgresSource(Properties properties) {
        this.url = properties.getProperty("url");
        this.username = properties.getProperty("username");
        this.password = properties.getProperty("password");
        this.time = new Long(properties.getProperty("time"));
    }

    private Boolean isRunning = true;
    private String url;
    private String username;
    private String password;
    private HikariDataSource dataSource;
    private Long time;

    @Override
    public void run(SourceContext<HashMap<String,JSONObject>> sourceContext) throws Exception {
        while(isRunning){
            HashMap<String , JSONObject> users = getUserBroadCast();
            if( users!= null ){
                sourceContext.collect(users);
            }
            Thread.sleep(time);
        }
    }

    private  HashMap<String , JSONObject> getUserBroadCast() throws SQLException {
        HashMap<String , JSONObject> result = new HashMap<>();
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);
        ResultSet res = statement.executeQuery("select * from userdb.user");
        while (res.next()){
            JSONObject user = new JSONObject();
            user.put("name",res.getString(1));
            user.put("age",res.getInt(2));
            result.put(res.getString(3),user);
        }
        return result;
    }

    @Override
    public void cancel() {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.dataSource = new HikariDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setJdbcUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setPoolName("pg ");
    }

    @Override
    public void close() throws Exception {
        dataSource.close();
    }
}
