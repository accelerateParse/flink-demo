package com.prey.flink.mysql;


import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author ： YaoJiangXiao
 * @data 2022/06/15/17:04
 * @description:
 */
public class BroadcastUtils {


    private boolean isRunning = true;

    private static Logger log = LoggerFactory.getLogger(BroadcastUtils.class);


    public static HikariDataSource getDataSource() throws SQLException {
        HikariDataSource hikariDataSource = new HikariDataSource();
        hikariDataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        hikariDataSource.setJdbcUrl("jdbc:mysql://localhost:3306/db1");
        hikariDataSource.setUsername("username");
        hikariDataSource.setPassword("password");
        hikariDataSource.setPoolName("mysql-broadcast ");
        hikariDataSource.setConnectionTimeout(3000L);
        hikariDataSource.setLoginTimeout(3);
        return hikariDataSource;
    }

    public static DBConnection connect() throws SQLException, InterruptedException {
        HikariDataSource dataSource = null;
        Connection connection = null;
        try {
            dataSource = getDataSource();
            connection = dataSource.getConnection();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (connection == null) {
                Thread.sleep(20000);
                dataSource = getDataSource();
                connection = dataSource.getConnection();
            }
        }
        Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        return new DBConnection(connection, dataSource, statement);
    }


}
