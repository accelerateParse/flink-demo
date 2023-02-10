package com.prey.flink.mysql;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author ： YaoJiangXiao
 * @data 2022/08/30/10:36
 * @description:
 */
public class DBConnection {
    private  Connection connection;
    private HikariDataSource dataSource;

    private Statement statement;
    private ResultSet resultSet;

    public ResultSet getResultSet() {
        return resultSet;
    }

    public void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public DBConnection() {
    }

    public DBConnection(Connection connection, HikariDataSource dataSource, Statement statement) {
        this.connection = connection;
        this.dataSource = dataSource;
        this.statement = statement;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public HikariDataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Statement getStatement() {
        return statement;
    }

    public void setStatement(Statement statement) {
        this.statement = statement;
    }

    public void close() throws SQLException {
        if(this.getResultSet() != null){
            this.getResultSet().close();
        }
        if( this.dataSource != null){
            this.dataSource.close();
        }
        if( this.connection !=null){
            this.connection.close();
        }

    }
}
