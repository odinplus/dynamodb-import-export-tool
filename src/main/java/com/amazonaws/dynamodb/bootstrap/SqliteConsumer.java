/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.dynamodb.bootstrap;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class SqliteConsumer extends AbstractLogConsumer {

    private final Connection connection;
    private final PreparedStatement preparedStatement;
    private final String tableName;
    private static final Logger LOGGER = LogManager
            .getLogger(SqliteConsumer.class);

    /**
     */
    public SqliteConsumer(String dbFileName, String tableName) throws SQLException {
        this.tableName = tableName;
        this.connection = DriverManager.getConnection(String.format("jdbc:sqlite:%s", dbFileName));
        //preparedStatement = connection.prepareStatement(String.format("INSERT INTO %s VALUES (?, ?, ?)", tableName));
        Statement statement = connection.createStatement();
        statement.execute(String.format("CREATE TABLE IF NOT EXISTS %s (uid STRING, ver INTEGER)", tableName));

        preparedStatement = connection.prepareStatement(String.format("INSERT INTO %s VALUES (?, ?)", tableName));
        //super.threadPool = exec;
        //super.exec = new ExecutorCompletionService<Integer>(threadPool);
    }

    /**
     */
    @Override
    public List<Future<Integer>> writeResult(SegmentedScanResult result) {
        List<Future<Integer>> futureList = new ArrayList<>();
        List<Map<String, AttributeValue>> items = result.getScanResult().getItems();
        LOGGER.info(String.format("%s %s", result.getSegment(), items.size()));
        boolean ok = false;
        while (!ok) {
            try {
                preparedStatement.clearParameters();
                for (Map<String, AttributeValue> item : items) {
                    preparedStatement.setString(1, item.get("uid").getS());
                    //preparedStatement.setInt(2, Integer.parseInt(item.get("ver").getN()));
                    preparedStatement.setString(2, item.get("cc").getS());
                    //preparedStatement.setInt(3, Integer.parseInt(item.get("vc").getN()));
                    preparedStatement.addBatch();

                }
                connection.setAutoCommit(false);
                /*int[] counts =*/ preparedStatement.executeBatch();
                connection.setAutoCommit(true);
                ok = true;
                //LOGGER.info(Arrays.toString(counts));

            } catch (SQLException e) {
                LOGGER.error(e.getMessage(), e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    LOGGER.error(ie.getMessage(), ie);
                }
            }
        }

        return futureList;
    }

    public void shutdown(boolean awaitTermination) {
        try {
            this.connection.commit();
        } catch (SQLException e){
            LOGGER.error(e.getMessage(), e);
        }
        try {
            this.connection.close();
        } catch (SQLException e){
            LOGGER.error(e.getMessage(), e);
        }

    }
}