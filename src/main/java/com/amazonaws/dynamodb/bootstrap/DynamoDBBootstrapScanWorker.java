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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.dynamodb.bootstrap.exception.NullReadCapacityException;
import com.amazonaws.dynamodb.bootstrap.exception.SectionOutOfRangeException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.internal.InternalUtils;
import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * The base class to start a parallel scan and connect the results with a
 * consumer to accept the results.
 */
public class DynamoDBBootstrapScanWorker extends AbstractLogProvider {
    private final AmazonDynamoDBClient client;
    private final double rateLimit;
    private final String tableName;
    private final int numSegments;
    private int section;
    private int totalSections;
    private final boolean consistentScan;
    private final String projectionExpression;
    private final String filterExpression;
    private final String expressionAttributeValues;

    private static final Logger LOGGER = LogManager
            .getLogger(DynamoDBBootstrapScanWorker.class);


    /**
     * Creates the DynamoDBBootstrapScanWorker, calculates the number of segments a
     * table should have, and creates a thread pool to prepare to scan.
     * 
     * @throws Exception
     */
    public DynamoDBBootstrapScanWorker(AmazonDynamoDBClient client,
                                       double rateLimit, String tableName, ExecutorService exec,
                                       int section, int totalSections, int numSegments,
                                       boolean consistentScan, String projectionExpression, String filterExpression, String expressionAttributeValues) throws SectionOutOfRangeException {
        if (section > totalSections - 1 || section < 0) {
            throw new SectionOutOfRangeException(
                    "Section of scan must be within [0...totalSections-1]");
        }

        this.client = client;
        this.rateLimit = rateLimit;
        this.tableName = tableName;

        this.numSegments = numSegments;
        this.section = section;
        this.totalSections = totalSections;
        this.consistentScan = consistentScan;
        this.projectionExpression = projectionExpression;
        this.filterExpression = filterExpression;
        this.expressionAttributeValues = expressionAttributeValues;

        super.threadPool = exec;
    }

    /**
     * Creates the DynamoDBBootstrapScanWorker, calculates the number of segments a
     * table should have, and creates a thread pool to prepare to scan using an
     * eventually consistent scan.
     * 
     * @throws Exception
     */
    public DynamoDBBootstrapScanWorker(AmazonDynamoDBClient client,
                                       double rateLimit, String tableName, int numThreads, String projectionExpression, String filterExpression, String expressionAttributeValues)
            throws NullReadCapacityException {
        this.client = client;
        this.rateLimit = rateLimit;
        this.tableName = tableName;
        TableDescription description = client.describeTable(tableName)
                .getTable();
        this.section = 0;
        this.totalSections = 1;
        this.consistentScan = false;

        this.numSegments = getNumberOfSegments(description);
        int numProcessors = Runtime.getRuntime().availableProcessors() * 4;
        if (numProcessors > numThreads) {
            numThreads = numProcessors;
        }
        this.projectionExpression = projectionExpression;
        this.filterExpression = filterExpression;
        this.expressionAttributeValues = expressionAttributeValues;
        super.threadPool = Executors.newFixedThreadPool(numThreads);
    }

    /**
     * Begins to pipe the log results by parallel scanning the table and the
     * consumer writing the results.
     */
    public void pipe(final AbstractLogConsumer consumer)
            throws ExecutionException, InterruptedException {
        LOGGER.info(String.format("numSegments %s", numSegments));

        final DynamoDBTableScan scanner = new DynamoDBTableScan(rateLimit,
                client);

        final QueryRequest queryRequest = new QueryRequest().withTableName(tableName)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withLimit(BootstrapConstants.SCAN_LIMIT)
                .withConsistentRead(consistentScan);

        final ScanRequest request = new ScanRequest().withTableName(tableName)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withLimit(BootstrapConstants.SCAN_LIMIT)
                .withConsistentRead(consistentScan);
        if (projectionExpression != null && !projectionExpression.isEmpty()) {
               request.setProjectionExpression(projectionExpression);
        }
        if (filterExpression != null && !filterExpression.isEmpty()) {
            LOGGER.info(String.format("filterExpression %s", filterExpression));
            request.setFilterExpression(filterExpression);

            Item i = new Item();
            String json = expressionAttributeValues;
            i = i.fromJSON(json);
            Map<String,AttributeValue> attributesValues = InternalUtils.toAttributeValues(i);

            request.setExpressionAttributeValues(attributesValues);
            LOGGER.info(String.format("%s", request));
        }

        final ParallelScanExecutor scanService = scanner
                .getParallelScanCompletionService(request, numSegments,
                        threadPool, section, totalSections);

        List<Future<Integer>> futureList = new ArrayList<>();
        while (!scanService.finished()) {
            SegmentedResult result = scanService.grab();
            //LOGGER.info(result.getSegment());
            //LOGGER.info(result.getScanResult().getScannedCount());
            /*List<Future<Integer>> anotherFutureList =*/ consumer.writeResult(result);
            //futureList.addAll(anotherFutureList);
            //CompletionService<Integer> ecs = new ExecutorCompletionService<Integer>(e);
            //LOGGER.info(f.get());
        }

        shutdown(true);
        consumer.shutdown(true);

        /*int c =0;
        for (Future<Integer> future : futureList){
            if (future.isDone()){
                c+=future.get();
            } else {
                LOGGER.info(future.isCancelled());
            }
        }
        LOGGER.info(String.format("c = %s", c));*/
    }

    /**
     * returns the approximate number of segments a table should be broken up
     * when parallel scanning. This function is based off of either read and
     * write capacity, with which you can scan much faster, or the size of your
     * table, which should need many more segments in order to scan the table
     * fast enough in parallel so that one worker does not finish long before
     * other workers.
     * 
     * @throws NullReadCapacityException
     *             if the table returns a null readCapacity units.
     */
    public static int getNumberOfSegments(TableDescription description)
            throws NullReadCapacityException {
        ProvisionedThroughputDescription provisionedThroughput = description
                .getProvisionedThroughput();
        double tableSizeInGigabytes = Math.ceil(description.getTableSizeBytes()
                / BootstrapConstants.GIGABYTE);
        Long readCapacity = provisionedThroughput.getReadCapacityUnits();
        Long writeCapacity = provisionedThroughput.getWriteCapacityUnits();
        if (writeCapacity == null) {
            writeCapacity = 1L;
        }
        if (readCapacity == null) {
            throw new NullReadCapacityException(
                    "Cannot scan with a null readCapacity provisioned throughput");
        }
        double throughput = (readCapacity + 3 * writeCapacity) / 3000.0;
        return (int) (10 * Math.max(Math.ceil(throughput),
                Math.ceil(tableSizeInGigabytes) / 10));
    }

}
