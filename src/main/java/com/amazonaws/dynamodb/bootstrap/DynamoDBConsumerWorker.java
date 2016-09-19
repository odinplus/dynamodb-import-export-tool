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

import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Callable;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Callable class that is used to write a batch of items to DynamoDB with exponential backoff.
 */
public class DynamoDBConsumerWorker implements Callable<Integer> {
    private final AmazonDynamoDBClient client;
    private final RateLimiter rateLimiter;
    private long exponentialBackoffTime;
    private BatchWriteItemRequest batch;
    private final String tableName;
    private int total_processed=0;

    private static final Logger LOGGER = LogManager
            .getLogger(DynamoDBConsumerWorker.class);

    /**
     * Callable class that when called will try to write a batch to a DynamoDB
     * table. If the write returns unprocessed items it will exponentially back
     * off until it succeeds.
     */
    public DynamoDBConsumerWorker(BatchWriteItemRequest batchWriteItemRequest,
            AmazonDynamoDBClient client, RateLimiter rateLimiter,
            String tableName) {
        this.batch = batchWriteItemRequest;
        this.client = client;
        this.rateLimiter = rateLimiter;
        this.tableName = tableName;
        this.exponentialBackoffTime = BootstrapConstants.INITIAL_RETRY_TIME_MILLISECONDS;
    }

    /**
     * Batch writes the write request to the DynamoDB endpoint and THEN acquires
     * permits equal to the consumed capacity of the write.
     */
    @Override
    public Integer call() {
        List<ConsumedCapacity> batchResult = runWithBackoff(batch);
        Iterator<ConsumedCapacity> it = batchResult.iterator();
        int consumedCapacity = 0;
        while (it.hasNext()) {
            consumedCapacity += it.next().getCapacityUnits().intValue();
        }
        rateLimiter.acquire(consumedCapacity);
        //LOGGER.info(consumedCapacity);
        return total_processed;
    }

    /**
     * Writes to DynamoDBTable using an exponential backoff. If the
     * batchWriteItem returns unprocessed items then it will exponentially
     * backoff and retry the unprocessed items.
     */
    public List<ConsumedCapacity> runWithBackoff(BatchWriteItemRequest req) {
        BatchWriteItemResult writeItemResult = null;
        List<ConsumedCapacity> consumedCapacities = new LinkedList<ConsumedCapacity>();
        Map<String, List<WriteRequest>> unprocessedItems = null;
        boolean interrupted = false;
        total_processed = 0;
        try {
            do {
                writeItemResult = null;
                while (writeItemResult == null) {
                    try {
                        writeItemResult = client.batchWriteItem(req);
                    } catch (ProvisionedThroughputExceededException e) {
                        //LOGGER.info(String.format("%s %s", e.getMessage(), req));
                        try {
                            LOGGER.info(String.format("%s sleeping %s", Thread.currentThread().getId(), exponentialBackoffTime));
                            Thread.sleep(exponentialBackoffTime);
                        } catch (InterruptedException ie) {
                            interrupted = true;
                        } finally {
                            exponentialBackoffTime *= 2;
                            if (exponentialBackoffTime > BootstrapConstants.MAX_EXPONENTIAL_BACKOFF_TIME) {
                                exponentialBackoffTime = BootstrapConstants.MAX_EXPONENTIAL_BACKOFF_TIME;
                            }
                        }
                    }
                }
                    unprocessedItems = writeItemResult.getUnprocessedItems();
                int total_unprocessed = 0;
                for (List<WriteRequest> list : req.getRequestItems().values()) {
                    total_processed += list.size();
                }

                consumedCapacities
                        .addAll(writeItemResult.getConsumedCapacity());

                if (unprocessedItems != null) {
                    for (List<WriteRequest> list : unprocessedItems.values()) {
                        total_unprocessed += list.size();
                    }
                    total_processed -= total_unprocessed;
                    req.setRequestItems(unprocessedItems);
                    try {
                        Thread.sleep(exponentialBackoffTime);
                    } catch (InterruptedException ie) {
                        interrupted = true;
                    } finally {
                        exponentialBackoffTime *= 2;
                        if (exponentialBackoffTime > BootstrapConstants.MAX_EXPONENTIAL_BACKOFF_TIME) {
                            exponentialBackoffTime = BootstrapConstants.MAX_EXPONENTIAL_BACKOFF_TIME;
                        }
                    }
                }
            } while (unprocessedItems != null && unprocessedItems.get(tableName) != null);
            return consumedCapacities;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
