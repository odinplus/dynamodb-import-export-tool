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

import java.util.*;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Takes in SegmentedScanResults and launches several DynamoDBConsumerWorker for
 * each batch of items to write to a DynamoDB table.
 */
public class DynamoDBConsumer extends AbstractLogConsumer {

    private final AmazonDynamoDBClient client;
    private final String tableName;
    private final RateLimiter rateLimiter;
    private static final Logger LOGGER = LogManager
            .getLogger(DynamoDBConsumer.class);

    /**
     * Class to consume logs and write them to a DynamoDB table.
     */
    public DynamoDBConsumer(AmazonDynamoDBClient client, String tableName,
            double rateLimit, ExecutorService exec) {
        LOGGER.info(String.format("rateLimit %s", rateLimit));
        this.client = client;
        this.tableName = tableName;
        this.rateLimiter = RateLimiter.create(rateLimit);
        super.threadPool = exec;
        super.exec = new ExecutorCompletionService<Integer>(threadPool);
    }

    /**
     * calls splitResultIntoBatches to turn the SegmentedScanResult into several
     * BatchWriteItemRequests and then submits them as individual jobs to the
     * ExecutorService.
     */
    @Override
    public List<Future<Integer>> writeResult(SegmentedResult result) {
        Future<Integer> jobSubmission = null;
        List<Future<Integer>> futureList = new ArrayList<>();
        List<BatchWriteItemRequest> batches = splitResultIntoBatches(
                result.getResult(), tableName);
        Iterator<BatchWriteItemRequest> batchesIterator = batches.iterator();
        while (batchesIterator.hasNext()) {
            try {
                jobSubmission = exec
                        .submit(new DynamoDBConsumerWorker(batchesIterator
                                .next(), client, rateLimiter, tableName));
                //futureList.add(jobSubmission);
            } catch (NullPointerException npe) {
                throw new NullPointerException(
                        "Thread pool not initialized for LogStashExecutor");
            }
        }
        return futureList;
    }

    /**
     * Splits up a ScanResult into a list of BatchWriteItemRequests of size 25
     * items or less each.
     */
    public static List<BatchWriteItemRequest> splitResultIntoBatches(
            ScanQueryCommon result, String tableName) {
        List<BatchWriteItemRequest> batches = new LinkedList<BatchWriteItemRequest>();
        Iterator<Map<String, AttributeValue>> it = result.getItems().iterator();

        BatchWriteItemRequest req = new BatchWriteItemRequest()
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        List<WriteRequest> writeRequests = new LinkedList<WriteRequest>();
        int i = 0;
        while (it.hasNext()) {
            PutRequest put = new PutRequest(it.next());
            writeRequests.add(new WriteRequest(put));

            i++;
            if (i == BootstrapConstants.MAX_BATCH_SIZE_WRITE_ITEM) {
                req.addRequestItemsEntry(tableName, writeRequests);
                batches.add(req);
                req = new BatchWriteItemRequest()
                        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
                writeRequests = new LinkedList<WriteRequest>();
                i = 0;
            }
        }
        if (i > 0) {
            req.addRequestItemsEntry(tableName, writeRequests);
            batches.add(req);
        }
        return batches;
    }
}