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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

/**
 * This class implements Callable, and when called iterates through it's
 * SegmentedScanResult, then pushes each item with it's size onto a blocking
 * queue.
 */
public class MapOfQueuesWorker implements Callable<Integer> {

    /**
     * Logger for the LogStashQueueWorker.
     */
    private static final Logger LOGGER = LogManager
            .getLogger(MapOfQueuesWorker.class);

    private final List<BlockingQueue<Map<String, AttributeValue>>> queue;
    private final SegmentedResult result;

    public MapOfQueuesWorker(List<BlockingQueue<Map<String, AttributeValue>>> queue,
                             SegmentedResult result) {
        this.queue = queue;
        this.result = result;
    }

    @Override
    public Integer call() {
        final ScanQueryCommon scanResult = result.getResult();
        final List<Map<String, AttributeValue>> items = scanResult.getItems();
        boolean interrupted = false;
        for (Map<String, AttributeValue> item : items) {
            boolean ok = false;
            while (!ok) {
                try {
                    queue.get(result.getSegment()).add(item);
                    ok = true;
                } catch (IllegalStateException ise) {
                    try {
                        Thread.sleep(100);
                        //LOGGER.info(String.format("Segment %s", result.getSegment()));
                    } catch (InterruptedException e) {
                        interrupted = true;
                        LOGGER.warn("interrupted when writing item to queue: "
                                + e.getMessage());
                    }

                }
            }
        }
        return items.size();
    }
}