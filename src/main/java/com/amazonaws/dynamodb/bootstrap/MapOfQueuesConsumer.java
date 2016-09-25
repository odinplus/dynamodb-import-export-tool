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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;

/**
 */
public class MapOfQueuesConsumer extends AbstractLogConsumer {

    private static final Logger LOGGER = LogManager
            .getLogger(MapOfQueuesConsumer.class);

    private final List<BlockingQueue<Map<String, AttributeValue>>> queue;
    private int currentIndex = 0;

    public MapOfQueuesConsumer(ExecutorService exec, int numThreads, int numSegments) {
        this.queue = Collections.synchronizedList(new ArrayList<BlockingQueue<Map<String, AttributeValue>>>(numSegments));
        for (int i = 0; i < numSegments; i++) {
            this.queue.add(new ArrayBlockingQueue<Map<String, AttributeValue>>(1000));
        }
        int numProcessors = Runtime.getRuntime().availableProcessors();
        if (numProcessors > numThreads) {
            numThreads = numProcessors;
        }
        super.threadPool = exec;
        super.exec = new ExecutorCompletionService<Integer>(threadPool);

        //this.threadPool = Executors.newFixedThreadPool(numThreads);
        //this.exec = new ExecutorCompletionService<Integer>(threadPool);
    }

    @Override
    public List<Future<Integer>> writeResult(SegmentedScanResult result) {
        List<Future<Integer>> futureList = new ArrayList<>();
        try {
            exec.submit(new MapOfQueuesWorker(queue, result));
            //futureList.add(jobSubmission);
        } catch (NullPointerException npe) {
            throw new NullPointerException(
                    "Thread pool not initialized for LogStashExecutor");
        }
        return futureList;
    }

    /**
     */
    public List<BlockingQueue<Map<String, AttributeValue>>> getQueue() {
        return queue;
    }

    public int getQueueSize() {
        int c = 0;
        for (BlockingQueue<Map<String, AttributeValue>> q : queue) {
            c += q.size();
        }
        return c;
    }

    /**
     * Shuts down the threadpool then adds a termination result to the end of
     * the queue.
     */
    @Override
    public void shutdown(boolean awaitTermination) {
        super.shutdown(awaitTermination);
    }

    public synchronized List<Map<String, AttributeValue>> popNElementsFromQueue(int n) {
        currentIndex = 0;
        synchronized (queue) {
            Collections.sort(queue, new Comparator<BlockingQueue<Map<String, AttributeValue>>>() {
                @Override
                public int compare(BlockingQueue<Map<String, AttributeValue>> lhs, BlockingQueue<Map<String, AttributeValue>> rhs) {
                    int ls = 0;
                    int rs = 0;
                    try {
                        ls = lhs.size();
                    } catch (Exception e) {
                        ls = 0;
                    }
                    try {
                        rs = rhs.size();
                    } catch (Exception e) {
                        rs = 0;
                    }
                    if (ls > rs) {
                        return -1;
                    } else if (rs > ls) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });
        }
        List<Map<String, AttributeValue>> l = new ArrayList<Map<String, AttributeValue>>();
        while (n-- != 0) {
            if (!queue.get(currentIndex).isEmpty()) {
                Map<String, AttributeValue> m = queue.get(currentIndex).poll();
                l.add(m);
            } else {
                if (getQueueSize() > 0) {
                    n++;
                    try {
                        Thread.sleep(100);
                        LOGGER.debug("Sleeping");
                    } catch (Exception e) {
                        LOGGER.debug(e.getMessage());
                    }
                }
            }
            if (++currentIndex >= queue.size()) {
                currentIndex = 0;
            }
        }
        return l;
    }
}
