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
 * This class implements ILogConsumer, and when called to writeResult, it will
 * submit a new job to it's ExecutorCompletionService with a new
 * LogStashQueueWorker. It will then shutdown by adding a 'poison pill' to the
 * end of the blocking queue to notify that it has reached the end of the scan.
 */
public class MapOfQueuesConsumer extends AbstractLogConsumer {

    private static final Logger LOGGER = LogManager
            .getLogger(MapOfQueuesConsumer.class);

    private List<BlockingQueue<Map<String, AttributeValue>>> queue;
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
        Future<Integer> jobSubmission = null;
        List<Future<Integer>> futureList = new ArrayList<>();
        try {
            jobSubmission = exec.submit(new MapOfQueuesWorker(queue, result));
            //futureList.add(jobSubmission);
        } catch (NullPointerException npe) {
            throw new NullPointerException(
                    "Thread pool not initialized for LogStashExecutor");
        }
        return futureList;
    }

    /**
     * Returns the blocking queue to which the LogStashQueueWorkers add results
     */
    public List<BlockingQueue<Map<String, AttributeValue>>> getQueue() {
        return queue;
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
        int c =0;
        int s =0;
        int sc=0;
        ArrayList<Integer> si = new ArrayList<>();
        for (BlockingQueue<Map<String, AttributeValue>> q : queue){
            c+=q.size();
        }
        int initialCurrentIndex = currentIndex;
        List<Map<String, AttributeValue>> l = new ArrayList<Map<String, AttributeValue>>();
        while (n-- != 0) {
            if (!queue.get(currentIndex).isEmpty()) {
                si.add(currentIndex);
                Map<String, AttributeValue> m = queue.get(currentIndex).poll();
                l.add(m);
                sc++;
            } else {
                n++;
                s++;
            }
            if (++currentIndex >= queue.size()) {
                currentIndex = 0;
            }
            if (currentIndex == initialCurrentIndex) {
                break;
            }
        }
        LOGGER.info(String.format("Values %s segments %s skipped %s indexes %s", c, sc, s, Arrays.toString(si.toArray())));
        return l;
    }

}
