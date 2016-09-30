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

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.*;

/**
 */
public class MapOfQueuesConsumer extends AbstractLogConsumer {

    private static final Logger LOGGER = LogManager
            .getLogger(MapOfQueuesConsumer.class);

    private final List<BlockingQueue<Map<String, AttributeValue>>> queue;
    private final Set<Integer> finishedQueuesIndexes;

    public MapOfQueuesConsumer(ExecutorService exec, int numThreads, int numSegments) {
        this.queue = Collections.synchronizedList(new ArrayList<BlockingQueue<Map<String, AttributeValue>>>(numSegments));
        for (int i = 0; i < numSegments; i++) {
            this.queue.add(new ArrayBlockingQueue<Map<String, AttributeValue>>(1000));
        }
        int numProcessors = Runtime.getRuntime().availableProcessors();
        if (numProcessors > numThreads) {
            numThreads = numProcessors;
        }
        this.finishedQueuesIndexes = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
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
        if (result != null) {
            ScanResult sresult = result.getScanResult();
            if (sresult.getLastEvaluatedKey() == null
                    || sresult.getLastEvaluatedKey().isEmpty()) {
                finishedQueuesIndexes.add(result.getSegment());
            }
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

    private List<Map.Entry<Integer, Integer>> getQueueSizes() {
        List<Map.Entry<Integer, Integer>> l = new ArrayList<>();
        int c = 0;
        for (BlockingQueue<Map<String, AttributeValue>> q : queue) {
            Map.Entry<Integer, Integer> entry = new AbstractMap.SimpleEntry<Integer, Integer>(c, q.size());
            l.add(entry);
            c++;
        }
        return l;
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
        int currentIndex = 0;

        List<Map.Entry<Integer, Integer>> sizes = getQueueSizes();
        Collections.sort(sizes, new Comparator<Map.Entry<Integer, Integer>>() {
            @Override
            public int compare(Map.Entry<Integer, Integer> lhs, Map.Entry<Integer, Integer> rhs) {
                int ls = 0;
                int rs = 0;
                try {
                    ls = lhs.getValue();
                } catch (Exception e) {
                    ls = 0;
                }
                try {
                    rs = rhs.getValue();
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
        //LOGGER.info(Arrays.toString(sizes.toArray()));
        List<Map<String, AttributeValue>> l = new ArrayList<Map<String, AttributeValue>>();
        while (n-- != 0) {
            int index = sizes.get(currentIndex).getKey();
            if (!queue.get(index).isEmpty()) {
                Map<String, AttributeValue> m = queue.get(index).poll();
                if (m != null) { // null if empty
                    l.add(m);
                }
            } else {
                if (getQueueSize() > 0 && !this.finishedQueuesIndexes.contains(index)) {
                    n++;
                    try {
                        Thread.sleep(100);
                        LOGGER.debug("Sleeping");
                    } catch (Exception e) {
                        LOGGER.debug(e.getMessage());
                    }
                }
            }
            if (++currentIndex >= sizes.size()) {
                currentIndex = 0;
            }
        }
        return l;
    }
}
