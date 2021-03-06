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
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class executes multiple scan requests on one segment of a table in
 * series, as a runnable. Instances meant to be used as tasks of the worker
 * thread pool for parallel scans.
 * 
 */
public class ParallelScanExecutor {
    private final BitSet finished;
    private final ScanSegmentWorker[] workers;
    private final ExecutorCompletionService<SegmentedResult> exec;
    private final Map<Integer, ScanQueryCommon> previousResultMap = new HashMap<>();
    private final Map<Integer, ScanQueryCommon> resultMap = new HashMap<>();
    private AtomicInteger count = new AtomicInteger(0);
    private AtomicInteger previousCount = new AtomicInteger(0);
    private final String tableName;

    private static final Logger LOGGER = LogManager
            .getLogger(ParallelScanExecutor.class);

    public ParallelScanExecutor(Executor executor, int segments, String name) {
        this.exec = new ExecutorCompletionService<SegmentedResult>(executor);
        this.finished = new BitSet(segments);
        this.finished.clear();
        this.workers = new ScanSegmentWorker[segments];
        this.tableName = name;
    }

    /**
     * Set the segment to finished
     */
    public void finishSegment(int segment) {
        synchronized (finished) {
            if (segment > finished.size()) {
                throw new IllegalArgumentException(
                        "Invalid segment passed to finishSegment");
            }
            finished.set(segment);
        }
    }

    /**
     * returns if the scan is finished
     */
    public boolean finished() {
        synchronized (finished) {
            return finished.cardinality() == workers.length;
        }
    }

    /**
     * This method gets a segmentedScanResult and submits the next scan request
     * for that segment, if there is one.
     * 
     * @return the next available ScanResult
     * @throws ExecutionException
     *             if one of the segment pages threw while executing
     * @throws InterruptedException
     *             if one of the segment pages was interrupted while executing.
     */
    public SegmentedResult grab() throws ExecutionException,
            InterruptedException {
        Future<SegmentedResult> ret = exec.take();

        int segment = ret.get().getSegment();
        ScanSegmentWorker sw = workers[segment];

        if (sw.hasNext()) {
            exec.submit(sw);
        } else {
            finishSegment(segment);
        }

        ScanQueryCommon result = ret.get().getResult();
        count.addAndGet(((ScanResultWrapper)result).getScannedCount());
        resultMap.put(segment, result);
        LOGGER.info(String.format("count = %s", count));
        if (count.get() > previousCount.get()+BootstrapConstants.SER_EVERY_AMOUNT) {
            previousCount.set(count.get());
            if (!previousResultMap.isEmpty()) {
                try {
                    FileOutputStream fos = new FileOutputStream(String.format(BootstrapConstants.SER_FILE_NAME_FORMAT_STRING, this.tableName));
                    ObjectOutputStream oos = new ObjectOutputStream(fos);
                    oos.writeObject(previousResultMap);
                    oos.close();
                } catch (Exception e) {
                    LOGGER.error(e.getMessage());
                }
            }
            previousResultMap.clear();
            previousResultMap.putAll(resultMap);
        }
        return ret.get();
    }

    /**
     * adds a worker to the ExecutorCompletionService
     */
    public void addWorker(ScanSegmentWorker ssw, int segment) {
        workers[segment] = ssw;
        exec.submit(ssw);
    }
}
