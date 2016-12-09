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

import java.util.List;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;

/**
 * The interface to use with the DynamoDBBootstrapScanWorker.java class to consume
 * logs and write the results somewhere.
 */
public abstract class AbstractLogConsumer {

    public ExecutorCompletionService<Integer> exec;
    protected ExecutorService threadPool;

    /**
     * Logger for the DynamoDBBootstrapScanWorker.
     */
    private static final Logger LOGGER = LogManager
            .getLogger(AbstractLogConsumer.class);

    /**
     * Writes the result of a scan to another endpoint asynchronously. Will call
     * getWorker to determine what job to submit with the result.
     *
     */
    public abstract List<Future<Integer>> writeResult(SegmentedResult result);

    /**
     * Shuts the thread pool down.
     *
     */
    public void shutdown(boolean awaitTermination) {
        if (awaitTermination) {
            boolean interrupted = false;
            threadPool.shutdown();
            try {
                while (!threadPool.awaitTermination(BootstrapConstants.WAITING_PERIOD_FOR_THREAD_TERMINATION_SECONDS, TimeUnit.SECONDS)) {
                    LOGGER.warn("Waiting for the threadpool to terminate...");
                }
            } catch (InterruptedException e) {
                interrupted = true;
                LOGGER.warn("Threadpool was interrupted when trying to shutdown: "
                        + e.getMessage());
            } finally {
                if (interrupted)
                    Thread.currentThread().interrupt();
            }
        } else {
            threadPool.shutdownNow();
        }
    }
}