package com.amazonaws.dynamodb.bootstrap;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class MapToDynamoWorker extends AbstractLogProvider {

    private static final Logger LOGGER = LogManager
            .getLogger(MapToDynamoWorker.class);

    private MapOfQueuesConsumer sourceConsumer;
    private DynamoDBBootstrapScanWorker fromDBWorker;

    public MapToDynamoWorker(DynamoDBBootstrapScanWorker worker, MapOfQueuesConsumer consumer) {
        this.sourceConsumer = consumer;
        this.fromDBWorker = worker;
    }

    @Override
    public void pipe(AbstractLogConsumer consumer) throws ExecutionException, InterruptedException {
        int count = 0;
        int c = -1;
        while (!fromDBWorker.threadPool.isTerminated() || sourceConsumer.getQueueSize() !=0){
            List<Map<String, AttributeValue>> l =sourceConsumer.popNElementsFromQueue(25);
            c = l.size();
            count+=c;
            ScanResultWrapper result = new ScanResultWrapper(new ScanResult());
            result.withItems(l);
            SegmentedResult sresult = new SegmentedResult(result, 0);
            consumer.writeResult(sresult);
        }
        //shutdown(true);
        consumer.shutdown(true);

        LOGGER.info(String.format("c = %s", count));

    }
}
