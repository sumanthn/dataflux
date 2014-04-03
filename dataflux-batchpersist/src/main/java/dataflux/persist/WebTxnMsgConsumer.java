package dataflux.persist;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Message consumer for WebTransactions
 * Created by sumanthn
 */
public class WebTxnMsgConsumer extends KafkaConsumer implements Runnable {

    private static final Logger logger = Logger.getLogger(WebTxnMsgConsumer.class);
    // 5 seconds, the data is not for realtime processing
    //Bulk persist data is more relevant & advantageous
    int maxBatchSize = 1000;
    int maxWaitTime = 5 * 1000;
    AtomicLong lastTimeUpdated = new AtomicLong(System.currentTimeMillis());
    private ConsumerIterator streamHandle = null;
    private ScheduledExecutorService dispatcherThr = Executors.newSingleThreadScheduledExecutor();

    public WebTxnMsgConsumer(String topic) {
        super(topic);
    }

    public WebTxnMsgConsumer(String topic, String configFile) {
        super(topic, configFile);
    }

    public static void main(String[] args) {
        final String topicName = "WebTxn";
        WebTxnMsgConsumer webTxnConsumer = new WebTxnMsgConsumer(topicName);
        webTxnConsumer.initConsumer();
        webTxnConsumer.consumeMessages();
    }

    @Override
    public void consumeMessages() {
        //dispatcherThr.scheduleAtFixedRate(new DispatchMonitor(), 1l,1l, TimeUnit.SECONDS);

        Map<String, Integer> topicCount = new HashMap<>();
        // Define single thread for topic
        topicCount.put(topic, new Integer(1));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams =
                consumer.createMessageStreams(topicCount);

        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.
                get(topic);

        MessageBatch dataBatch = new MessageBatch();

        for (final KafkaStream stream : streams) {

            ConsumerIterator<byte[], byte[]> consumerIte = stream.
                    iterator();

            streamHandle = consumerIte;

            while (consumerIte.hasNext()) {
                lastTimeUpdated.set(System.currentTimeMillis());

                MessageAndMetadata<byte[], byte[]> payload = consumerIte.next();

                int partitionKey = payload.partition();
                long offset = payload.offset();

                dataBatch.getDataBatch().add(payload.message());
                //TODO: work on timed sending of messages when rcvd message is smaller
                if (dataBatch.getDataBatch().size() >= maxBatchSize) {
                    OffsetInfo offsetInfo = new OffsetInfo(topic, partitionKey, offset);
                    dataBatch.setOffsetInfo(offsetInfo);
                    //send it across
                    BatchPersistManager.getInstance().submitBatch(dataBatch);

                    dataBatch = new MessageBatch();
                }
            }//while

            System.out.println("Ended the while stream...");
        }//for streams

        // break in loop , send the last batch

    }

    @Override
    public void run() {
        consumeMessages();
    }

    private class DispatchMonitor implements Runnable {

        @Override
        public void run() {

            if (streamHandle == null) {
                System.out.println("stream handle is NULL");
            } else {

                long timeDiff = System.currentTimeMillis() - lastTimeUpdated.longValue();
                if (timeDiff > 500) {
                    System.out.println("Running dispatch monitor " + timeDiff);
                    try {

                        System.out.println("Client is " + streamHandle.clientId() + " " +
                                streamHandle.kafka$consumer$ConsumerIterator$$consumedOffset());

                        // streamHandle.next();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
