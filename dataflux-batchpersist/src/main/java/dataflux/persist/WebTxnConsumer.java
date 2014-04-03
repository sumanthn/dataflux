package dataflux.persist;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import dataflux.common.type.WebTxnData;
import dataflux.common.util.KryoPool;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Message consumer with serialization in place
 * Created by sumanthn
 */
public class WebTxnConsumer extends KafkaConsumer implements Runnable {

    private static final Logger logger = Logger.getLogger(WebTxnConsumer.class);
    // 5 seconds, the data is not for realtime processing
    //Bulk persist data is more relevant & advantageous
    int maxBatchSize = 1000;
    int maxWaitTime = 5 * 1000;

    public WebTxnConsumer(String topic) {
        super(topic);
    }

    public WebTxnConsumer(String topic, String configFile) {
        super(topic, configFile);
    }

    public static void main(String[] args) {
        final String topicName = "WebTxn";
        WebTxnConsumer webTxnConsumer = new WebTxnConsumer(topicName);
        webTxnConsumer.initConsumer();
        webTxnConsumer.consumeMessages();
    }

    @Override
    public void consumeMessages() {
        KryoPool kryoPool = KryoPool.getInstance();
        Kryo kryo = kryoPool.getKryo();
        Map<String, Integer> topicCount = new HashMap<>();
        // Define single thread for topic
        topicCount.put(topic, new Integer(1));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams =
                consumer.createMessageStreams(topicCount);

        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.
                get(topic);

        List<WebTxnData> dataBatch = new ArrayList<>();
        long lastBatchDispatcedTime = -1;
        for (final KafkaStream stream : streams) {

            ConsumerIterator<byte[], byte[]> consumerIte = stream.
                    iterator();
            while (consumerIte.hasNext()) {

                MessageAndMetadata<byte[], byte[]> msg = consumerIte.next();
               /* System.out.println(new String(msg.key())
                        +" coming from " +  msg.partition() + "@ offset "
                        +msg.offset() + " client id " +consumerIte.clientId()  + " " + Thread.currentThread().getId()) ;
                */
                Input input = new Input();
                input.setBuffer(consumerIte.next().message());
                WebTxnData txnDataRead = kryo.readObject(input, WebTxnData.class);

                dataBatch.add(txnDataRead);

                if (dataBatch.size() >= maxBatchSize) {
                    //BatchPersistManager.getInstance().submitBatch(dataBatch);
                    dataBatch = new ArrayList<>(maxBatchSize);
                    lastBatchDispatcedTime = System.currentTimeMillis();
                } else if ((System.currentTimeMillis() - lastBatchDispatcedTime) > maxWaitTime) {

                    //  BatchPersistManager.getInstance().submitBatch(dataBatch);
                    lastBatchDispatcedTime = System.currentTimeMillis();
                    dataBatch = new ArrayList<>(maxBatchSize);
                } else {

                    logger.trace("Consumer waiting for more data to send data across " + dataBatch.size()
                            + " " + lastBatchDispatcedTime);
                }
            }//while
        }//for streams

        // break in loop , send the last batch
        //BatchPersistManager.getInstance().submitBatch(dataBatch);

        kryoPool.returnToPool(kryo);
    }

    @Override
    public void run() {
        consumeMessages();
    }
}
