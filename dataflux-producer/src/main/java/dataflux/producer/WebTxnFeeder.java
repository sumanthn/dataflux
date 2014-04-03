package dataflux.producer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import dataflux.common.type.WebTxnData;
import dataflux.common.util.KryoPool;
import kafka.producer.KeyedMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Reads from the file and pumps data into kafka
 * Created by sumanthn on 24/3/14.
 */
public class WebTxnFeeder {

    private static Logger logger = Logger.getLogger(WebTxnFeeder.class);
    private String fileName;
    private int maxTps = 50;

    private ExecutorService taskPool = Executors.newFixedThreadPool(2);

    private ScheduledExecutorService watchDogTask = Executors.newSingleThreadScheduledExecutor();

    private class DispatchMsgBatch implements Runnable {

        Random random = new Random();
        KryoPool kryoPool = KryoPool.getInstance();
        public final List<WebTxnData> dataBatch;

        private DispatchMsgBatch(List<WebTxnData> dataBatch) {
            this.dataBatch = dataBatch;
        }

        @Override
        public void run() {

            Kryo kryoInstance = kryoPool.getKryo();
            KafkaProducerPool pool = KafkaProducerPool.getInstance();
            KafkaProducer producer = pool.getProducer();

            try {

                for (WebTxnData dataItem : dataBatch) {

                    ByteBufferOutput bufferOutput = new ByteBufferOutput(4096);
                    kryoInstance.writeObject(bufferOutput, dataItem);
                    byte[] msgBytes = bufferOutput.toBytes();

                    bufferOutput.clear();

                    //serialize item
                    byte[] keyBytes = String.valueOf(random.nextInt()).getBytes();

                    KeyedMessage<byte[], byte[]> message = new KeyedMessage<byte[], byte[]>(MsgConstants.WEB_TXN_TOPIC_NAME,
                            keyBytes,
                            msgBytes);
                    // message producer is Async , it will queue and send out the pipe
                    //order doesn't matter
                    producer.sendMsg(message);
                }
                dataBatch.clear();
            } catch (Exception e) {
                logger.warn("Exception in sending message");
            } finally {

                pool.returnToPool(producer);
                kryoPool.returnToPool(kryoInstance);
            }
        }
    }

    private class WatchDogTask implements Runnable {

        @Override
        public void run() {
            //checks the queue health of the taskpool
            int queuedBatches = ((ThreadPoolExecutor) (taskPool)).getQueue().size();
            logger.info("Queued dispatcher tasks " + queuedBatches);
        }
    }

    private void init() {

        watchDogTask.scheduleAtFixedRate(new WatchDogTask(), 5L, 10L, TimeUnit.SECONDS);
    }

    public void feedTransactions() {

        int batchsize = 0;
        if (maxTps < 500) {
            batchsize = maxTps;
        } else {
            batchsize = 500;
        }
        logger.info("Start feeding from file " + fileName + " @" + maxTps);

        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line = reader.readLine();

            RateLimiter rateLimiter = RateLimiter.create(maxTps);
            int recordCount = 0;
            List<WebTxnData> dataBatch = new ArrayList<WebTxnData>(batchsize);
            while (line != null) {

                rateLimiter.acquire();
                // line  = "11888367,/funkyfashion/gift.html,1388514601020,GIFT,,200,928,4680,153.10.104.249,215.27.84.91,Chrome WEBKIT,\"ITEMSBAG=swatch_beige-tan,swatch_PDP_black,work,swatch_PDP_multi\"\n";
                //open CSV can be used

                recordCount++;
                WebTxnData dataItem = makeObject(line);

                if (dataItem != null)
                    dataBatch.add(dataItem);

                if (dataBatch.size() >= batchsize) {

                    logger.info("Submitting batch for dispatch" + System.currentTimeMillis());

                    taskPool.submit(new DispatchMsgBatch(dataBatch));

                }

                line = reader.readLine();
            }

            if (dataBatch.size() > 0)
                taskPool.submit(new DispatchMsgBatch(dataBatch));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("Extracted all messages from file " + fileName);
        //call for shutdown

    }

    public WebTxnFeeder(String fileName) {
        this.fileName = fileName;
        init();
    }

    public WebTxnFeeder(String fileName, final int tps) {
        this.fileName = fileName;
        this.maxTps = tps;
        init();
    }

    private WebTxnData makeObject(final String line) {
        String[] tkns = line.split(",");
        if (tkns.length > 0) {

            WebTxnData dataItem = new WebTxnData();
            dataItem.setTxnId(Integer.valueOf(tkns[0]));
            dataItem.setUrl(tkns[1]);
            dataItem.setTimestamp(Long.valueOf(tkns[2]));
            dataItem.setOperation(tkns[3]);
            if (!Strings.isNullOrEmpty(tkns[4]))
                dataItem.setUserId(Integer.valueOf(tkns[4]));

            dataItem.setHttpCode(Integer.valueOf(tkns[5]));
            dataItem.setResponseTime(Integer.valueOf(tkns[6]));

            dataItem.setServerIp(tkns[7]);
            dataItem.setClientIp(tkns[8]);
            dataItem.setClientAgent(tkns[9]);

            String customStr = StringUtils.substringBetween(line, "\"");
            if (!Strings.isNullOrEmpty(customStr)) {

                String[] customData = customStr.split("=");
                if (customData.length == 2) {
                    //  System.out.println("set pair data " + customData [0] + " " + customData[1]);
                    Map<String, String> data = new HashMap<String, String>();
                    data.put(customData[0], customData[1]);
                    dataItem.setCustomData(data);
                }
            }
            return dataItem;
        }

        return null;
    }

    public static void main(String[] args) {


        Preconditions.checkNotNull(args[0], "Please provide the file to extract data");
        WebTxnFeeder txnFeeder = null;
        if (args[1] != null) {
            txnFeeder = new WebTxnFeeder(args[0], Integer.valueOf(args[1]));
        } else {
            txnFeeder = new WebTxnFeeder(args[0]);
        }

        logger.info("Starting data feed from file " + args[0]);

        txnFeeder.feedTransactions();
    }
}
