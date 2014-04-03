package dataflux.producer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.google.common.util.concurrent.RateLimiter;
import dataflux.common.type.WebTxnData;
import dataflux.common.util.KryoPool;
import dataflux.datagenerator.DataGenerator;
import dataflux.producer.util.DataGenManager;
import kafka.producer.KeyedMessage;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Feeds web transaction into the kafka
 * Created by sumanthn
 */
public class WebTxnKafkaProducer {

    private static Logger logger = Logger.getLogger(WebTxnKafkaProducer.class);
    private static WebTxnKafkaProducer ourInstance = new WebTxnKafkaProducer();
    private int maxTps = 50;
    private ExecutorService taskPool = Executors.newFixedThreadPool(2);
    private ScheduledExecutorService watchDogTask = Executors.newSingleThreadScheduledExecutor();

    private WebTxnKafkaProducer() {
    }

    public static WebTxnKafkaProducer getInstance() {
        return ourInstance;
    }

    /**
     * Args 0 config dir
     * 1 tps
     * 2 startDate in yyyy-mm-dd HH:mm:ss
     *
     * @param args
     */
    public static void main(String[] args) {

        if (!(args.length > 2)) {
            System.out.println("Speicfy ConfigDir Tps StartDateTime");
            return;
        }

        DataGenerator dataGenerator = new DataGenerator(args[0]);

        final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

        int tps = Integer.parseInt(args[1]);
        dataGenerator.initData();

        final String startTime = args[2];
        DateTime cur = DateTime.parse(startTime, DATE_TIME_FORMATTER);

        boolean isForever = true;
        DateTime endTs = null;
        if (args.length == 4){
        final String endTime = args[3];

        if (endTime != null) {
            endTs = DateTime.parse(endTime, DATE_TIME_FORMATTER);
            isForever = false;
        }
        }

        try {
            logger.info("Init Producer Pool ");
            KafkaProducerPool.getInstance().preFillPool();
        } catch (Exception e) {
            e.printStackTrace();
        }

        WebTxnKafkaProducer.getInstance().init();
        try {
            KafkaProducerPool.getInstance().preFillPool();
        } catch (Exception e) {
            e.printStackTrace();
        }

        DataGenManager.getInstance().initMBean();

        RateLimiter rateLimiter = RateLimiter.create(tps);
        Random tpsRandomizer = new Random();
        int nonNormalPeriod = -1;


        DataGenManager dataGenManager = DataGenManager.getInstance();
        dataGenManager.setNormalRate();
        List<WebTxnData> dataBatch = null;
        logger.info("Start transaction generation from " + cur.toString(DATE_TIME_FORMATTER) + " at " + tps);
        outer:
        while (true) {

            if (nonNormalPeriod == 0) {
                logger.info("Switching back to normal mode " + " " + cur.toString(DATE_TIME_FORMATTER));
                dataGenManager.setNormalRate();

                //rateLimiter.acquire(tps);
            }


         /*    dataBatch =
                    dataGenerator.generateDataBatch(tps, cur);*/

            //TODO: logic of determining the TPS rate should be moved to DataGenManager
            switch (dataGenManager.getDataGenState()) {

                case Bursts:
                    logger.info(" Data bursts " + dataGenManager.getBurstDataProperties().getBurstRate() + " " + cur.toString(DATE_TIME_FORMATTER));
                    dataBatch = dataGenerator.generateDataBatch(dataGenManager.getBurstDataProperties().getBurstRate(), cur);

                    if (nonNormalPeriod == -1) {
                        nonNormalPeriod = dataGenManager.getBurstDataProperties().getPeriod() - 1;
                    } else {
                        nonNormalPeriod--;
                    }

                    break;
                case Spike:

                    int newTps = tpsRandomizer.nextInt(4 * tps) + tps;
                    logger.info("Spiky data bursts " + newTps + " " + cur.toString(DATE_TIME_FORMATTER));
                    dataBatch = dataGenerator.generateDataBatch(newTps, cur);

                    if (nonNormalPeriod == -1) {
                        nonNormalPeriod = dataGenManager.getBurstDataProperties().getPeriod() - 1;
                    } else {
                        nonNormalPeriod--;
                    }

                    break;

                case Anomaly:
                    dataBatch = dataGenerator.generateDataBatch(tps, cur);

                    if (nonNormalPeriod == -1) {
                        nonNormalPeriod = dataGenManager.getBurstDataProperties().getPeriod() - 1;
                    } else {
                        nonNormalPeriod--;
                    }
                    break;

                case Normal:

                    nonNormalPeriod = -1;
                    dataBatch = dataGenerator.generateDataBatch(tps, cur);
                    break;
            }

            logger.info("batch of txn " + dataBatch.size());

            WebTxnKafkaProducer.getInstance().feedDataBatch(dataBatch);

            if (!isForever) {
                if (cur.isAfter(endTs)) break outer;
            }

            //TODO:need to adjust rate precisely here
            try {
                Thread.sleep(1 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            cur = cur.plusSeconds(1);
        }

        dataGenManager.setNormalRate();

        KafkaProducerPool.getInstance().closePool();
    }

    public synchronized void init() {

        watchDogTask.scheduleAtFixedRate(new WatchDogTask(), 5L, 10L, TimeUnit.SECONDS);
    }

    public void feedDataBatch(final List<WebTxnData> dataBatch) {
        //taskPool.execute(new DispatchMsgBatch(dataBatch));
    }

    private class DispatchMsgBatch implements Runnable {

        public final List<WebTxnData> dataBatch;
        Random random = new Random();
        KryoPool kryoPool = KryoPool.getInstance();

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
                    byte[] keyBytes = UUID.randomUUID().toString().getBytes();

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
}
