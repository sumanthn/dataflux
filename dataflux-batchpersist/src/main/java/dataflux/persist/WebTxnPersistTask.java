package dataflux.persist;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.base.Stopwatch;
import dataflux.common.type.WebTxnData;
import dataflux.common.util.KryoPool;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Persist the raw transaction data
 * <p/>
 * Created by sumanthn
 */
//TODO: Persist the offset data of the batch
public class WebTxnPersistTask implements Runnable {

    private static Logger logger = Logger.getLogger(WebTxnPersistTask.class);
    private List<WebTxnData> dataBatch;
    private MessageBatch messageBatch;

    public WebTxnPersistTask(MessageBatch messageBatch) {
        this.messageBatch = messageBatch;
    }

    @Override
    public void run() {

        List<byte[]> rawDataBatch = messageBatch.getDataBatch();

        if (rawDataBatch != null) {
            //convert your data first
            dataBatch = new ArrayList<>(rawDataBatch.size());
            //kryopool in action
            KryoPool pool = KryoPool.getInstance();
            Kryo kryoDeserializer = pool.getKryo();
            try {

                for (byte[] incomingData : rawDataBatch) {

                    Input input = new Input();
                    input.setBuffer(incomingData);
                    WebTxnData txnDataItem = kryoDeserializer.readObject(input, WebTxnData.class);
                    dataBatch.add(txnDataItem);
                }
            } catch (Exception e) {

                logger.warn("Error in deserializing raw message " + e.getCause());
                e.printStackTrace();
            } finally {

                pool.returnToPool(kryoDeserializer);
            }
        }

        if (dataBatch != null) {

            //simulate slowness
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //TODO:persist the offset info and restart the topic with the same info
            Stopwatch stopwatch = Stopwatch.createStarted();
            System.out.println("Persisting data batch " + dataBatch.size() + " with offset info " + messageBatch.getOffsetInfo().toString());
            stopwatch.stop();

            if (logger.isTraceEnabled())
                logger.trace("Time in persisting data batch of size " + dataBatch.size() + " is " +
                        stopwatch.elapsed(TimeUnit.MILLISECONDS) + " " + Thread.currentThread().getName() +
                        " OFFSet Info " + messageBatch.getOffsetInfo().toString());
        }
    }
}
