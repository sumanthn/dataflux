package dataflux.persist;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import dataflux.common.util.HandleThreadException;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Manages all startup activity of the consumer
 * Created by sumanthn
 */
//TODO: handle starting from last persisted offset
    // Externalize queue and other consumer configuration
public class ConsumerStartup {

    private static final Logger logger = Logger.getLogger(ConsumerStartup.class);
    private static ConsumerStartup ourInstance = new ConsumerStartup();
    private ExecutorService consumerTasks;

    private ConsumerStartup() {
    }

    public static ConsumerStartup getInstance() {
        return ourInstance;
    }

    public static void main(String[] args) {

        ConsumerStartup consumerStartup = ConsumerStartup.getInstance();
        consumerStartup.init(4);
    }

    public void init(int consumerPartitions) {

        int coreThreads = (int) Math.ceil(1.5 * Runtime.getRuntime().availableProcessors()); // most are I/O bound tasks
        int maxThreads = coreThreads + Runtime.getRuntime().availableProcessors(); //don't overwhelm system
        int queueSize = 10; //don't accumulate too many batches else there could be data loss
        BatchPersistManager.getInstance().init(coreThreads, maxThreads, queueSize);

        //now start consumer
        //how many is based on partitions start n/2 partitions

        int consumersCount = Math.round(consumerPartitions / 2);

        //TODO: start up to include offset manager and recovery from where we left last time processing

        final String poolName = "WebTxnConsumers";
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(poolName + "-%d").
                setUncaughtExceptionHandler(new HandleThreadException()).
                build();

        consumerTasks = Executors.newFixedThreadPool(consumersCount, namedThreadFactory);
        final String topicName = "WebTxn";

        for (int i = 0; i < consumersCount; i++) {

           /* WebTxnConsumer webTxnConsumer = new WebTxnConsumer(topicName);
            webTxnConsumer.initConsumer();
            consumerTasks.execute(webTxnConsumer);*/

            WebTxnMsgConsumer webTxnMsgConsumer = new WebTxnMsgConsumer(topicName);
            webTxnMsgConsumer.initConsumer();
            consumerTasks.execute(webTxnMsgConsumer);
        }
        logger.info("Started persistence engine with " + maxThreads + "Threds" + " Queue size  " + queueSize + " and consumer " + consumersCount);
    }
}
