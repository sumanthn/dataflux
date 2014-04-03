package dataflux.persist;

import dataflux.common.util.PermitBasedBlockingExecutor;
import dataflux.common.util.ResizeableSemaphore;
import org.apache.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages persistence tasks
 * Created by sumanthn
 */

//TODO: Handle graceful shutdown
public class BatchPersistManager {

    private static final Logger logger = Logger.getLogger(BatchPersistManager.class);
    private static BatchPersistManager ourInstance = new BatchPersistManager();
    private ScheduledExecutorService monitorTask = Executors.newSingleThreadScheduledExecutor();
    private ResizeableSemaphore permits;
    private PermitBasedBlockingExecutor blockingExecutor;
    private double highWaterMarkPct = 0.75d;

    private BatchPersistManager() {
    }

    public static BatchPersistManager getInstance() {
        return ourInstance;
    }

    public synchronized void init(int initialThreadsCount,
                                  int maxThreadsCount,
                                  int maxQueueSize) {

        blockingExecutor = new PermitBasedBlockingExecutor(initialThreadsCount, maxThreadsCount, maxQueueSize,
                "BatchPersistWebTxn");
        int maxPermits = maxThreadsCount + maxQueueSize;
        permits = new ResizeableSemaphore(maxPermits);
        monitorTask.scheduleAtFixedRate(new MonitorTask(), 1L, 15L, TimeUnit.SECONDS);
    }

    /**
     * Can block if no permits are available
     */
    public void submitBatch(final MessageBatch dataBatch) {

        System.out.println("Received Message batch of size " + dataBatch.toString());
        blockingExecutor.execute(new WebTxnPersistTask(dataBatch));
    }

    private class MonitorTask implements Runnable {

        //track blocked in last min
        private int blockedCount;
        private int onHigherMark;

        @Override
        public void run() {

            if (blockingExecutor.isFull()) {
                logger.info("Producer may be blocked " + " Batches in queue " + blockingExecutor.getQueueSize() + " " + permits.toString());
            } else {

                if (blockingExecutor.getQueueSize() > (highWaterMarkPct * blockingExecutor.getQueueSize())) {
                    if (onHigherMark > 5)
                        logger.warn("Persist queue on high water mark");
                    onHigherMark++;
                } else {
                    onHigherMark--;
                }

         /*   if (onHigherMark > 10){
                logger.info("Increasing 10% of permits to keep producers happy");
                permits.increasePermits((int) Math.ceil(permits.getInitPermits()*0.10));

            }*/
            }
        }
    }
}
