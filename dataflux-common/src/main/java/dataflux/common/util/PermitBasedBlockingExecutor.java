package dataflux.common.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.log4j.Logger;

import java.util.concurrent.*;

/**
 * Poor mans Flow Control , Blocks submitters based on permits
 * Queue is unbounded control is enforced by using semaphores(permits)
 * Also, provides way to re-size permits using dynamically resizable semaphores
 * Increase queues size by increasing in permits
 * Decrease producer rate by decreasing permits
 * Created by sumanthn
 */
public class PermitBasedBlockingExecutor extends ThreadPoolExecutor {

    private static final Logger logger = Logger.getLogger(PermitBasedBlockingExecutor.class);
    private final ResizeableSemaphore semaphore;
    private int maxPermits;

    public PermitBasedBlockingExecutor(final int corePoolSize, final int maxPoolSize, final int queueSize,
                                       final String poolName) {
        super(corePoolSize, maxPoolSize, 5L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        maxPermits = maxPoolSize + queueSize;
        // the semaphore is bounding both the number of tasks currently executing
        // and those queued up
        semaphore = new ResizeableSemaphore(maxPermits);
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(poolName + "-%d").
                setUncaughtExceptionHandler(new HandleThreadException()).
                build();

        this.setThreadFactory(namedThreadFactory);
    }

    public boolean isFull() {
        return semaphore.hasQueuedThreads();
    }

    public int getQueueSize() {
        return super.getQueue().size();
    }

    public void increasePermits(int amount) {
        semaphore.increasePermits(amount);
    }

    public void reducePermits(int amount) {
        semaphore.reducePermits(amount);
    }

    @Override
    public void execute(final Runnable task) {
        boolean acquired = false;
        do {
            try {
                semaphore.acquire();
                acquired = true;
            } catch (final InterruptedException e) {
                logger.warn("Interrupt during task execution", e);
            }
        } while (!acquired);

        try {
            super.execute(task);
        } catch (final RejectedExecutionException e) {
            semaphore.release();
            throw e;
        }
    }

    @Override
    protected void afterExecute(final Runnable r, final Throwable t) {
        super.afterExecute(r, t);
        semaphore.release();
    }
}
