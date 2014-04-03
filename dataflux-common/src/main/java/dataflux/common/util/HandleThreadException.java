package dataflux.common.util;

import org.apache.log4j.Logger;

/**
 * Generic Exception handler
 * Created by sumanthn
 */
public class HandleThreadException implements Thread.UncaughtExceptionHandler {

    private static final Logger logger = Logger.getLogger(HandleThreadException.class);

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        logger.fatal("Exception in processing thread " + t.getId() + " " + t.getName() + " " + t.getThreadGroup().getName());
        logger.fatal(e.getMessage() + " " + e.getCause());
        e.printStackTrace();
    }
}
