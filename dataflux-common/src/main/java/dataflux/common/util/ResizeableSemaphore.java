package dataflux.common.util;

import java.util.concurrent.Semaphore;

/**
 * A semaphore which can be dynamically resized
 * Created by sumanthn
 */
public class ResizeableSemaphore extends Semaphore {

    int initPermits;

    public ResizeableSemaphore(int permits) {
        super(permits);
        this.initPermits = permits;
    }

    @Override
    protected void reducePermits(int reduction) {
        super.reducePermits(reduction);
    }

    public synchronized void increasePermits(int amount) {
        this.release(amount);
    }

    public int getInitPermits() {
        return initPermits;
    }

    @Override
    public String toString() {
        return "ResizeableSemaphore{" +
                "initPermits=" + initPermits + " " + super.toString() +
                '}';
    }
}
