package dataflux.producer.util;

/**
 * Created by sumanthn on 28/3/14.
 */
public class BurstDataProperties {

    private final int burstRate;
    private final int period;

    public BurstDataProperties(int burstRate, int period) {
        this.burstRate = burstRate;
        this.period = period;
    }

    public int getBurstRate() {
        return burstRate;
    }

    public int getPeriod() {
        return period;
    }
}
