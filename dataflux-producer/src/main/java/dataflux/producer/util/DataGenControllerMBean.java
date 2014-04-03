package dataflux.producer.util;

/**
 * Created by sumanthn on 28/3/14.
 */
public interface DataGenControllerMBean {

    /** Induce bursts for specified TPS for given period*/
    public String generateTrafficBursts(Integer tps, Integer period);

    /** generates spikes during a period */
    public String generateRandomSpikes(Integer period);

}
