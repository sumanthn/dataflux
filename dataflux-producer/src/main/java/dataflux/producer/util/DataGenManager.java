package dataflux.producer.util;

import org.apache.log4j.Logger;

import javax.management.*;
import java.lang.management.ManagementFactory;

/**
 * Created by sumanthn on 28/3/14.
 */
public class DataGenManager {

    private Logger logger = Logger.getLogger(DataGenManager.class);

    public enum DataGenState {
        Anomaly,
        Bursts,
        Spike,
        Normal

    }

    private BurstDataProperties burstDataProperties;
    private int spikerPeriod;

    //currently only one state is supported
    //TODO: support multiple states like Anomaly & Bursty
    private volatile DataGenManager.DataGenState dataGenState;

    private static DataGenManager ourInstance = new DataGenManager();

    public static DataGenManager getInstance() {
        return ourInstance;
    }

    private DataGenManager() {
    }

    public synchronized boolean setDataBursts(int tps, int period) {
        if (dataGenState != DataGenState.Normal) {
            return false;
        } else {
            dataGenState = DataGenState.Bursts;
            burstDataProperties = new BurstDataProperties(tps, period);
        }

        return true;
    }

    public synchronized boolean setSpikyDataRate(int period) {
        if (dataGenState != DataGenState.Normal) {
            return false;
        } else {
            dataGenState = DataGenState.Spike;
            spikerPeriod = period;
        }

        return true;
    }

    public synchronized boolean setAnomalousData(int period) {
        if (dataGenState != DataGenState.Normal) {
            return false;
        } else {
            dataGenState = DataGenState.Anomaly;
        }

        return true;
    }

    public synchronized void setNormalRate() {
        dataGenState = DataGenState.Normal;
    }

    public DataGenState getDataGenState() {
        return dataGenState;
    }

    public boolean isNormal(){
        if (dataGenState == DataGenState.Normal)
            return true;

        return false;
    }

    public BurstDataProperties getBurstDataProperties() {
        return burstDataProperties;
    }

    public int getSpikerPeriod() {
        return spikerPeriod;
    }


    public void initMBean(){
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = null;
        try {
            name = new ObjectName("dataflux.producer:util=DataGenController");
            DataGenController mbean = new DataGenController();
            mbs.registerMBean(mbean, name);

        } catch (MalformedObjectNameException e) {
            e.printStackTrace();
        } catch (NotCompliantMBeanException e) {
            e.printStackTrace();e.printStackTrace();
        } catch (InstanceAlreadyExistsException e) {
            e.printStackTrace();
        } catch (MBeanRegistrationException e) {
            e.printStackTrace();
        }
    }
}
