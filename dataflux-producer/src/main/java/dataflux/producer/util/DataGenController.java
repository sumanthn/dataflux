package dataflux.producer.util;

/**
 * Created by sumanthn on 28/3/14.
 */
public class DataGenController implements DataGenControllerMBean{


    DataGenManager dataGenManager = DataGenManager.getInstance();
    @Override
    public String generateTrafficBursts(Integer tps, Integer period) {
        if (dataGenManager.setDataBursts(tps,period))
            return  "Successfully set bursts in data generation";

        return "Failed to set bursts in data generation";
    }

    @Override
    public String generateRandomSpikes(Integer period) {

        if (dataGenManager.setSpikyDataRate(period))
        return  "Successfully set spikes in data generation";

        return "Failed to set spikes in data generation";
    }
}
