package dataflux.producer;

import dataflux.common.type.WebTxnData;
import dataflux.datagenerator.DataGenerator;
import dataflux.producer.util.DataGenManager;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;
import java.util.Random;

/**
 * Feeds in data as Continous stream
 * Created by sumanthn on 24/3/14.
 */
public class WebTxnStreamFeeder {


    public static void main(String [] args){

        DataGenManager dataGenManager = DataGenManager.getInstance();
        if (args.length!=1)
            System.out.println("Specify the Configuration directory");

        DataGenerator dataGenerator = new DataGenerator(args[0]);
        final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

        dataGenManager.setNormalRate();

        dataGenerator.initData();
        int tps = 5000;
        final String startTime = "2014-01-01 00:00:00";
        DateTime cur = DateTime.parse(startTime, DATE_TIME_FORMATTER);
        Random tpsRandomizer = new Random();
        int nonNormalPeriod = -1;
        for(int i=0;i < 400;i++){


            if (i == 60){
                dataGenManager.setDataBursts(2*tps,10);
            }
            if (i== 80){
                dataGenManager.setSpikyDataRate(10);
            }

            List<WebTxnData> txnData =null;
            /*if (dataGenManager.isNormal()){
                 txnData= dataGenerator.generateDataBatch(tps,cur);
            }
            */

            if (nonNormalPeriod == 0) {
                dataGenManager.setNormalRate();
            }
            switch (DataGenManager.getInstance().getDataGenState()){

                case Bursts:
                    System.out.println(" Data bursts "  +   dataGenManager.getBurstDataProperties().getBurstRate());
                    txnData= dataGenerator.generateDataBatch( dataGenManager.getBurstDataProperties().getBurstRate(),cur);

                    if (nonNormalPeriod == -1){
                        nonNormalPeriod =  dataGenManager.getBurstDataProperties().getPeriod()-1;
                    }else{
                        nonNormalPeriod --;
                    }

                    break;
                case Spike:

                    int newTps  = tpsRandomizer.nextInt(4*tps) + tps;
                    System.out.println("Spiky data bursts "  +  newTps);
                    txnData= dataGenerator.generateDataBatch( newTps,cur);

                    if (nonNormalPeriod == -1){
                        nonNormalPeriod =  dataGenManager.getBurstDataProperties().getPeriod()-1;
                    }else{
                        nonNormalPeriod --;
                    }

                    break;


                case Anomaly:
                    break;

                case Normal:
                    System.out.println("Switching back to normal mode");
                    nonNormalPeriod=-1;
                    txnData=dataGenerator.generateDataBatch(tps,cur);
                    break;
            }


            System.out.println(txnData.size());
            cur = cur.plusSeconds(1);
        }
    }

}
