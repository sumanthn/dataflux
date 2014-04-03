package dataflux.producer;

import dataflux.common.type.WebTxnData;
import dataflux.datagenerator.DataGenerator;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;

/**
 * Created by sumanthn on 24/3/14.
 */
public class DataGen {


    public static void main(String [] args){
        if (args.length!=1)
            System.out.println("Specify the Configuration directory");

        DataGenerator dataGenerator = new DataGenerator(args[0]);
        final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");


        dataGenerator.initData();
        int tps = 5000;
        final String startTime = "2014-01-01 00:00:00";
        DateTime cur = DateTime.parse(startTime, DATE_TIME_FORMATTER);
        for(int i=0;i < 400;i++){


            List<WebTxnData> txnData = dataGenerator.generateDataBatch(tps,cur);


            System.out.println(txnData.size());
            cur = cur.plusSeconds(1);
        }
    }

}
