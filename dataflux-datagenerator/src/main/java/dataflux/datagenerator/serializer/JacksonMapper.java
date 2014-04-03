package dataflux.datagenerator.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import dataflux.common.type.WebTxnData;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Test Jackson mapper
 * Created by sumanthn
 */
public class JacksonMapper {

    private static class SummaryData{
        double avg;
        double min;
        double max;
        double stdDev;

        private SummaryData(double avg, double min, double max, double stdDev) {
            this.avg = avg;
            this.min = min;
            this.max = max;
            this.stdDev = stdDev;
        }
    }
   static final ObjectMapper mapper = new ObjectMapper(new SmileFactory());
    static DescriptiveStatistics statsSerializeNanoSeconds =  new DescriptiveStatistics();


    static DescriptiveStatistics statsDeSerializeNanoSeconds =  new DescriptiveStatistics();


    static DescriptiveStatistics statsSize =  new DescriptiveStatistics();



    static List<SummaryData> statsDataSerializer = new ArrayList<SummaryData>();
    static List<SummaryData> statsDataDeSerializer = new ArrayList<SummaryData>();

    public static void serializeDeserialize(final List<WebTxnData> txnData){


        for(int i=0;i < 10;i++){
            int recordCount =0;

            int warmupAt =10000;

            for(WebTxnData txn : txnData){


                long startM = System.nanoTime();
                byte[] carBytes = new byte[0];
                try {
                    carBytes = mapper.writeValueAsBytes(txn);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                long endM = System.nanoTime();

                if (recordCount > warmupAt)
                    statsSerializeNanoSeconds.addValue(endM -startM);

                long startU = System.nanoTime();
                try {
                    WebTxnData output = mapper.readValue(carBytes, WebTxnData.class);
                    output.getUrl();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                long endU = System.nanoTime();

                if (recordCount > warmupAt)
                    statsDeSerializeNanoSeconds.addValue(endU-startU);


                recordCount++;
            }

            SummaryData summaryDataSerializer = new SummaryData(statsSerializeNanoSeconds.getMean(),statsSerializeNanoSeconds.getMin(),statsSerializeNanoSeconds.getMax(),statsSerializeNanoSeconds.getStandardDeviation());
            SummaryData summaryDataDeSerializer = new SummaryData(statsDeSerializeNanoSeconds.getMean(),
                    statsDeSerializeNanoSeconds.getMin(),statsDeSerializeNanoSeconds.getMax(),statsDeSerializeNanoSeconds.getStandardDeviation());

            statsDataSerializer.add(summaryDataSerializer);
            statsDataDeSerializer.add(summaryDataDeSerializer);
            statsDeSerializeNanoSeconds.clear();
            statsSerializeNanoSeconds.clear();

            }



        /*System.out.println("Serilizaer data");
        for(SummaryData serializerSummary : statsDataSerializer){

            System.out.println(serializerSummary.avg +","+serializerSummary.min + "," + serializerSummary.max + "," + serializerSummary.stdDev);
        }



        System.out.println("DESerilizaer data");
        for(SummaryData serializerSummary : statsDataDeSerializer){

            System.out.println(serializerSummary.avg +","+serializerSummary.min + "," + serializerSummary.max + "," + serializerSummary.stdDev);
        }*/


        writeResultsToCsv(statsDataSerializer,"/tmp/jacksonSerializer.csv");
        writeResultsToCsv(statsDataDeSerializer,"/tmp/jacksonDeSerializer.csv");


    }

    static void writeResultsToCsv(final List<SummaryData> summaryDataBag, final String fileName){

        DecimalFormat formatter = new DecimalFormat("#.####");
        final String flddelim=",";
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));

            for(SummaryData sdata : summaryDataBag){


                StringBuilder sb = new StringBuilder();
                sb.append(formatter.format(sdata.avg)).append(flddelim).
                        append(formatter.format(sdata.min)).append(flddelim).
                        append(formatter.format(sdata.max)).append(flddelim).append(formatter.format(sdata.stdDev));
                writer.write(sb.toString());
                writer.newLine();
            }


            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }



}
