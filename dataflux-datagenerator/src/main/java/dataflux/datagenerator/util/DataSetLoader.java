package dataflux.datagenerator.util;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import dataflux.datagenerator.types.ShoppingItem;
import dataflux.datagenerator.types.TxnType;
import dataflux.datagenerator.types.WebTxn;
import dataflux.datagenerator.types.WebTxnFlow;
import org.apache.log4j.Logger;
import org.supercsv.cellprocessor.CellProcessorAdaptor;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads the various data sets from configuration files
 * Created by sumanthn
 */
public class DataSetLoader {

    static final Logger logger = Logger.getLogger(DataSetLoader.class);

    //crunches the txndata
    public static ImmutableList<WebTxn> readTxns(final String fileName) {

        List<WebTxn> txns = new ArrayList<WebTxn>();
        try {
            CsvBeanReader reader = new CsvBeanReader(new FileReader(fileName),
                    CsvPreference.STANDARD_PREFERENCE);
            try {
                String[] headers = reader.getHeader(true);

                CellProcessor[] processors = {
                        new ParseInt(),
                        null,
                        null,
                        new ParseInt(),
                        new ParseCustomAttrs()
                        //null
                };

                WebTxn webTxn = reader.read(WebTxn.class, headers, processors);

                while (webTxn != null) {

                    // System.out.println(reader.getRowNumber());
                    // System.out.println(" ");
                    //System.out.println(webTxn.getId() + " " + webTxn.getUrl() + " " + webTxn.getOpcode() + " custom attrs size " + webTxn.getCustomAttributes().size());

                    txns.add(webTxn);

                    webTxn = reader.read(WebTxn.class, headers, processors);
                }

                reader.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        logger.debug("Loaded Transaction details from file, a total of " + txns.size());
        return ImmutableList.copyOf(txns);
    }

    public static ImmutableList<ShoppingItem> readItemsData(final String fileName) {

        CsvBeanReader reader = null;
        try {
            reader = new CsvBeanReader(new FileReader(fileName),
                    CsvPreference.STANDARD_PREFERENCE);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        List<ShoppingItem> items = new ArrayList<ShoppingItem>();
        try {
            String[] headers = reader.getHeader(true);
            CellProcessor[] processors = {
                    new ParseInt(), null, null
            };

            ShoppingItem shoppingItem = reader.read(ShoppingItem.class, headers, processors);
            while (shoppingItem != null) {
                items.add(shoppingItem);
                shoppingItem = reader.read(ShoppingItem.class, headers, processors);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.debug("Loaded shopping items from file, total items are " + items.size());
        return ImmutableList.copyOf(items);
    }

    public static ImmutableMap<Integer, String> readUserIds(final String fileName) {

        Map<Integer, String> userIdMap = new HashMap<Integer, String>();

        try {

            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line = reader.readLine();
            //skip header
            // line = reader.readLine();

            while (line != null) {

                String[] tkns = line.split(",");
                userIdMap.put(Integer.valueOf(tkns[0]), tkns[1]);

                line = reader.readLine();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.debug("Filled up user id map with" + userIdMap.size() + " keys");
        return ImmutableMap.copyOf(userIdMap);
    }

    public static List<WebTxnFlow> loadFlowPattern(final String fileName) {

        List<WebTxnFlow> flowPattern = new ArrayList<WebTxnFlow>();

        try {

            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line = reader.readLine();

            while (line != null) {

                String[] tkns = line.split(",");
                if (tkns.length == 2) {

                    Iterable<String> strs = Splitter.on("|").omitEmptyStrings().split(tkns[1]);
                    List<TxnType> txnTypes = new ArrayList<TxnType>();
                    for (String str : strs) {
                        txnTypes.add(TxnType.valueOf(str));
                    }

                    WebTxnFlow flow = new WebTxnFlow(Integer.valueOf(tkns[0]).intValue(),
                            txnTypes);

                    flowPattern.add(flow);
                }

                line = reader.readLine();
            }

            reader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.debug("Loaded Transaction flow patterns " + flowPattern.size());
        return ImmutableList.copyOf(flowPattern);
    }

    public static void main(String[] args) {

        final String fileName = "/home/sumanthn/datasets/txn/Txndetails.csv";
        final String itemsFile = "/home/sumanthn/datasets/txn/Itemnames.csv";

        final String userIdFile = "/home/sumanthn/datasets/txn/Userdata.csv";

        final String flowPatternFile = "/home/sumanthn/datasets/txn/Txnflow.csv";
        List<WebTxn> allTxn = DataSetLoader.readTxns(fileName);

        System.out.println(allTxn.size());

        List<ShoppingItem> allItems = DataSetLoader.readItemsData(itemsFile);
        System.out.println("Shopping items " + allItems.size());

        Map<Integer, String> userIdMap = DataSetLoader.readUserIds(userIdFile);
        System.out.println("User ids " + userIdMap.size());

        List<WebTxnFlow> flows = DataSetLoader.loadFlowPattern(flowPatternFile);

        System.out.println("Flow data  " + flows.size());
    }

    static class ParseCustomAttrs extends CellProcessorAdaptor {

        @Override
        public Object execute(Object val, CsvContext csvContext) {

            String[] strs = ((String) val).split("\\|");
            ArrayList<String> attrs = Lists.newArrayList();
            if (strs.length > 0) {
                for (String str : strs)
                    attrs.add(str);
            }

            return (ArrayList<String>) attrs;
        }
    }
}
