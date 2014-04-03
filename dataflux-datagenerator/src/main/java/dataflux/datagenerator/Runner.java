package dataflux.datagenerator;

import dataflux.common.type.WebTxnData;
import dataflux.datagenerator.types.ShoppingItem;
import dataflux.datagenerator.types.TxnType;
import dataflux.datagenerator.types.WebTxn;
import dataflux.datagenerator.types.WebTxnFlow;
import dataflux.datagenerator.util.DataSetLoader;
import dataflux.datagenerator.util.TxnInstanceBuilder;
import gnu.trove.list.array.TIntArrayList;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Creates and dumps transaction to  a file
 * Created by sumanthn
 */
public class Runner {

    static Random shufflerR = new Random();
    final String configDir;
    String outFile;
    List<WebTxnData> allData = new ArrayList<WebTxnData>();
    List<String> clientIpAddressBag = new ArrayList<String>();
    List<String> serverIpAddressBag = new ArrayList<String>();
    //minified versions, not use full strings
    List<String> agentStrData = new ArrayList<String>();
    //for convinence
    int[] userIds;
    //stores the flow patterns
    List<WebTxnFlow> flows;
    Map<TxnType, WebTxn> txnOpsMap = new HashMap<TxnType, WebTxn>();
    //String [] items ;
    TIntArrayList items;
    /**
     * ===============DATA======================*
     */
    String[] cards = new String[]{
            "VISA",
            "AMEX",
            "DEBT",
            "MASTER"
    };
    int[] responseCodes = new int[]{
            100,
            200, //specifically set to 1 for zipf dist
            300,
            301,
            303,
            400,
            401,
            500
    };
    /**
     * ===============DATA======================*
     */
    ZipfDistribution responseCodeDist = new ZipfDistribution(responseCodes.length, 5);
    //random gen for all datasets
    Random numberGen = new Random();

    public Runner(String configDir) {
        this.configDir = configDir;
    }

    public Runner(String configDir, final String outFile) {
        this.configDir = configDir;
        this.outFile = outFile;
    }

    public static void main(String[] args) {
        final String configDir = "/home/sumanthn/datasets/txn";

        final String dumpFile = "/home/sumanthn/datasets/txn/TxnRawData.csv";
        //Runner runner = new Runner(configDir);
        Runner runner = new Runner(configDir, dumpFile);
        //runner.loadDatasets();
        runner.initData();

        final String startTime = "2014-01-01 00:00:00";
        final String endTs = "2014-01-02 00:01:00";

        int maxTps = 500;

        runner.generateFlow(maxTps, startTime, endTs);
    }

    void generateIpAddressBag(final int count) {
        Random random = new Random();
        random.setSeed(500);
        int ipCount = 0;
        while (ipCount < count) {
            StringBuilder ipAddr = new StringBuilder();
            for (int i = 0; i < 3; i++) {
                ipAddr.append(random.nextInt(255)).append(".");
            }
            ipAddr.append(random.nextInt(255));
            if (ipCount > 50) {
                serverIpAddressBag.add(ipAddr.toString());
            } else {
                clientIpAddressBag.add(ipAddr.toString());
            }
            ipCount++;
        }
    }

    void loadAgentStrData() {
        //simply load the known strs
        //minified version in use not the entire string
        agentStrData.add("Mozilla Gecko");
        agentStrData.add("Chrome Webkit");
        agentStrData.add("Safari");
        agentStrData.add("MSIE");
        agentStrData.add("Andriod Webkit");
        agentStrData.add("Opera");
        agentStrData.add("HTC_Touch");
        agentStrData.add("BlackBerry");
        agentStrData.add("GoogleBot");
    }

    int getResponsecode() {
        int num = responseCodeDist.sample();
        if (num < responseCodes.length)
            return responseCodes[num];
        return responseCodes[1];
    }

    void initData() {
        final int MAX_IPADDRS = 10000;
        generateIpAddressBag(MAX_IPADDRS);
        loadAgentStrData();
        loadDatasets();
    }

    void loadDatasets() {
        final String fileName = configDir + "/Txndetails.csv";
        final String itemsFile = configDir + "/Itemnames.csv";
        final String userIdFile = configDir + "/Userdata.csv";
        final String flowPatternFile = configDir + "/Txnflow.csv";
        List<WebTxn> allTxn = DataSetLoader.readTxns(fileName);
        System.out.println(allTxn.size());

        for (WebTxn txn : allTxn)
            txnOpsMap.put(TxnType.valueOf(txn.getOpcode()), txn);

        List<ShoppingItem> allItems = DataSetLoader.readItemsData(itemsFile);
        System.out.println("Shopping items " + allItems.size());

        //items  = new String[allItems.size()];
        items = new TIntArrayList();
        int sIdx = 0;
        for (ShoppingItem item : allItems) {
            //items[sIdx++] = item.getName();
            items.add(item.getId());
        }

        Map<Integer, String> userIdMap = DataSetLoader.readUserIds(userIdFile);
        System.out.println("User ids " + userIdMap.size());
        userIds = new int[userIdMap.size()];
        //userIdMap.keySet().toArray(userIds);
        int idItr = 0;
        for (Integer id : userIdMap.keySet()) {
            userIds[idItr++] = id;
        }
        flows = DataSetLoader.loadFlowPattern(flowPatternFile);
        System.out.println("Flow data  " + flows.size());
    }

    List<WebTxnData> generateDataBatch(int tps, final String startTime, final String endTime) {

        List<WebTxnData> dataBatch = new ArrayList<WebTxnData>();

        final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        //adjust so that it doesn't overshoot
        int adjustTps = tps;
        if (tps >= 10)
            adjustTps = tps - 2;

        long tpsGlobalCount = 0;

        DateTime cur = DateTime.parse(startTime, DATE_TIME_FORMATTER);
        final DateTime endTs = DateTime.parse(endTime, DATE_TIME_FORMATTER);

        int tpsCount = 0;

        while (tpsCount < adjustTps) {

            final long startMs = cur.getMillis();

            //generate a flow data

            int flowId = numberGen.nextInt(flows.size());
            List<WebTxnData> txns = generateFlow(flowId, startMs);
            // allData.addAll(txns);

            dataBatch.addAll(txns);

            // System.out.println("generated total tps " + tpsCount);
            tpsCount = tpsCount + txns.size();
        }

        return dataBatch;
    }

    void generateFlow(int tps, final String startTime, final String endTime) {

        BufferedWriter writer = null;
        try {
            if (outFile != null) {
                writer = new BufferedWriter(new FileWriter(outFile));
            }
        } catch (IOException e) {
        }

        final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        //adjust so that it doesn't overshoot
        int adjustTps = tps;
        if (tps >= 10)
            adjustTps = tps - 2;

        long tpsGlobalCount = 0;

        DateTime cur = DateTime.parse(startTime, DATE_TIME_FORMATTER);
        final DateTime endTs = DateTime.parse(endTime, DATE_TIME_FORMATTER);
        while (cur.isBefore(endTs)) {

            int tpsCount = 0;

            while (tpsCount < adjustTps) {

                final long startMs = cur.getMillis();

                //generate a flow data

                int flowId = numberGen.nextInt(flows.size());
                List<WebTxnData> txns = generateFlow(flowId, startMs);
                // allData.addAll(txns);

                if (writer != null) {
                    for (WebTxnData txn : txns) {
                        try {
                            writer.write(txn.toCsv());
                            writer.newLine();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }

                // System.out.println("generated total tps " + tpsCount);
                tpsCount = tpsCount + txns.size();
            }

            tpsGlobalCount = tpsGlobalCount + tpsCount;
            if (tpsGlobalCount % 1000 == 0)
                System.out.println("Generated " + tpsGlobalCount);
            cur = cur.plusSeconds(1);
        }

        System.out.println("Total objects " + allData.size());

        /*System.out.println("Launching serialize - deserialze using kryo");
        KryoSerializer.serializeDeserialize(allData);

        System.out.println("Launching serialize - deserialze using jackson");
        JacksonMapper.serializeDeserialize(allData);*/

        if (writer != null)
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    List<WebTxnData> generateFlow(final int flowPatternId, final long startMs) {

        List<WebTxnData> txnDataBag = new ArrayList<WebTxnData>();
        int maxItems = 40;

        long curTs = startMs;
        //just store the user id if required
        int userId = userIds[numberGen.nextInt(userIds.length)];

        int maxItemsForTxn = numberGen.nextInt(maxItems);

        final String SESSION_ID_ATTR = "SessionId";
        //maintain session id across statefull transaction
        String sessionId = UUID.randomUUID().toString();

        if (maxItemsForTxn < 5)
            maxItemsForTxn = 5;

        //keep track of signed in user
        boolean userIdSet = false;

        TIntArrayList itemsInSession = makeItemSet(maxItemsForTxn);

        //String itemsStr = makeItemsStr(maxItemsForTxn);

        String agentStr = agentStrData.get(numberGen.nextInt(agentStrData.size()));

        WebTxnFlow flow = flows.get(flowPatternId);

        for (TxnType txnType : flow.getTxnInFlow()) {

            WebTxn txn = txnOpsMap.get(txnType);

            WebTxnData txnData = TxnInstanceBuilder.buildTxnInstance(txn);
            double bytes = txn.getBytes() * numberGen.nextGaussian();
            txnData.setBytesTransferred(Math.abs(Double.valueOf(bytes).intValue()));
            txnData.setTimestamp(curTs);
            txnData.setHttpCode(getResponsecode());

            int responseTime = numberGen.nextInt(450);
            if (responseTime == 0)
                responseTime = 10;
            txnData.setResponseTime(responseTime);

            //in ideal world we would add up this to the next time stamp
            curTs = curTs + (responseTime) / 2;

            //set server and client ips
            txnData.setServerIp(serverIpAddressBag.get((numberGen.nextInt(serverIpAddressBag.size()))));
            txnData.setClientIp(clientIpAddressBag.get((numberGen.nextInt(clientIpAddressBag.size()))));
            txnData.setClientAgent(agentStr);

            //generating strings to stress the system
            if (txn.getCustomAttributes().size() > 0) {
                for (String customAttr : txn.getCustomAttributes()) {
                    boolean userIdSetInLoop = false;

                    if (customAttr.equalsIgnoreCase("UserId")) {

                        userIdSet = true;
                        userIdSetInLoop = true;
                        txnData.addCustomData(customAttr, Integer.valueOf(userId).toString());
                        txnData.setUserId(userId);
                        txnData.addCustomData(SESSION_ID_ATTR, sessionId);
                    } else if (customAttr.contains("Items")) {

                        txnData.addCustomData(customAttr, makeItemStr(itemsInSession));
                        //remake items in session
                        itemsInSession = getSubsetItems(itemsInSession);
                    } else if (customAttr.equals("Cardtype")) {

                        //make card and add it
                        txnData.addCustomData(customAttr, cards[numberGen.nextInt(cards.length)]);
                    }

                    //additionally
                    if (!(userIdSet && userIdSetInLoop)) {
                        final String userIdAttr = "UserId";
                        txnData.addCustomData(userIdAttr, Integer.valueOf(userId).toString());
                        txnData.setUserId(userId);
                        txnData.addCustomData(SESSION_ID_ATTR, sessionId);
                    }
                }
            } else {
                //this would force the user id being set
                if (userIdSet) {
                    final String userIdAttr = "UserId";
                    txnData.addCustomData(userIdAttr, Integer.valueOf(userId).toString());
                    txnData.setUserId(userId);
                    txnData.addCustomData(SESSION_ID_ATTR, sessionId);
                }
            }

            //System.out.println(txnData.toString());
            txnDataBag.add(txnData);
        }//end flow

        return txnDataBag;
    }

    String makeItemStr(TIntArrayList itemsInSession) {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < itemsInSession.size() - 2; i++) {
            sb.append(itemsInSession.get(i)).append("|");
        }

        sb.append(itemsInSession.get(itemsInSession.size() - 1));
        return sb.toString();
    }

    TIntArrayList getSubsetItems(TIntArrayList itemsInSession) {
        //can also just return index to be used
        //keep flipping the coin
        if (itemsInSession.size() == 1)
            return itemsInSession;

        itemsInSession.shuffle(shufflerR);
        itemsInSession.remove(itemsInSession.size() - 1);

        return itemsInSession;
    }

    TIntArrayList makeItemSet(int maxItemsForTxn) {
        TIntArrayList itemsInSession = new TIntArrayList(maxItemsForTxn);

        for (int i = 0; i < maxItemsForTxn; i++) {

            itemsInSession.add(items.get(numberGen.nextInt(items.size())));
        }
        return itemsInSession;
    }
}
