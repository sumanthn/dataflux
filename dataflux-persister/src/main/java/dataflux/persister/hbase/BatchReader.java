package dataflux.persister.hbase;

import dataflux.common.type.WebTxnData;
import dataflux.persister.schema.TColumnFamily;
import dataflux.persister.schema.TWebTxnData;
import dataflux.persister.util.PersistenceUtil;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

public class BatchReader implements Callable<List<WebTxnData>> {

    String tblName = null;

    long startDate = 0;
    long stopDate = 0;
    int txnUId = 0;

    public BatchReader(String tblName, int txnUId, long startDate,
                       long stopDate) {
        this.tblName = tblName;
        this.startDate = startDate;
        this.stopDate = stopDate;
        this.txnUId = txnUId;
    }

    public static void main(String args[]) {
        try {
            BatchReader abr = new BatchReader("TranLoad1", 9895, (new Date("2014-01-23 11:00:00")).getTime(),
                    (new Date("2014-01-23 13:00:00")).getTime());
            List<WebTxnData> ab = abr.call();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public List<WebTxnData> call() throws Exception {

        List<WebTxnData> txnAudit = new ArrayList<WebTxnData>();
        double timeTaken = 0;

        HBaseResourceFactory.HTablePool htp = HBaseResourceFactory.getHBaseTablePool();
        try (HTableInterface table = htp.getTable(tblName);) {
            Scan scan = new Scan();
            String startRow = txnUId + PersistenceUtil.reverseTimeStamp(startDate);
            String stopRow = txnUId + PersistenceUtil.reverseTimeStamp(stopDate);
            System.out.println("startRow : " + startRow + ", stopRow : " + stopRow);
            scan.setStartRow(startRow.getBytes());
            scan.setStopRow(stopRow.getBytes());
            scan.addFamily(TColumnFamily.TXNAUD_CF_BA);
            scan.setBatch(10000);
            String strVal = null;
            try (ResultScanner rs = table.getScanner(scan);) {
                double startTime = System.currentTimeMillis();
                for (Result rslt : rs) {
                    WebTxnData txnData = new WebTxnData();
                    strVal = new String(rslt.getValue(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_ID_BA));
                    txnData.setTxnId(PersistenceUtil.getIntValue(strVal, 0));
                    strVal = new String(rslt.getValue(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_URL_BA));
                    txnData.setUrl(strVal);
                    strVal = new String(rslt.getValue(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_TIMESTAMP_BA));
                    txnData.setTimestamp(PersistenceUtil.getLongValue(strVal, 0));
                    strVal = new String(rslt.getValue(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_OPERATION_BA));
                    txnData.setOperation(strVal);
                    strVal = new String(rslt.getValue(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_USERID_BA));
                    txnData.setUserId(PersistenceUtil.getIntValue(strVal, 0));
                    strVal = new String(rslt.getValue(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_HTTPCODE_BA));
                    txnData.setHttpCode(PersistenceUtil.getIntValue(strVal, 0));
                    strVal = new String(rslt.getValue(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_RTIME_BA));
                    txnData.setResponseTime(PersistenceUtil.getIntValue(strVal, 0));
                    strVal = new String(rslt.getValue(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_BYTES_BA));
                    txnData.setBytesTransferred(PersistenceUtil.getIntValue(strVal, 0));
                    strVal = new String(rslt.getValue(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_SERVERIP_BA));
                    txnData.setServerIp(strVal);
                    strVal = new String(rslt.getValue(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_CLIENTIP_BA));
                    txnData.setClientIp(strVal);
                    strVal = new String(rslt.getValue(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_CLIENTAGENT_BA));
                    txnData.setClientAgent(strVal);
                }
                timeTaken = System.currentTimeMillis() - startTime;
                System.out.println("No. of records : " + txnAudit.size() + " in : " + timeTaken + " ms @ " + (txnAudit.size() / timeTaken) * 1000 + " rps");
            }
            //System.out.println("After close");
        } catch (Exception exp) {
            System.out.println("No. of records : " + txnAudit.size() + " time : " + timeTaken + " ms");
            exp.printStackTrace();
        }
        return txnAudit;
    }
}
