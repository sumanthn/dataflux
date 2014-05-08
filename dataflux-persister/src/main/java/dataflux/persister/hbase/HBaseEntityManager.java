package dataflux.persister.hbase;

import dataflux.common.type.WebTxnData;
import dataflux.persister.IEntityManager;
import dataflux.persister.schema.TColumnFamily;
import dataflux.persister.schema.TWebTxnData;
import dataflux.persister.util.PersistenceUtil;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBaseEntityManager implements IEntityManager, Closeable {

    private static Logger log = LoggerFactory.getLogger("HBaseTableFactory");

    public int persist(String tblName, List<WebTxnData> webTxnBatch) {
        int written = 0;
        BatchWriter writeJob = new BatchWriter(tblName, webTxnBatch);
        try {
            written = writeJob.call();
        } catch (Exception exp) {
            log.error(exp.getMessage());
        }
        return written;
    }

    @Override
    public int persist(String tblName, WebTxnData webTxnData) {
        HBaseResourceFactory.HTablePool htp = HBaseResourceFactory.getHBaseTablePool();
        HTableInterface hti = null;
        try {
            hti = htp.getTable(tblName);
            // rowkey --> <txn uid><reversetimestamp of txn endtime upto secs>
            String rowKey = null;
            try {
                rowKey = webTxnData.getTxnId() + PersistenceUtil.reverseTimeStamp(webTxnData.getTimestamp());
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException | ClassCastException exp) {
                log.error("Invalid time stamp : " + webTxnData.getTimestamp());
                return 0;
            }
            Put put = new Put(Bytes.toBytes(rowKey));
            // cf:key value --> Audit:TxnInstanceId <TxnInstanceId value>
            put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_ID_BA,
                    Bytes.toBytes(webTxnData.getTxnId()));
            put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_URL_BA,
                    Bytes.toBytes(webTxnData.getUrl()));
            put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_TIMESTAMP_BA,
                    Bytes.toBytes(webTxnData.getTimestamp()));
            put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_OPERATION_BA,
                    Bytes.toBytes(webTxnData.getOperation()));
            put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_HTTPCODE_BA,
                    Bytes.toBytes(webTxnData.getHttpCode()));
            put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_RTIME_BA,
                    Bytes.toBytes(webTxnData.getResponseTime()));
            put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_BYTES_BA,
                    Bytes.toBytes(webTxnData.getBytesTransferred()));
            put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_SERVERIP_BA,
                    Bytes.toBytes(webTxnData.getServerIp()));
            put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_CLIENTIP_BA,
                    Bytes.toBytes(webTxnData.getClientIp()));
            put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_CLIENTAGENT_BA,
                    Bytes.toBytes(webTxnData.getClientAgent()));
            hti.put(put);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return 0;
    }

    public List<WebTxnData> fetch(String tblName, int txnUId, long startDate,
                             long stopDate) {
        List<WebTxnData> result = new ArrayList<>();
        BatchReader readJob = new BatchReader(tblName, txnUId, startDate, startDate);
        try {
            result = readJob.call();
        } catch (Exception exp) {
            log.error(exp.getMessage());
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        HBaseResourceFactory.HTablePool htp = HBaseResourceFactory.getHBaseTablePool();
        htp.close();
    }

    @Override
    public boolean isOpen() {
        try {
            HBaseResourceFactory.HAdmin hAdmin = HBaseResourceFactory.getHAdmin();
            hAdmin.checkHBaseAvailable();
        } catch (Exception exp) {
            log.error(exp.getMessage());
            return false;
        }
        return true;
    }

    public static void main(String[] args) {
        List<WebTxnData> txnData = new ArrayList<>();
        for(int indx = 1; indx < 1000; indx++) {
            WebTxnData webTxn = new WebTxnData();
            webTxn.setTxnId(1);
            webTxn.setClientAgent("Mozilla");
            webTxn.setClientIp("10.1.1.231");
            webTxn.setServerIp("10.1.1.155");
            webTxn.setBytesTransferred(10000);
            webTxn.setHttpCode(200);

            Map<String, String> cProps = new HashMap<String, String>();
            cProps.put("prop1", "value1");
            cProps.put("prop2", "value2");

            webTxn.setCustomData(cProps);
            webTxn.setOperation("addItem.do");
            webTxn.setResponseTime(2000);
            webTxn.setTimestamp(System.currentTimeMillis());
            webTxn.setUserId(13213);
            webTxn.setUrl("/site/addItem.do");
            txnData.add(webTxn);
        }

        IEntityManager entityManager = new HBaseEntityManager();
        entityManager.persist("TranLoad1", txnData);
    }
}
