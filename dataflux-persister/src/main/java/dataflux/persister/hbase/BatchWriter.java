package dataflux.persister.hbase;

import dataflux.common.type.WebTxnData;
import dataflux.persister.schema.TColumnFamily;
import dataflux.persister.schema.TWebTxnData;
import dataflux.persister.util.PersistenceUtil;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class BatchWriter implements Callable<Integer> {

	private List<WebTxnData> batch = null;
    private String tblName;

	public BatchWriter(String tblName, List<WebTxnData> batch) {
		this.batch = batch;
        this.tblName = tblName;
	}


	@Override
	public Integer call() throws Exception {
		int written = 0;
		double timeTaken = 0;
        try {
        	HBaseResourceFactory.HAdmin hAdmin = HBaseResourceFactory.getHAdmin();
        	hAdmin.checkHBaseAvailable();
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}

        // <table name> --> A1TxnAudit:<AppName>
		/*try ( //HTable table = new HTable(hbConfig, Constants.TXNAUD_NS+":"+auditBatch.getAppName());
				HTable table = new HTable(hbConfig, auditBatch.getAppName()); ) {*/
        HBaseResourceFactory.HTablePool htp = HBaseResourceFactory.getHBaseTablePool();

        try (HTableInterface table = htp.getTable(tblName);) {
			List<Put> puts = getPuts(batch);
			written = puts.size();
			table.setAutoFlushTo(false);
			double startTime = System.currentTimeMillis();
			table.put(puts);
			timeTaken =  System.currentTimeMillis() - startTime;
			table.flushCommits();
			System.out.println("No. of records : " + written +" in : " + timeTaken +" ms @ " + (written/timeTaken) * 1000 +" rps");
			//System.out.println("After close");
		} catch (Exception exp) {
			System.out.println("No. of records : " + written +" time : " + timeTaken +" ms");
			exp.printStackTrace();
		}
		return written;
	}
	
	private List<Put> getPuts(List<WebTxnData> auditBatch) {
		String rowKey = null;
		ArrayList<Put> puts = new ArrayList<Put>();

		for (WebTxnData txnAud : auditBatch) {

			// rowkey --> <txn uid><reversetimestamp of txn endtime upto secs>
			try {
				rowKey = txnAud.getTxnId()+PersistenceUtil.reverseTimeStamp(txnAud.getTimestamp());
			} catch (NumberFormatException | ClassCastException exp) {
				//System.out.println("Time stamp : " + timeStamp);
				continue;
			}

			Put put = new Put(Bytes.toBytes(rowKey));

			// cf:key value --> Audit:TxnInstanceId <TxnInstanceId value>
			put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_ID_BA,
					Bytes.toBytes(txnAud.getTxnId()));
            put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_URL_BA,
                    Bytes.toBytes(txnAud.getUrl()));
            put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_TIMESTAMP_BA,
                    Bytes.toBytes(txnAud.getTimestamp()));
            put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_OPERATION_BA,
                    Bytes.toBytes(txnAud.getOperation()));
            put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_HTTPCODE_BA,
                    Bytes.toBytes(txnAud.getHttpCode()));
            put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_RTIME_BA,
                    Bytes.toBytes(txnAud.getResponseTime()));
            put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_BYTES_BA,
                    Bytes.toBytes(txnAud.getBytesTransferred()));
            put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_SERVERIP_BA,
                    Bytes.toBytes(txnAud.getServerIp()));
            put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_CLIENTIP_BA,
                    Bytes.toBytes(txnAud.getClientIp()));
            put.add(TColumnFamily.TXNAUD_CF_BA, TWebTxnData.TXN_CLIENTAGENT_BA,
                    Bytes.toBytes(txnAud.getClientAgent()));
			puts.add(put);
		}
		return puts;
	}
}
