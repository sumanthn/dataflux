package dataflux.persister.hbase;

import dataflux.persister.hbase.HBaseResourceFactory.HAdmin;
import dataflux.persister.schema.TColumnFamily;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

/**
 * Class to generate Schema in HBase
 * 
 * @author nageswara.v
 * 
 */
public class HBaseSchemaManager implements Closeable {

	public void createAppTables(Set<String> apps) throws IOException {
		HAdmin htp = HBaseResourceFactory.getHAdmin();
		for (String appName : apps) {
			htp.createTables(getTableDescriptor(appName));
		}
	}

	private HTableDescriptor getTableDescriptor(String appName) {
		// HTableDescriptor tbl = new
		// HTableDescriptor(TableName.valueOf(Constants.TXNAUD_NS, appName));
		HTableDescriptor tbl = new HTableDescriptor(TableName.valueOf(appName));
		HColumnDescriptor clm = new HColumnDescriptor(TColumnFamily.TXNAUD_CF);
		/*
		 * clm.setMaxVersions(3); // default is 3,
		 * http://hbase.apache.org/book/schema.versions.html
		 * clm.setCompressionType(null); clm.setInMemory(false);
		 * clm.setBlockCacheEnabled(true); clm.setBlocksize(64);
		 * clm.setTimeToLive(Integer.MAX_VALUE); // Forever
		 * clm.setBloomFilterType(bt); // bloom filter clm.setScope(0); //
		 * default
		 */
		tbl.addFamily(clm);
		return tbl;
	}
	
	public void deleteTable(String tableName) throws IOException {
		HAdmin hAdmin = HBaseResourceFactory.getHAdmin();
		hAdmin.deleteTable(tableName);
	}

	public void deleteTables(Set<String> tbls) throws IOException {
		HAdmin hAdmin = HBaseResourceFactory.getHAdmin();
		for (String tableName : tbls) {
			hAdmin.deleteTable(tableName);
		}
	}

	@Override
	public void close() throws IOException {
		HAdmin hAdmin = HBaseResourceFactory.getHAdmin();
		hAdmin.close();
	}
}
