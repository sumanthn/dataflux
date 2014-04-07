package dataflux.persister.hbase;

import com.google.protobuf.ServiceException;
import dataflux.persister.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class HBaseResourceFactory {

	private static Logger log = LoggerFactory.getLogger("HBaseTableFactory");

	public static HTablePool getHBaseTablePool() {
		return HTablePool.tblPool;
	}

	public static HAdmin getHAdmin() {
		return HAdmin.hAdmin;
	}

    private static Properties props = new Properties();

    static {
        try {
            InputStream is = HBaseResourceFactory.class.getResourceAsStream("/conf/config.properties");
            props.load(is);
        } catch (Exception exp) {
            log.error("Failed to load configuration : " + exp.getMessage());
        }
    }

	private static class HConfig {

		private static HConfig hConfig = new HConfig();
		private Configuration config = null;

		private HConfig() {
			initHConfig();
		}

		private void initHConfig() {
			config = HBaseConfiguration.create();
			config.clear();
			config.set(Constants.HBASE_QUORUM_IP, props.getProperty(Constants.HBASE_QUORUM_IP));
            config.set(Constants.HBASE_QUORUM_PORT, props.getProperty(Constants.HBASE_QUORUM_PORT));
		}
	}

	public static class HAdmin implements Closeable {

		private static HAdmin hAdmin = new HAdmin();
		private HBaseAdmin admin = null;

		private HAdmin() {
			initHAdmin();
		}

		private void initHAdmin() {
			try {
				admin = new HBaseAdmin(HConfig.hConfig.config);
			} catch (IOException ioExp) {
				// TODO Auto-generated catch block
				ioExp.printStackTrace();
			}
		}

		public void checkHBaseAvailable() throws ServiceException,
				MasterNotRunningException, ZooKeeperConnectionException,
				IOException {
			admin.checkHBaseAvailable(HConfig.hConfig.config);
		}

		void createTables(HTableDescriptor appDesc) throws IOException {
			admin.createTable(appDesc);
		}

		void deleteTable(String tableName) throws IOException {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}

		@Override
		public void close() throws IOException {
			admin.close();
		}
	}

	public static class HTablePool implements Closeable {

		private static HTablePool tblPool = new HTablePool();

		private HConnection conn = null;

		private HTablePool() {
			initHConnection();
		}

		private void initHConnection() {
			try {
				conn = HConnectionManager
						.createConnection(HConfig.hConfig.config);
			} catch (IOException ioExp) {
				// TODO Auto-generated catch block
				ioExp.printStackTrace();
			}
		}

		HTableInterface getTable(String tableName) throws IOException {
			HTableInterface table = conn.getTable(tableName);
			return table;
		}

		public void close() throws IOException {
			conn.close();
		}
	}
}
