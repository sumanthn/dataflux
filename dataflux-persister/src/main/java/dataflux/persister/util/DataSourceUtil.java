package dataflux.persister.util;

import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourceUtil {
	
	private static final BasicDataSource DATASOURCE;
	private static Logger log = LoggerFactory.getLogger("BasicDataSource");
	static {
		try {
			DATASOURCE = new BasicDataSource();
			DATASOURCE.setDriverClassName("com.mysql.jdbc.Driver");
			DATASOURCE.setUrl("jdbc:mysql://10.1.1.169:3306/Appnomic");
			DATASOURCE.setUsername("root");
			DATASOURCE.setPassword("");
			DATASOURCE.setInitialSize(5);
			DATASOURCE.setMaxActive(5);
			DATASOURCE.setMaxWait(30000);
			log.debug("Created BasicDataSource");
		} catch (Throwable ex) {
			throw new ExceptionInInitializerError(ex);
		}
	}

	public synchronized static BasicDataSource getDataSource() {
		return DATASOURCE;
	}
}