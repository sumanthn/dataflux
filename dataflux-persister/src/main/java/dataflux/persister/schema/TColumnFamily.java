package dataflux.persister.schema;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by nageswara.v on 3/31/2014.
 */
public class TColumnFamily {
    public static final String TXNAUD_CF = "Audit";

    public static final byte[] TXNAUD_CF_BA = Bytes.toBytes(TXNAUD_CF);
}
