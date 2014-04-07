package dataflux.persister.schema;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by nageswara.v on 3/31/2014.
 */
public class TWebTxnData {

    public static String TXN_ID = "txnId";
    public static String TXN_URL = "url";
    public static String TXN_TIMESTAMP = "timestamp";
    public static String TXN_OPERATION = "operation";
    public static String TXN_USERID = "userId";
    public static String TXN_HTTPCODE = "httpCode";
    public static String TXN_RTIME = "responseTime";
    public static String TXN_BYTES = "bytesTransferred";
    public static String TXN_SERVERIP = "serverIp";
    public static String TXN_CLIENTIP = "clientIp";
    public static String TXN_CLIENTAGENT = "clientAgent";

    public static byte[] TXN_ID_BA = Bytes.toBytes(TXN_ID);
    public static byte[] TXN_URL_BA = Bytes.toBytes(TXN_URL);
    public static byte[] TXN_TIMESTAMP_BA = Bytes.toBytes(TXN_TIMESTAMP);
    public static byte[] TXN_OPERATION_BA = Bytes.toBytes(TXN_OPERATION);
    public static byte[] TXN_USERID_BA = Bytes.toBytes(TXN_USERID);
    public static byte[] TXN_HTTPCODE_BA = Bytes.toBytes(TXN_HTTPCODE);
    public static byte[] TXN_RTIME_BA = Bytes.toBytes(TXN_RTIME);
    public static byte[] TXN_BYTES_BA = Bytes.toBytes(TXN_BYTES);
    public static byte[] TXN_SERVERIP_BA = Bytes.toBytes(TXN_SERVERIP);
    public static byte[] TXN_CLIENTIP_BA = Bytes.toBytes(TXN_CLIENTIP);
    public static byte[] TXN_CLIENTAGENT_BA = Bytes.toBytes(TXN_CLIENTAGENT);

    private static Map<String, byte[]> customData = new HashMap<>();

    public static byte[] getBA(String colqual) {
        byte[] ba = customData.get(colqual);
        if(ba == null) {
            ba = Bytes.toBytes(colqual);
            customData.put(colqual, ba);
        }
        return ba;
    }
}