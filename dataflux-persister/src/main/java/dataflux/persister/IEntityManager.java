package dataflux.persister;

import dataflux.common.type.WebTxnData;

import java.io.IOException;
import java.util.List;

/**
 * Created by nageswara.v on 4/2/2014.
 */
public interface IEntityManager {
    /**
     * Synchronous batch write operation
     *
     * @return
     */
    public List<WebTxnData> fetch(String tblName, int txnUId, long startDate,
                                  long stopDate);
    /**
     * Synchronous batch read operation
     *
     * @return
     */
    public int persist(String tblName, List<WebTxnData> webTxnBatch);

    public int persist(String appName, WebTxnData webTxnData);

    public void close() throws IOException;

    public boolean isOpen();
}
