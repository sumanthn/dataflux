package dataflux.datagenerator.util;

import dataflux.common.type.WebTxnData;
import dataflux.datagenerator.types.WebTxn;

/**
 * Utility to build a transaction instance
 * Created by sumanthn
 */
public class TxnInstanceBuilder {

    private TxnInstanceBuilder() {
    }

    public static WebTxnData buildTxnInstance(final WebTxn txn) {

        WebTxnData txnData = new WebTxnData();
        txnData.setTxnId(txn.getId());
        txnData.setOperation(txn.getOpcode());
        txnData.setUrl(txn.getUrl());

        return txnData;
    }
}
