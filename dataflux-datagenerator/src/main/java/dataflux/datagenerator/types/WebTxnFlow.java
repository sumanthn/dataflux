package dataflux.datagenerator.types;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Represents a flow / conversation in a typical shopping cart application
 * A Flow consists of a predefined set of user transaction
 * Created by sumanthn
 */
public class WebTxnFlow {

    private int id;
    private ImmutableList<TxnType> txnInFlow ;//= new ArrayList<TxnType>();

    public WebTxnFlow(int id) {
        this.id = id;
    }

    public WebTxnFlow(int id, List<TxnType> txnInFlow) {
        this.id = id;
        this.txnInFlow = ImmutableList.copyOf(txnInFlow);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public List<TxnType> getTxnInFlow() {
        return txnInFlow;
    }


}
