package dataflux.common.type;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Transaction data type
 * Created by sumanthn
 */
public class WebTxnData implements Serializable {

    public static final String FLD_DELIM = ",";

    private int txnId;//unique id per transaction
    private String url; //the Url in use
    private long timestamp; //start of txn
    private String operation; //operations defined
    private int userId; // user id , this can be null for some transactions
    private int httpCode;

    private int responseTime;//ms
    private long bytesTransferred;

    private String serverIp;
    private String clientIp;
    private String clientAgent;

    /**
     * custom attributes used to push in any additional data for transaction
     */
    //TODO: to prevent anything that can be pushed in as data , ensure that there is well defined fields(schema) for every txn
    //schema comes from configuration

    private Map<String, String> customData = new HashMap<String, String>();

    public String toCsv() {

        StringBuilder sb = new StringBuilder();
        sb.append(txnId).append(FLD_DELIM)
                .append(url).append(FLD_DELIM)
                .append(timestamp).append(FLD_DELIM)
                .append(operation).append(FLD_DELIM)
                .append(userId).append(FLD_DELIM)
                .append(httpCode).append(FLD_DELIM)
                .append(responseTime).append(FLD_DELIM)
                .append(bytesTransferred).append(FLD_DELIM)
                .append(serverIp).append(FLD_DELIM)
                .append(clientIp).append(FLD_DELIM)
                .append(clientAgent);

        if (customData.size() > 0) {
            sb.append(FLD_DELIM);
            sb.append("\"");
            for (String customAttr : customData.keySet()) {
                sb.append(customAttr).append("=").append(customData.get(customAttr)).append(FLD_DELIM);
            }

            sb.append("\"");
        }

        return sb.toString();
    }

    public WebTxnData() {

    }

    public int getTxnId() {
        return txnId;
    }

    public void setTxnId(int txnId) {
        this.txnId = txnId;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getHttpCode() {
        return httpCode;
    }

    public void setHttpCode(int httpCode) {
        this.httpCode = httpCode;
    }

    public int getResponseTime() {
        return responseTime;
    }

    public void setResponseTime(int responseTime) {
        this.responseTime = responseTime;
    }

    public long getBytesTransferred() {
        return bytesTransferred;
    }

    public void setBytesTransferred(long bytesTransferred) {
        this.bytesTransferred = bytesTransferred;
    }

    public String getServerIp() {
        return serverIp;
    }

    public void setServerIp(String serverIp) {
        this.serverIp = serverIp;
    }

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    public String getClientAgent() {
        return clientAgent;
    }

    public void setClientAgent(String clientAgent) {
        this.clientAgent = clientAgent;
    }

    public Map<String, String> getCustomData() {
        return customData;
    }

    public void setCustomData(Map<String, String> customData) {
        this.customData = customData;
    }

    public void addCustomData(final String attr, final String val) {
        customData.put(attr, val);
    }
}
