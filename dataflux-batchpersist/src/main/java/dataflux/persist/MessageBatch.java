package dataflux.persist;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple batch of messages with offset info
 * Created by sumanthn
 */
public class MessageBatch {

    private  OffsetInfo offsetInfo;



    private List<byte[]> dataBatch = new ArrayList<>();

    public OffsetInfo getOffsetInfo() {
        return offsetInfo;
    }

    public List<byte[]> getDataBatch() {
        return dataBatch;
    }

    public void setDataBatch(List<byte[]> dataBatch) {
        this.dataBatch = dataBatch;
    }

    public void setOffsetInfo(OffsetInfo offsetInfo) {
        this.offsetInfo = offsetInfo;
    }

    @Override
    public String toString() {
        return "MessageBatch{" +
                "offsetInfo=" + offsetInfo + "  Size " + dataBatch.size() +
                '}' ;
    }
}
