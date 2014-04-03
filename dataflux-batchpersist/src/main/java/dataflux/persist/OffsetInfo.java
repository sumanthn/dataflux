package dataflux.persist;

/**
 * Maintains the offset information
 * Created by sumanthn
 */
public class OffsetInfo {

    private final String topicName;
    private final int partition;
    private final long offset;

    public OffsetInfo(String topicName, int partition, long offset) {
        this.topicName = topicName;
        this.partition = partition;
        this.offset = offset;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "OffsetInfo{" +
                "topicName='" + topicName + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                '}';
    }
}
