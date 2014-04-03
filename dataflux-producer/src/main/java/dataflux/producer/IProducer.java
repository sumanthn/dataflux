package dataflux.producer;
import kafka.producer.KeyedMessage;

import java.util.List;

/**
 * Lig
 * Created by sumanthn on 23/3/14.
 */
public interface IProducer {


    /** sends out the filled in message  to kafka */
    public void dispatchMessage(KeyedMessage<byte[], byte[]> msg);

    public void shutdown();

    public void dispatchMessage(final List<KeyedMessage<byte[], byte[]>> msgBatch );


}
