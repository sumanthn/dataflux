package dataflux.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Base Kafka producer
 * Created by sumanthn
 */
public class KafkaProducer {

    private Producer<byte[], byte[]> producer;
    private ProducerConfig config;

    public KafkaProducer() {
        Properties props = null;
        try {

            //read in default configuration
            InputStream in = getClass().getResourceAsStream(MsgConstants.BROKER_PROPS_FILE);

            props = new Properties();

            props.load(in);

            config = new ProducerConfig(props);
            producer = new Producer<>(config);

            //in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public KafkaProducer(final String fileName) {
        Properties props = null;
        try {

            FileReader reader = new FileReader(fileName);

            props = new Properties();

            props.load(reader);

            config = new ProducerConfig(props);
            producer = new Producer<>(config);

            //in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        producer.close();
    }

    public void sendMsg(final KeyedMessage<byte[], byte[]> msg) {
        producer.send(msg);
    }
}
