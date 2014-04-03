package dataflux.persist;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Abstract consumer
 * Created by sumanthn
 */
public abstract class KafkaConsumer {

    protected ConsumerConnector consumer;
    protected String topic;
    protected ConsumerConfig config;

    public KafkaConsumer(final String topic) {
        this.topic = topic;
        Properties props = null;
        try {

            //read in default configuration
            InputStream in = getClass().getResourceAsStream("/consumer.properties");

            props = new Properties();

            props.load(in);

            config = new ConsumerConfig(props);

            //in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public KafkaConsumer(final String topic, final String configFile) {
        this.topic = topic;
        Properties props = null;
        try {

            FileReader reader = new FileReader(configFile);

            props = new Properties();

            props.load(reader);

            config = new ConsumerConfig(props);

            //in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void initConsumer() {
        consumer = Consumer.createJavaConsumerConnector(config);
    }

    protected abstract void consumeMessages();
}
