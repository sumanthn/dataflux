package dataflux.producer;

import org.apache.commons.pool2.PoolUtils;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.log4j.Logger;

/**
 * A producer pool
 * Created by sumanthn
 */
//TODO: validate if this is heavy weight, if really required
public class KafkaProducerPool {

    private Logger logger = Logger.getLogger(KafkaProducerPool.class);
    private static KafkaProducerPool ourInstance = new KafkaProducerPool();

    private GenericObjectPool<KafkaProducer> pool;
    public static KafkaProducerPool getInstance() {
        return ourInstance;
    }

    final String producerConfigProperty ="BrokerConfig";

    private KafkaProducerPool() {
        pool = new GenericObjectPool<KafkaProducer>(new ProducerPoolFactory());
    }

    public synchronized void preFillPool() throws Exception {
        PoolUtils.prefill(pool,2);
    }

    public void closePool(){
        pool.close();
    }


    public KafkaProducer getProducer(){
        try {
            return pool.borrowObject();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public void returnToPool(final KafkaProducer producer){
        pool.returnObject(producer);
    }

    private class ProducerPoolFactory implements PooledObjectFactory<KafkaProducer>{

        @Override
        public PooledObject<KafkaProducer> makeObject() throws Exception {


            //get the config file
            String configFile = System.getProperty(producerConfigProperty);
            KafkaProducer kafkaProducer = null;
            if (configFile==null){
                kafkaProducer = new KafkaProducer();

            }else{
                kafkaProducer = new KafkaProducer(configFile);

            }

            return new DefaultPooledObject<KafkaProducer>(kafkaProducer);

        }

        @Override
        public void destroyObject(PooledObject<KafkaProducer> kafkaProducerPooledObject) throws Exception {
            kafkaProducerPooledObject.getObject().shutdown();
        }

        @Override
        public boolean validateObject(PooledObject<KafkaProducer> kafkaProducerPooledObject) {

            //very tricky to validate of the pooled object is not being used
            //check if zookeeper is knocked out then return false
            return true;
        }

        @Override
        public void activateObject(PooledObject<KafkaProducer> kafkaProducerPooledObject) throws Exception {

        }

        @Override
        public void passivateObject(PooledObject<KafkaProducer> kafkaProducerPooledObject) throws Exception {

        }
    }




}
