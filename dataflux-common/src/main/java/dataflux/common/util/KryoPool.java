package dataflux.common.util;

import com.esotericsoftware.kryo.Kryo;
import dataflux.common.type.WebTxnData;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Kryo is not thread safe , need different instances
 * Takes approx 2-3ms to create on, instead just pool a bunch
 * Created by sumanthn
 */

public class KryoPool {

    private static KryoPool ourInstance = new KryoPool();
    private GenericObjectPool<Kryo> kryoPool;

    private KryoPool() {

        //TODO: externalize this config
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(2000);
        poolConfig.setMaxIdle(10);
        poolConfig.setMinIdle(5);
        poolConfig.setMaxWaitMillis(60 * 1000);
        kryoPool = new GenericObjectPool<Kryo>(new KryoPoolFactory(), poolConfig);
    }

    public static KryoPool getInstance() {
        return ourInstance;
    }

    public Kryo getKryo() {
        try {
            return kryoPool.borrowObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void returnToPool(final Kryo kryo) {
        this.kryoPool.returnObject(kryo);
    }

    class KryoPoolFactory implements PooledObjectFactory<Kryo> {

        final String packageOfTypes = "dataflux.type";

        @Override
        public PooledObject<Kryo> makeObject() throws Exception {
            Kryo kryo = new Kryo();
            //TODO: can automatically discover all types in the package type and register it
            //Reflections reflections = new Reflections(packageOfTypes);

            kryo.register(WebTxnData.class);
            kryo.register(Map.class);
            kryo.register(HashMap.class);
            return new DefaultPooledObject<Kryo>(kryo);
        }

        @Override
        public void destroyObject(PooledObject<Kryo> kryoPooledObject) throws Exception {

        }

        @Override
        public boolean validateObject(PooledObject<Kryo> kryoPooledObject) {
            return true;
        }

        @Override
        public void activateObject(PooledObject<Kryo> kryoPooledObject) throws Exception {

        }

        @Override
        public void passivateObject(PooledObject<Kryo> kryoPooledObject) throws Exception {

        }
    }
}
