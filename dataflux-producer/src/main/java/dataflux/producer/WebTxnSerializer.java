package dataflux.producer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import dataflux.common.util.KryoPool;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

/**
 * Created by sumanthn on 24/3/14.
 */
public class WebTxnSerializer implements Encoder<Object> {

    KryoPool pool = KryoPool.getInstance();
    public WebTxnSerializer(VerifiableProperties prop){ }

    @Override
    public byte[] toBytes(Object dataItem) {
        Kryo kryoInstance = pool.getKryo();
        ByteBufferOutput bufferOutput = new ByteBufferOutput(4096);
        kryoInstance.writeObject(bufferOutput,dataItem);
        byte [] msg = bufferOutput.toBytes();

        bufferOutput.clear();
        pool.returnToPool(kryoInstance);
        return msg;
    }
}
