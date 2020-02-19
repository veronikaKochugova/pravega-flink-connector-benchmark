package serde;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;

import static org.apache.flink.util.Preconditions.checkNotNull;

@PublicEvolving
public class RawBytesSerializationSchema implements SerializationSchema<byte[]> {

    @Override
    public byte[] serialize(byte[] data) {
        return data;
    }

}
