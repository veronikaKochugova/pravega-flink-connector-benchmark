package serde;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class RawBytesDeserializationSchema implements DeserializationSchema {

    @Override
    public Object deserialize(final byte[] message) throws IOException {
        return message;
    }

    @Override
    public boolean isEndOfStream(final Object nextElement) {
        return false;
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(byte[].class);
    }


}
