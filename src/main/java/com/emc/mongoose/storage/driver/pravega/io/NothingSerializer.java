package com.emc.mongoose.storage.driver.pravega.io;

import com.emc.mongoose.base.item.DataItem;
import com.emc.mongoose.base.logging.Loggers;
import com.github.akurilov.commons.system.SizeInBytes;
import io.pravega.client.stream.Serializer;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class NothingSerializer<I extends ByteBuffer>
        implements Serializer<I>, Serializable {

    private final boolean useDirectMem;

    /**
     * @param useDirectMem Specifies whether should it use direct memory for the resulting buffer containing the
     *                     serialized data or not. Using the direct memory may lead to the better performance in case of
     *                     large data chunks but less safe.
     */
    public NothingSerializer(final boolean useDirectMem) {
        this.useDirectMem = useDirectMem;
    }

    /**
     * @param buf the Mongoose's data item to serialize
     * @return the resulting byte buffer filled with data described by the given data item
     */
    @Override
    public final ByteBuffer serialize(final I buf) {
            return buf;
    }

    /**
     * Not implemented. Do not invoke this.
     * @throws AssertionError
     */
    @Override
    public final I deserialize(final ByteBuffer serializedValue) {
        throw new AssertionError("Not implemented");
    }
}
