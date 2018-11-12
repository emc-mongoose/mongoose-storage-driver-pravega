package com.emc.mongoose.storage.driver.pravega.io;

import com.emc.mongoose.item.DataItem;

import com.emc.mongoose.logging.Loggers;
import com.github.akurilov.commons.system.SizeInBytes;
import io.pravega.client.stream.Serializer;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public final class DataItemSerializer
implements Serializer<DataItem>, Serializable {

	private final boolean useDirectMem;

	/**
	 * @param useDirectMem Specifies whether should it use direct memory for the resulting buffer containing the
	 *                     serialized data or not. Using the direct memory may lead to the better performance in case of
	 *                     large data chunks but less safe.
	 */
	public DataItemSerializer(final boolean useDirectMem) {
		this.useDirectMem = useDirectMem;
	}

	/**
	 * @param dataItem the Mongoose's data item to serialize
	 * @return the resulting byte buffer filled with data described by the given data item
	 * @throws OutOfMemoryError if useDirectMem is disabled and not enough memory to allocate the buffer
	 */
	@Override
	public final ByteBuffer serialize(final DataItem dataItem)
	throws OutOfMemoryError, IllegalArgumentException {
		final ByteBuffer dstBuff;
		try {
			final long dataItemSize = dataItem.size();
			if(Integer.MAX_VALUE < dataItemSize) {
				throw new IllegalArgumentException("Can't serialize the data item with size > 2^31 - 1");
			}
			if(MAX_EVENT_SIZE < dataItemSize) {
				Loggers.ERR.warn(
					"Event size is {}, Pravega storage doesn't support the event size more than {}",
					SizeInBytes.formatFixedSize(dataItemSize), SizeInBytes.formatFixedSize(MAX_EVENT_SIZE)
				);
			}
			if(useDirectMem) {
				dstBuff = ByteBuffer.allocateDirect((int) dataItemSize); // will crash if not enough memory
			} else {
				dstBuff = ByteBuffer.allocate((int) dataItemSize); // will throw OOM error if not enough memory
			}
			while(dstBuff.remaining() > 0) {
				dataItem.read(dstBuff);
			}
			dstBuff.flip();
			return dstBuff;
		} catch(final IOException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Not implemented. Do not invoke this.
	 * @throws AssertionError
	 */
	@Override
	public final DataItem deserialize(final ByteBuffer serializedValue) {
		throw new AssertionError("Not implemented");
	}
}
