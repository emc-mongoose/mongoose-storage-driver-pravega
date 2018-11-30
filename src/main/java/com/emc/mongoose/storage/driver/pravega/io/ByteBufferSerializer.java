package com.emc.mongoose.storage.driver.pravega.io;

import io.pravega.client.stream.Serializer;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class ByteBufferSerializer
implements Serializer<Integer>, Serializable {

	/**
	 * Not implemented. Do not invoke this.
	 *
	 * @throws AssertionError
	 */
	@Override
	public final ByteBuffer serialize(final Integer someNumber) {
		throw new AssertionError("Not implemented");
	}

	/**
	 * Deserializes the given ByteBuffer into an event.
	 *
	 * @param serializedValue An event that has been previously serialized.
	 * @return countBytesDone.
	 */
	@Override
	public final Integer deserialize(final ByteBuffer serializedValue)
	throws OutOfMemoryError, IllegalArgumentException {
		if (serializedValue == null) {
			return 0;
		}
		//as we do a flip in serialize buffer method after filling a buffer
		//we don't need it here
		int bytesDone = 0;
		while (serializedValue.remaining() > 0) {
			serializedValue.get();
			++bytesDone;
		}

		//val dataItemSize = dataItem.size();
		return bytesDone;
	}
}
