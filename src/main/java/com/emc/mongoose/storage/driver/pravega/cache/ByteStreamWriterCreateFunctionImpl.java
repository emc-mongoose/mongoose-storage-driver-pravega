package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.byteStream.ByteStreamClient;
import io.pravega.client.byteStream.ByteStreamWriter;
import lombok.Value;

@Value
public final class ByteStreamWriterCreateFunctionImpl implements ByteStreamWriterCreateFunction {

	ByteStreamClient client;

	@Override
	public final ByteStreamWriter apply(final String streamName) {
		return client.createByteStreamWriter(streamName);
	}
}
