package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.byteStream.ByteStreamWriter;

import java.util.function.Function;

public interface ByteStreamWriterCreateFunction
extends Function<String, ByteStreamWriter> {

	@Override
	ByteStreamWriter apply(final String streamName);
}
