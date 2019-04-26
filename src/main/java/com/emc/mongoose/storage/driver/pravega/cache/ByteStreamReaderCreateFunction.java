package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.byteStream.ByteStreamReader;
import java.util.function.Function;

public interface ByteStreamReaderCreateFunction extends Function<String, ByteStreamReader> {

	@Override
	ByteStreamReader apply(final String streamName);
}
