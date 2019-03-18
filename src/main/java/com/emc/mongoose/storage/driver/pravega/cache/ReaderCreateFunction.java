package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.stream.EventStreamReader;

import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 A function to create the event stream reader using the reader group name as the function argument
 */
public interface ReaderCreateFunction
extends Function<String, EventStreamReader<ByteBuffer>> {

	/**
	 @param readerGroup the reader group name
	 @return the created event stream reader
	 */
	@Override
	EventStreamReader<ByteBuffer> apply(String readerGroup);
}
