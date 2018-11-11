package com.emc.mongoose.storage.driver.pravega.cache;

import com.emc.mongoose.item.DataItem;

import io.pravega.client.stream.EventStreamWriter;

import java.util.function.Function;

/**
 A function to create the event stream writer using a stream name as a function argument
 */
public interface EventWriterCreateFunction
extends Function<String, EventStreamWriter<DataItem>> {

	/**
	 @param streamName the destination stream name
	 @return the event writer for the given stream
	 */
	@Override
	EventStreamWriter<DataItem> apply(final String streamName);
}
