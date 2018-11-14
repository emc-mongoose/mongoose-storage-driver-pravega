package com.emc.mongoose.storage.driver.pravega.cache;

import com.emc.mongoose.storage.driver.pravega.exception.StreamCreateException;

import io.pravega.client.stream.StreamConfiguration;

import java.util.function.Function;

/**
 A function to create the stream using the stream name as a function argument
 */
public interface StreamCreateFunction
extends Function<String, StreamConfiguration> {

	/**
	 @param streamName the name of the stream to create
	 @return the corresponding stream configuration
	 @throws StreamCreateException
	 */
	@Override
	StreamConfiguration apply(final String streamName)
	throws StreamCreateException;
}
