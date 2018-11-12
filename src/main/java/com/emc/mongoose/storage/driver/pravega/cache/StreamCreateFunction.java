package com.emc.mongoose.storage.driver.pravega.cache;

import com.emc.mongoose.storage.driver.pravega.exception.StreamCreateException;

import java.util.function.Function;

/**
 A function to create the stream using the stream name as a function argument
 */
public interface StreamCreateFunction
extends Function<String, String> {

	/**
	 @param streamName the name of the stream to create
	 @return the name of the created stream (same as the stream name on the input)
	 @throws StreamCreateException
	 */
	@Override
	String apply(final String streamName)
	throws StreamCreateException;
}
