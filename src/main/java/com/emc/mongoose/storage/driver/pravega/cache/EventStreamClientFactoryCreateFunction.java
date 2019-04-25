package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.EventStreamClientFactory;

import java.util.function.Function;

/**
 A function to create the client factory using the scope name as the function argument
 */
public interface EventStreamClientFactoryCreateFunction
				extends Function<String, EventStreamClientFactory> {

	/**
	 @param scopeName the scope name
	 @return the created client factory
	 */
	@Override
	EventStreamClientFactory apply(final String scopeName);
}
