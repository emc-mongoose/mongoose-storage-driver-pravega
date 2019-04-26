package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.stream.StreamConfiguration;

import java.util.function.Function;

/**
 A function to create the scope using the scope name as a function argument
 */
public interface ScopeCreateFunctionForStreamConfig
				extends Function<String, StreamConfiguration> {

	/**
	 @param scopeName the name of the scope to create
	 @return the corresponding stream configuration
	 */
	@Override
	StreamConfiguration apply(final String scopeName);
}
