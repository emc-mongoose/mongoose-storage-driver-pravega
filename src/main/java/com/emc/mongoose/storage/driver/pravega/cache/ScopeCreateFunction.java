package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.stream.StreamConfiguration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 A function to create the scope using the scope name as a function argument
 */
public interface ScopeCreateFunction
				extends Function<String, StreamCreateFunction> {

	/**
	 @param scopeName the name of the scope to create
	 @return the function to create a stream in the given scope
	 */
	@Override
	StreamCreateFunction apply(final String scopeName);
}
