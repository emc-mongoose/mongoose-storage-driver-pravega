package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.ClientFactory;

import java.util.function.Function;

/**
 A function to create the client factory using the scope name as the function argument
 */
public interface ClientFactoryCreateFunction
extends Function<String, ClientFactory> {

	/**
	 @param scopeName the scope name
	 @return the created client factory
	 */
	@Override
	ClientFactory apply(final String scopeName);
}
