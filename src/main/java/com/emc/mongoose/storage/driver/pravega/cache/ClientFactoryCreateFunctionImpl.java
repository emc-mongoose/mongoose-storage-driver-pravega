package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.ClientFactory;

import lombok.Value;

import java.net.URI;

@Value
public final class ClientFactoryCreateFunctionImpl
implements ClientFactoryCreateFunction {

	URI endpointUri;

	@Override
	public final ClientFactory apply(final String scopeName) {
		return ClientFactory.withScope(scopeName, endpointUri);
	}
}
