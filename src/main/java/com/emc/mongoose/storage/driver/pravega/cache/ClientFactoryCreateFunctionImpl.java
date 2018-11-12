package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.ClientFactory;

import lombok.AllArgsConstructor;
import lombok.Value;

import java.net.URI;

@AllArgsConstructor @Value
public final class ClientFactoryCreateFunctionImpl
implements ClientFactoryCreateFunction {

	URI endpointUri;

	@Override
	public final ClientFactory apply(final String scopeName) {
		return ClientFactory.withScope(scopeName, endpointUri);
	}
}
