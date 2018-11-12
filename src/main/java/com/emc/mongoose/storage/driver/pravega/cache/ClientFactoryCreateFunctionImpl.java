package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.ClientFactory;

import java.net.URI;

public final class ClientFactoryCreateFunctionImpl
implements ClientFactoryCreateFunction {

	private final URI endpointUri;

	public ClientFactoryCreateFunctionImpl(final URI endpointUri) {
		this.endpointUri = endpointUri;
	}

	@Override
	public final ClientFactory apply(final String scopeName) {
		return ClientFactory.withScope(scopeName, endpointUri);
	}

	@Override
	public final int hashCode() {
		return endpointUri.hashCode();
	}

	@Override
	public final boolean equals(final Object other) {
		if(other instanceof ClientFactoryCreateFunction) {
			final ClientFactoryCreateFunctionImpl that = (ClientFactoryCreateFunctionImpl) other;
			return this.endpointUri.equals(that.endpointUri);
		} else {
			return false;
		}
	}
}
