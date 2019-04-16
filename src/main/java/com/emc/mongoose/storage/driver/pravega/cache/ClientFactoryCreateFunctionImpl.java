package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.ClientFactory;

import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import lombok.Value;

@Value
public final class ClientFactoryCreateFunctionImpl
implements ClientFactoryCreateFunction {

	Controller controller;
	ConnectionFactory connFactory;

	@Override
	public final ClientFactory apply(final String scopeName) {
		return new ClientFactoryImpl(scopeName, controller, connFactory);
	}
}
