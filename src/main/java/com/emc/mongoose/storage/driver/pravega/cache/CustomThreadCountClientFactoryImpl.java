package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;

public class CustomThreadCountClientFactoryImpl
extends ClientFactoryImpl
implements CustomThreadCountClientFactory {

	public CustomThreadCountClientFactoryImpl(
		final String scope, final Controller controller, final ConnectionFactory connectionFactory
	) {
		super(scope, controller, connectionFactory);
	}
}
