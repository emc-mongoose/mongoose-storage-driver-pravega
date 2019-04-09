package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import lombok.val;

import java.net.URI;

public interface CustomThreadCountClientFactory
extends ClientFactory {

	/**
	 * Creates a new instance of Client Factory.
	 *
	 * @param scope The scope string.
	 * @param controllerUri The URI for controller.
	 * @return Instance of ClientFactory implementation.
	 */
	static CustomThreadCountClientFactory withScope(
		final String scope, final URI controllerUri, final int threadCount
	) {
		return withScope(scope, ClientConfig.builder().controllerURI(controllerUri).build(), threadCount);
	}

	/**
	 * Creates a new instance of Client Factory.
	 *
	 * @param scope The scope string.
	 * @param config Configuration for the client.
	 * @return Instance of ClientFactory implementation.
	 */
	static CustomThreadCountClientFactory withScope(
		final String scope, final ClientConfig config, final int threadCount
	) {
		val connectionFactory = new ConnectionFactoryImpl(config, threadCount);
		return new CustomThreadCountClientFactoryImpl(
			scope, new ControllerImpl(ControllerImplConfig.builder().clientConfig(config).build(),
			connectionFactory.getInternalExecutor()), connectionFactory
		);
	}
}
