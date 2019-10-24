package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;

import lombok.Value;

@Value
public final class ReaderGroupManagerCreateFunctionImpl
				implements ReaderGroupManagerCreateFunction {

	ClientConfig clientConfig;

	@Override
	public final ReaderGroupManager apply(final String scopeName) {
		return ReaderGroupManager.withScope(scopeName, clientConfig);
	}
}
