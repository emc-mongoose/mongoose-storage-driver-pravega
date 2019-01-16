package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.admin.ReaderGroupManager;
import lombok.Value;

import java.net.URI;

@Value
public final class ReaderGroupManagerCreateFunctionImpl
implements ReaderGroupManagerCreateFunction {

	URI endpointUri;

	@Override
	public final ReaderGroupManager apply(final String scopeName) {
		return ReaderGroupManager.withScope(scopeName, endpointUri);
	}
}
