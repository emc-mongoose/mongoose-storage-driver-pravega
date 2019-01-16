package com.emc.mongoose.storage.driver.pravega.cache;

import com.emc.mongoose.storage.driver.pravega.io.DataItemSerializer;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import lombok.Value;

@Value
public final class ReaderCreateFunctionImpl
		implements ReaderCreateFunction {

	ClientFactory clientFactory;

	@Override
	public EventStreamReader apply(String readerGroup) {
		return clientFactory.createReader("reader", readerGroup, new DataItemSerializer(true), ReaderConfig.builder().build());

	}
}
