package com.emc.mongoose.storage.driver.pravega.cache;

import com.github.akurilov.commons.io.util.BufferedWritableByteChannel;
import com.github.akurilov.commons.io.util.OutputStreamWrapperChannel;
import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.byteStream.ByteStreamClient;
import io.pravega.client.byteStream.ByteStreamWriter;
import lombok.Value;

@Value
public final class ByteStreamWriterCreateFunctionImpl implements ByteStreamWriterCreateFunction {

	ByteStreamClientFactory clientFactory;

	@Override
	public final BufferedWritableByteChannel apply(final String streamName) {
		return new OutputStreamWrapperChannel(clientFactory.createByteStreamWriter(streamName), 0x1000);
	}
}
