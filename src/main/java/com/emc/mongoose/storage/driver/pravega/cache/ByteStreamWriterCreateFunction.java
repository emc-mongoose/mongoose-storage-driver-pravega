package com.emc.mongoose.storage.driver.pravega.cache;

import com.github.akurilov.commons.io.util.BufferedWritableByteChannel;
import io.pravega.client.byteStream.ByteStreamWriter;
import java.util.function.Function;

public interface ByteStreamWriterCreateFunction extends Function<String, BufferedWritableByteChannel> {

	@Override
	BufferedWritableByteChannel apply(final String streamName);
}
