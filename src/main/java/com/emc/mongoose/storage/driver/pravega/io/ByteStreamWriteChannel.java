package com.emc.mongoose.storage.driver.pravega.io;

import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.byteStream.ByteStreamWriter;
import lombok.val;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class ByteStreamWriteChannel
				implements WritableByteChannel {

	private final ByteStreamWriter out;

	public ByteStreamWriteChannel(final ByteStreamClientFactory clientFactory, final String streamName) {
		out = clientFactory.createByteStreamWriter(streamName);
	}

	@Override
	public final int write(final ByteBuffer src)
					throws IOException {
		val n = src.remaining();
		if (n > 0) {
			out.write(src);
		}
		return n;
	}

	@Override
	public final boolean isOpen() {
		return true;
	}

	@Override
	public final void close()
					throws IOException {
		out.close();
	}
}
