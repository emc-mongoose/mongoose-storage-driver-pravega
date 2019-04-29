package com.emc.mongoose.storage.driver.pravega.io;

import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.byteStream.ByteStreamWriter;
import lombok.val;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class ByteStreamWriteChannel
implements WritableByteChannel {

	public static ByteStreamWriteChannel newOrReuseInstance(
		final ByteStreamClientFactory clientFactory, final String streamName, final ByteStreamWriteChannel chan
	) {
		val chan_ = chan == null ? new ByteStreamWriteChannel() : chan;
		chan_.out = clientFactory.createByteStreamWriter(streamName);
		return chan_;
	}

	private volatile ByteStreamWriter out = null;

	@Override
	public final int write(final ByteBuffer src)
	throws IOException {
		val n = src.remaining();
		if(n > 0) {
			out.write(src);
		}
		return n;
	}

	@Override
	public final boolean isOpen() {
		return out != null;
	}

	@Override
	public final void close()
	throws IOException {
		if(out != null) {
			out.flush();
			out = null;
		}
	}
}
