package com.emc.mongoose.storage.driver.pravega.exception;

public abstract class StreamException
extends RuntimeException {

	protected StreamException(final String streamName, final Throwable cause) {
		super(streamName, cause);
	}

	public final String streamName() {
		return getMessage();
	}
}
