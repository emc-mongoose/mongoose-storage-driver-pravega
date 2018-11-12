package com.emc.mongoose.storage.driver.pravega.exception;

public class StreamCreateException
extends RuntimeException {

	public StreamCreateException(final String streamName, final Throwable cause) {
		super(streamName, cause);
	}

	public final String streamName() {
		return getMessage();
	}
}
