package com.emc.mongoose.storage.driver.pravega.exception;

public class StreamUpdateException
extends StreamException {

	public StreamUpdateException(final String streamName, final Throwable cause) {
		super(streamName, cause);
	}
}
