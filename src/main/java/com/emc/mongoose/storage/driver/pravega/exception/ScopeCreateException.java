package com.emc.mongoose.storage.driver.pravega.exception;

public class ScopeCreateException
extends RuntimeException {

	public ScopeCreateException(final String scopeName, final Throwable cause) {
		super(scopeName, cause);
	}

	public final String scopeName() {
		return getMessage();
	}
}
