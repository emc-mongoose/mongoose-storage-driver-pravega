package com.emc.mongoose.storage.driver.pravega;

public interface PravegaConstants {
	String DRIVER_NAME = "pravega";
	String DEFAULT_URI_SCHEMA = "tcp";
	String DEFAULT_SCOPE = "test_scope"; // TODO replace this by the config option "storage-namespace" value
}
