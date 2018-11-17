package com.emc.mongoose.storage.driver.pravega;

public interface PravegaConstants {

	String DRIVER_NAME = "pravega";

	String DEFAULT_URI_SCHEMA = "tcp";

	String DEFAULT_SCOPE = "goose"; // TODO replace this by the config option "storage-namespace" value

	int MAX_BACKOFF_MILLIS = 5_000;

	int BACKGROUND_THREAD_COUNT = 2;
}
