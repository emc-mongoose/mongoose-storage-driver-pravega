package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.control.impl.Controller;

import java.util.function.Function;

public interface ByteStreamClientFactoryCreateFunction
				extends Function<Controller, ByteStreamClientFactory> {

}
