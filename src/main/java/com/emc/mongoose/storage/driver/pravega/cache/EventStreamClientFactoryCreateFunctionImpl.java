package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import lombok.Value;

@Value
public final class EventStreamClientFactoryCreateFunctionImpl
    implements EventStreamClientFactoryCreateFunction {

  ClientConfig clientConfig;

  @Override
  public final EventStreamClientFactory apply(final String scopeName) {
    return EventStreamClientFactory.withScope(scopeName, clientConfig);
  }
}
