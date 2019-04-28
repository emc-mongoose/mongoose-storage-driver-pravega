package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.admin.ReaderGroupManager;
import java.net.URI;
import lombok.Value;

@Value
public final class ReaderGroupManagerCreateFunctionImpl
    implements ReaderGroupManagerCreateFunction {

  URI endpointUri;

  @Override
  public final ReaderGroupManager apply(final String scopeName) {
    return ReaderGroupManager.withScope(scopeName, endpointUri);
  }
}
