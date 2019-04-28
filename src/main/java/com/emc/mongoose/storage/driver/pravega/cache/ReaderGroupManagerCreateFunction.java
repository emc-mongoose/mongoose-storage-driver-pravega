package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.admin.ReaderGroupManager;
import java.util.function.Function;

/** A function to create the reader group manager using the scope name as the function argument */
public interface ReaderGroupManagerCreateFunction extends Function<String, ReaderGroupManager> {

  /**
   * @param scopeName the scope name
   * @return the created reader group manager
   */
  @Override
  ReaderGroupManager apply(final String scopeName);
}
