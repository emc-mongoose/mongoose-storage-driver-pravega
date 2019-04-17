package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.byteStream.ByteStreamReader;
import lombok.Value;

@Value
public final class ByteStreamReaderCreateFunctionImpl implements ByteStreamReaderCreateFunction {

  ByteStreamClientFactory clientFactory;

  @Override
  public final ByteStreamReader apply(final String streamName) {
    return clientFactory.createByteStreamReader(streamName);
  }
}
