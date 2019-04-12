package com.emc.mongoose.storage.driver.pravega.cache;

import io.pravega.client.byteStream.ByteStreamClient;
import io.pravega.client.byteStream.ByteStreamReader;
import lombok.Value;

@Value
public final class ByteStreamReaderCreateFunctionImpl implements ByteStreamReaderCreateFunction {

  ByteStreamClient client;

  @Override
  public final ByteStreamReader apply(final String streamName) {
    return client.createByteStreamReader(streamName);
  }
}
