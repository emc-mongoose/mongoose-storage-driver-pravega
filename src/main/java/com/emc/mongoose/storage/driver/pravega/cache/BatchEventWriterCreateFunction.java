package com.emc.mongoose.storage.driver.pravega.cache;

import com.emc.mongoose.base.item.DataItem;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.TransactionalEventStreamWriter;

import java.util.function.Function;

public interface BatchEventWriterCreateFunction<I extends DataItem>
				extends Function<EventStreamClientFactory, TransactionalEventStreamWriter<I>> {

	@Override
	TransactionalEventStreamWriter<I> apply(final EventStreamClientFactory clientFactory);
}
