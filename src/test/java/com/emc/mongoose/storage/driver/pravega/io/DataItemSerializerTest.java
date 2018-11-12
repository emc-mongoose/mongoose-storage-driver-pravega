package com.emc.mongoose.storage.driver.pravega.io;

import com.emc.mongoose.data.DataInput;
import com.emc.mongoose.item.DataItem;
import com.emc.mongoose.item.DataItemImpl;
import static com.emc.mongoose.Constants.MIB;

import com.github.akurilov.commons.system.SizeInBytes;

import io.pravega.client.stream.Serializer;

import java.nio.ByteBuffer;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class DataItemSerializerTest {

	@Test
	public final void serializeToDirectMemoryTest()
	throws Exception {
		try(final DataInput dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4KB"), 1)) {
			final DataItem dataItem = new DataItemImpl("test_event_item", 1234, MIB, 0);
			dataItem.dataInput(dataInput);
			final Serializer<DataItem> serializer = new DataItemSerializer(true);
			final ByteBuffer serializedData = serializer.serialize(dataItem);
			assertEquals(MIB, serializedData.capacity());
			assertEquals(MIB, serializedData.remaining());
		}
	}

	@Test
	public final void serializeToHeapMemoryTest()
	throws Exception {
		try(final DataInput dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("10KB"), 1)) {
			final DataItem dataItem = new DataItemImpl("test_event_item", 5678, MIB, 0);
			dataItem.dataInput(dataInput);
			final Serializer<DataItem> serializer = new DataItemSerializer(false);
			final ByteBuffer serializedData = serializer.serialize(dataItem);
			assertEquals(MIB, serializedData.capacity());
			assertEquals(MIB, serializedData.remaining());
		}
	}
}
