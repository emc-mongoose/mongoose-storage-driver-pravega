package com.emc.mongoose.storage.driver.pravega.io;

import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.item.DataItemImpl;
import com.github.akurilov.commons.system.SizeInBytes;

import lombok.val;

import org.junit.Test;

import static com.emc.mongoose.base.Constants.MIB;
import static org.junit.Assert.assertEquals;

public class DataItemSerializerTest {

	@Test
	public final void serializeToDirectMemoryTest()
					throws Exception {
		try (val dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4KB"), 1)) {
			val dataItem = new DataItemImpl("test_event_item", 1234, MIB, 0);
			dataItem.dataInput(dataInput);
			val serializer = new DataItemSerializer(true);
			val serializedData = serializer.serialize(dataItem);
			assertEquals(MIB, serializedData.capacity());
			assertEquals(MIB, serializedData.remaining());
		}
	}

	@Test
	public final void serializeToHeapMemoryTest()
					throws Exception {
		try (val dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("10KB"), 1)) {
			val dataItem = new DataItemImpl("test_event_item", 5678, MIB, 0);
			dataItem.dataInput(dataInput);
			val serializer = new DataItemSerializer(false);
			val serializedData = serializer.serialize(dataItem);
			assertEquals(MIB, serializedData.capacity());
			assertEquals(MIB, serializedData.remaining());
		}
	}
}
