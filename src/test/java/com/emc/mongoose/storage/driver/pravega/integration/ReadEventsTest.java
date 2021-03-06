package com.emc.mongoose.storage.driver.pravega.integration;

import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.item.DataItem;
import com.emc.mongoose.base.item.DataItemImpl;
import com.emc.mongoose.base.item.op.data.DataOperation;
import com.emc.mongoose.base.item.op.data.DataOperationImpl;
import com.emc.mongoose.base.storage.Credential;
import com.emc.mongoose.base.storage.driver.StorageDriver;
import com.emc.mongoose.storage.driver.pravega.PravegaStorageDriver;
import com.github.akurilov.commons.io.collection.ListOutput;
import com.github.akurilov.commons.system.SizeInBytes;
import lombok.val;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.emc.mongoose.base.Constants.MIB;
import static com.emc.mongoose.base.item.op.OpType.CREATE;
import static com.emc.mongoose.base.item.op.OpType.READ;
import static com.emc.mongoose.base.item.op.Operation.SLASH;
import static com.emc.mongoose.base.item.op.Operation.Status.SUCC;
import static org.junit.Assert.assertEquals;

@Ignore
public class ReadEventsTest {

	private static final int EVENT_COUNT = 100;
	private static final String STREAM_NAME = "stream1";

	private DataInput dataInput;
	private StorageDriver driver;
	private List<DataItem> evtItems;

	@Before
	public void setUp()
					throws Exception {
		dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes(1024 * 1024 - 8), 16, true);
		val config = DataOperationsTest.getConfig();
		driver = new PravegaStorageDriver(
						getClass().getSimpleName(), dataInput, config.configVal("storage"), false, 32768);
		val createResults = new ArrayList<DataOperation<DataItem>>(EVENT_COUNT);
		val createResultsOut = new ListOutput<DataOperation<DataItem>>(createResults);
		driver.operationResultOutput(createResultsOut);
		driver.start();
		evtItems = new ArrayList<>(EVENT_COUNT);
		for (var i = 0; i < EVENT_COUNT; i++) {
			evtItems.add(new DataItemImpl(Integer.toString(i), i, MIB));
		}
		val createOps = evtItems
						.stream()
						.map(
										evtItem -> new DataOperationImpl<>(
														0, CREATE, evtItem, SLASH + STREAM_NAME, SLASH + STREAM_NAME, Credential.NONE, null, 0))
						.collect(Collectors.toList());
		for (var i = 0; i < EVENT_COUNT; i += driver.put(createOps, i, EVENT_COUNT));
		for (var i = 0; i < 10; i ++) {
			if(EVENT_COUNT == createResults.size()) {
				break;
			}
			Thread.sleep(1_000);
		}
		assertEquals(EVENT_COUNT, createResults.size());
		for (var i = 0; i < EVENT_COUNT; i++) {
			val createResult = createResults.get(i);
			assertEquals(SUCC, createResult.status());
			assertEquals(MIB, createResult.countBytesDone());
		}
	}

	@After
	public void tearDown()
					throws Exception {
		driver.close();
		dataInput.close();
	}

	@Test
	public void testReadEvents1()
					throws Exception {
		val readOps = evtItems
						.stream()
						.map(
										evtItem -> new DataOperationImpl<>(
														0, READ, evtItem, SLASH + STREAM_NAME, SLASH + STREAM_NAME, Credential.NONE, null, 0))
						.collect(Collectors.toList());
		val readResults = new ArrayList<DataOperation<DataItem>>(EVENT_COUNT);
		val readResultsOut = new ListOutput<DataOperation<DataItem>>(readResults);
		driver.operationResultOutput(readResultsOut);
		for (var i = 0; i < EVENT_COUNT; i += driver.put(readOps, i, EVENT_COUNT));
		for (var i = 0; i < 10; i ++) {
			if(EVENT_COUNT == readResults.size()) {
				break;
			}
			Thread.sleep(1_000);
		}
		assertEquals(EVENT_COUNT, readResults.size());
		for (var i = 0; i < EVENT_COUNT; i++) {
			val readResult = readResults.get(i);
			assertEquals("Event #" + i + " read failed", SUCC, readResult.status());
			assertEquals(MIB, readResult.countBytesDone());
		}
	}

	@Test
	public void testReadEvents2()
					throws Exception {
		val readOps = evtItems
						.stream()
						.map(
										evtItem -> new DataOperationImpl<>(
														0, READ, evtItem, SLASH + STREAM_NAME, SLASH + STREAM_NAME, Credential.NONE, null, 0))
						.collect(Collectors.toList());
		val readResults = new ArrayList<DataOperation<DataItem>>(EVENT_COUNT);
		val readResultsOut = new ListOutput<DataOperation<DataItem>>(readResults);
		driver.operationResultOutput(readResultsOut);
		for (var i = 0; i < EVENT_COUNT; i += driver.put(readOps, i, EVENT_COUNT)) ;
		for (var i = 0; i < 10; i ++) {
			if(EVENT_COUNT == readResults.size()) {
				break;
			}
			Thread.sleep(1_000);
		}
		assertEquals(EVENT_COUNT, readResults.size());
		for (var i = 0; i < EVENT_COUNT; i++) {
			val readResult = readResults.get(i);
			assertEquals("Event #" + i + " read failed", SUCC, readResult.status());
			assertEquals(MIB, readResult.countBytesDone());
		}
	}
}
