package com.emc.mongoose.storage.driver.pravega.integration;

import static com.emc.mongoose.base.Constants.APP_NAME;
import static com.emc.mongoose.base.Constants.MIB;
import static org.junit.Assert.*;

import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.env.Extension;
import com.emc.mongoose.base.item.DataItem;
import com.emc.mongoose.base.item.DataItemImpl;
import com.emc.mongoose.base.item.op.OpType;
import com.emc.mongoose.base.item.op.Operation;
import com.emc.mongoose.base.item.op.data.DataOperation;
import com.emc.mongoose.base.item.op.data.DataOperationImpl;
import com.emc.mongoose.storage.driver.pravega.PravegaStorageDriver;
import com.emc.mongoose.storage.driver.pravega.io.ByteBufferSerializer;
import com.emc.mongoose.storage.driver.pravega.util.PravegaNode;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.io.collection.ListOutput;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.*;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.Ignore;
import org.junit.Test;

public class DataOperationsTest extends PravegaStorageDriver<DataItem, DataOperation<DataItem>> {
	private static final DataInput DATA_INPUT;

	static {
		try {
			DATA_INPUT = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes(1024 * 1024 - 8), 16, false);
		} catch (final IOException e) {
			throw new AssertionError(e);
		}
	}

	static Config getConfig() {
		try {
			val configSchemas = Extension.load(Thread.currentThread().getContextClassLoader()).stream()
							.map(Extension::schemaProvider)
							.filter(Objects::nonNull)
							.map(
											schemaProvider -> {
												try {
													return schemaProvider.schema();
												} catch (final Exception e) {
													fail(e.getMessage());
												}
												return null;
											})
							.filter(Objects::nonNull)
							.collect(Collectors.toList());
			SchemaProvider.resolve(APP_NAME, Thread.currentThread().getContextClassLoader()).stream()
							.findFirst()
							.ifPresent(configSchemas::add);
			val configSchema = TreeUtil.reduceForest(configSchemas);
			val config = new BasicConfig("-", configSchema);

			config.val("load-batch-size", 1_000);

			config.val("storage-net-reuseAddr", true);
			config.val("storage-net-bindBacklogSize", 0);
			config.val("storage-net-keepAlive", true);
			config.val("storage-net-rcvBuf", 0);
			config.val("storage-net-sndBuf", 0);
			config.val("storage-net-uri-schema", "tcp");
			config.val("storage-net-ssl", false);
			config.val("storage-net-tcpNoDelay", false);
			config.val("storage-net-interestOpQueued", false);
			config.val("storage-net-linger", 0);
			config.val("storage-net-timeoutMillis", 0);
			config.val("storage-net-node-addrs", PravegaNode.addr());
			config.val("storage-net-node-port", PravegaNode.PORT);
			config.val("storage-net-node-conn-attemptsLimit", 0);
			config.val("storage-net-node-conn-pooling", true);
			config.val("storage-net-maxConnPerSegmentstore", 5);
			
			config.val("storage-auth-uid", null);
			config.val("storage-auth-token", null);
			config.val("storage-auth-secret", null);

			config.val("storage-driver-control-scope", true);
			config.val("storage-driver-control-stream", true);
			config.val("storage-driver-control-timeoutMillis", 10_000);
			config.val("storage-driver-create-timestamp", false);
			config.val("storage-driver-event-transaction", false);
			config.val("storage-driver-event-key-enabled", true);
			config.val("storage-driver-event-key-count", 0);
			config.val("storage-driver-event-timeoutMillis", 100);
			config.val("storage-driver-scaling-type", "fixed");
			config.val("storage-driver-scaling-rate", 0);
			config.val("storage-driver-scaling-factor", 0);
			config.val("storage-driver-scaling-segments", 1);
			config.val("storage-driver-stream-data", "events");
			config.val("storage-driver-threads", 0);
			config.val("storage-driver-limit-queue-input", 1_000);
			config.val("storage-driver-limit-concurrency", 0);
			config.val("storage-namespace", "goose");

			return config;
		} catch (final Throwable cause) {
			throw new RuntimeException(cause);
		}
	}

	public DataOperationsTest() {
		this(getConfig());
	}

	private DataOperationsTest(final Config config) {
		super(
						"test-data-pravega-driver",
						DATA_INPUT,
						config.configVal("storage"),
						true,
						config.configVal("load").intVal("batch-size"));
		start();
	}

	@Test
	public final void testCreateEvent() throws Exception {
		val dataItem = new DataItemImpl(0, MIB, 0);
		dataItem.name("0000");
		dataItem.dataInput(DATA_INPUT);
		String streamName = "default";
		final DataOperation<DataItem> createTask = new DataOperationImpl<>(
						0, OpType.CREATE, dataItem, null, streamName, credential, null, 0, null);

		String scope = "goose";
		val result = new ArrayList<DataOperation<DataItem>>(1);
		val resultOut = new ListOutput<DataOperation<DataItem>>(result);
		operationResultOutput(resultOut);
		prepare(createTask);
		createTask.status(Operation.Status.ACTIVE);
		put(createTask);
		while (result.isEmpty()) {
			Thread.sleep(1);
		}
		assertEquals(Operation.Status.SUCC, result.get(0).status());
		assertEquals(dataItem.size(), createTask.countBytesDone());

		final URI controllerURI = URI.create(
						"tcp://"
										+ getConfig().listVal("storage-net-node-addrs").get(0)
										+ ":"
										+ String.valueOf(getConfig().intVal("storage-net-node-port")));
		final String readerGroup = UUID.randomUUID().toString().replace("-", "");
		final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().stream(Stream.of(scope, streamName)).build();
		try (final ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
			readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
		}

		try (val clientFactory = EventStreamClientFactory.withScope(scope,
				ClientConfig.builder().controllerURI(controllerURI).build());
			 EventStreamReader<ByteBuffer> reader = clientFactory.createReader(
										"reader",
										readerGroup,
										new ByteBufferSerializer(),
										ReaderConfig.builder().build())) {
			System.out.format("Reading all the events from %s/%s%n", scope, streamName);
			EventRead<ByteBuffer> event = null;
			event = reader.readNextEvent(evtOpTimeoutMillis);
			if (event.getEvent() != null) {
				assertEquals(
								"we didn't read the same size we had put into stream",
								(int) dataItem.size(),
								event.getEvent().remaining());
			}
		}
	}

	@Test
	@Ignore
	public final void testReadEvent() throws Exception {
		final DataItem dataItem = new DataItemImpl(0, MIB, 0);
		dataItem.name("0000");
		dataItem.dataInput(DATA_INPUT);
		String streamName = "default";
		final DataOperation<DataItem> createTask = new DataOperationImpl<>(0, OpType.CREATE, dataItem, null, streamName, credential, null, 0, null);
		String scope = "goose";
		val results = new ArrayList<DataOperation<DataItem>>(2);
		val resultOut = new ListOutput<DataOperation<DataItem>>(results);
		operationResultOutput(resultOut);
		prepare(createTask);
		createTask.status(Operation.Status.ACTIVE);
		put(createTask);
		while (results.isEmpty()) {
			Thread.sleep(1);
		}
		assertEquals(Operation.Status.SUCC, results.get(0).status());
		assertEquals(dataItem.size(), createTask.countBytesDone());
		final DataItem dataItem2 = new DataItemImpl(0, MIB, 0);
		dataItem2.name("0001");
		final DataOperation<DataItem> createTask2 = new DataOperationImpl<>(0, OpType.READ, dataItem2, null, streamName, credential, null, 0, null);
		prepare(createTask2);
		createTask2.status(Operation.Status.ACTIVE);
		assertTrue(put(createTask2));
		while (results.size() < 2) {
			Thread.sleep(1);
		}
		val result2 = results.get(1);
		val item2 = result2.item();
		assertEquals("we didn't read the same size we had put into stream", (int) dataItem.size(), item2.size());
	}
}
