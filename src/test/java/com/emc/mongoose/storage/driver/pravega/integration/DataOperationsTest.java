package com.emc.mongoose.storage.driver.pravega.integration;

import com.emc.mongoose.data.DataInput;
import com.emc.mongoose.env.Extension;
import com.emc.mongoose.exception.OmgShootMyFootException;
import com.emc.mongoose.item.DataItem;
import com.emc.mongoose.item.DataItemImpl;
import com.emc.mongoose.item.op.OpType;
import com.emc.mongoose.item.op.Operation;
import com.emc.mongoose.item.op.data.DataOperation;
import com.emc.mongoose.item.op.data.DataOperationImpl;
import com.emc.mongoose.storage.Credential;

import com.emc.mongoose.storage.driver.pravega.PravegaConstants;
import com.emc.mongoose.storage.driver.pravega.PravegaStorageDriver;
import com.emc.mongoose.storage.driver.pravega.io.ByteBufferSerializer;
import com.emc.mongoose.storage.driver.pravega.util.docker.PravegaNodeContainer;
import com.github.akurilov.commons.collection.Range;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.val;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import java.util.stream.IntStream;

import static com.emc.mongoose.Constants.APP_NAME;
import static com.emc.mongoose.Constants.MIB;
import static com.emc.mongoose.storage.driver.pravega.PravegaConstants.DEFAULT_SCOPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataOperationsTest
extends PravegaStorageDriver<DataItem, DataOperation<DataItem>>{
private static final DataInput DATA_INPUT;

	static {
		try {
			DATA_INPUT = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("1MB"), 16);
		} catch (final IOException e) {
			throw new AssertionError(e);
		}
	}

	private static PravegaNodeContainer PRAVEGA_NODE_CONTAINER;

	private static Config getConfig() {
		try {
			val configSchemas = Extension
				.load(Thread.currentThread().getContextClassLoader())
				.stream()
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
					}
				)
				.filter(Objects::nonNull)
				.collect(Collectors.toList());
			SchemaProvider
				.resolve(APP_NAME, Thread.currentThread().getContextClassLoader())
				.stream()
				.findFirst()
				.ifPresent(configSchemas::add);
			val configSchema = TreeUtil.reduceForest(configSchemas);
			val config = new BasicConfig("-", configSchema);

			config.val("load-batch-size", 4096);
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
			config.val("storage-net-node-addrs", Collections.singletonList("127.0.0.1"));
			config.val("storage-net-node-port", PravegaNodeContainer.PORT);
			config.val("storage-net-node-connAttemptsLimit", 0);

			config.val("storage-auth-uid", null);
			config.val("storage-auth-token", null);
			config.val("storage-auth-secret", null);

			config.val("storage-driver-control-timeoutMillis", 10_000);
			config.val("storage-driver-create-key-enabled", true);
			config.val("storage-driver-create-key-count", 0);
			config.val("storage-driver-read-timeoutMillis", 100);
			config.val("storage-driver-scaling-type", "fixed");
			config.val("storage-driver-scaling-rate", 0);
			config.val("storage-driver-scaling-factor", 0);
			config.val("storage-driver-scaling-segments", 1);
			config.val("storage-driver-threads", 0);
			config.val("storage-driver-limit-queue-input", 1_000_000);
			config.val("storage-driver-limit-queue-output", 1_000_000);
			config.val("storage-driver-limit-concurrency", 0);
			config.val("storage-namespace-scope", "goose");

			return config;
		} catch (final Throwable cause) {
			throw new RuntimeException(cause);
		}
	}

	public DataOperationsTest()
	throws OmgShootMyFootException {
		this(getConfig());
	}

	private DataOperationsTest(final Config config)
			throws OmgShootMyFootException {
		super("test-data-pravega-driver", DATA_INPUT, config.configVal("storage"), true,
				config.configVal("load").intVal("batch-size")
		);
	}

	@BeforeClass
	public static void setUpClass()
	throws Exception {
		try {
			PRAVEGA_NODE_CONTAINER = new PravegaNodeContainer();
		} catch (final Exception e) {
			throw new AssertionError(e);
		}
	}

	@AfterClass
	public static void tearDownClass()
	throws Exception {
		PRAVEGA_NODE_CONTAINER.close();
	}


	@Test
	public final void testCreateEvent()
			throws Exception {

		//final URI controllerURI = URI.create(getConfig().listVal("storage-net-node-addrs").get(0)+":"+String.valueOf(getConfig().intVal("storage-net-node-port")));
		//System.out.println(controllerURI.toString());
		final DataItem dataItem = new DataItemImpl(0, MIB, 0);
		dataItem.name("0000");
		dataItem.dataInput(DATA_INPUT);
		String streamName = "default";
		final DataOperation<DataItem> createTask = new DataOperationImpl<>(
				0, OpType.CREATE, dataItem, null, streamName, credential, null, 0, null
		);


		String scope = DEFAULT_SCOPE;
		prepare(createTask);
		createTask.status(Operation.Status.ACTIVE);
		//while(Operation.Status.ACTIVE.equals(createTask.status())) {
			submit(createTask);
		//}
		DataOperation<DataItem> result = get();
		while(result==null){
			result = get();
		}//need to wait for operation to be executed
		assertEquals(Operation.Status.SUCC, result.status());
		assertEquals(dataItem.size(), createTask.countBytesDone());


		final URI controllerURI = URI.create("tcp://"+getConfig().listVal("storage-net-node-addrs").get(0)+":"+String.valueOf(getConfig().intVal("storage-net-node-port")));
		final String readerGroup = UUID.randomUUID().toString().replace("-", "");
		final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
				.stream(Stream.of(scope, streamName))
				.build();
		try (final ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
			readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
		}

		try (final ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
			 EventStreamReader<ByteBuffer> reader = clientFactory.createReader("reader",
					 readerGroup,
			new ByteBufferSerializer(),
					 ReaderConfig.builder().build())) {
			System.out.format("Reading all the events from %s/%s%n", scope, streamName);
			EventRead<ByteBuffer> event = null;
			event = reader.readNextEvent(readTimeoutMillis);
			if (event.getEvent() != null) {

				assertEquals("we didn't read the same size we had put into stream",
				(int)dataItem.size(),event.getEvent().remaining());
			}
		}
		//assertEquals(dataItem.size(),pravegaStream.getSize());
		//how to get size of the stream to check that its size == dataItem.size() ?
	}

	@Test
	public final void testCopyFile()
	throws Exception {
	}

	@Test
	@Ignore
	public final void testConcatFile()
	throws Exception {
	}

	@Test
	public final void testReadFullFile()
	throws Exception {
	}

	@Test
	public final void testReadFixedRangesFile()
	throws Exception {
	}

	@Test
	public final void testReadRandomRangesFile()
	throws Exception {
	}

	@Test
	public final void testOverwriteFile()
	throws Exception {
	}

	@Test
	public final void testAppendFile()
	throws Exception {
	}

	@Test
	public final void testDeleteFile()
	throws Exception {
	}
}
