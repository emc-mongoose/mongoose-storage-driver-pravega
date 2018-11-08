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

import com.emc.mongoose.storage.driver.pravega.PravegaStorageDriver;
import com.emc.mongoose.storage.driver.pravega.util.docker.PravegaNodeContainer;
import com.github.akurilov.commons.collection.Range;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;


import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.emc.mongoose.Constants.APP_NAME;
import static com.emc.mongoose.Constants.MIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataOperationsTest {
	private final PravegaStorageDriver pravegaStorageDriver;
	private static final DataInput DATA_INPUT;

	static {
		try {
			DATA_INPUT = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 16);
		} catch (final IOException e) {
			throw new AssertionError(e);
		}
	}

	private static final Credential CREDENTIAL = Credential.getInstance("root", "nope");
	private static PravegaNodeContainer PRAVEGA_NODE_CONTAINER;

	private static Config getConfig() {
		try {
			final List<Map<String, Object>> configSchemas = Extension
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
			final Map<String, Object> configSchema = TreeUtil.reduceForest(configSchemas);
			final Config config = new BasicConfig("-", configSchema);

			config.val("load-batch-size", 4096);

			config.val("storage-net-reuseAddr", true);
			config.val("storage-net-bindBacklogSize", 0);
			config.val("storage-net-keepAlive", true);
			config.val("storage-net-rcvBuf", 0);
			config.val("storage-net-sndBuf", 0);
			config.val("storage-net-ssl", false);
			config.val("storage-net-tcpNoDelay", false);
			config.val("storage-net-interestOpQueued", false);
			config.val("storage-net-linger", 0);
			config.val("storage-net-timeoutMilliSec", 0);
			config.val("storage-net-node-addrs", Collections.singletonList("127.0.0.1"));
			config.val("storage-net-node-port", PravegaNodeContainer.PORT);
			config.val("storage-net-node-connAttemptsLimit", 0);

			config.val("storage-item-input-readerTimeout", 100);

			config.val("storage-auth-uid", CREDENTIAL.getUid());
			config.val("storage-auth-token", null);
			config.val("storage-auth-secret", CREDENTIAL.getSecret());


			config.val("storage-driver-threads", 0);
			config.val("storage-driver-limit-queue-input", 1_000_000);
			config.val("storage-driver-limit-queue-output", 1_000_000);
			config.val("storage-driver-limit-concurrency", 0);
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

		pravegaStorageDriver = null;/*new PravegaStorageDriver<DataItem, DataOperation<DataItem>>(
				"tcp://127.0.0.1:9090", "test-data-pravega-driver", DATA_INPUT,
				config.configVal("storage"), true, config.configVal("load").intVal("batch-size")
		);*/
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
	public final void testCreateFile()
			throws Exception {
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
