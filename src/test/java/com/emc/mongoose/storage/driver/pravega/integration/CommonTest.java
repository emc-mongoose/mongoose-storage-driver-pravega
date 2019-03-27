package com.emc.mongoose.storage.driver.pravega.integration;

import static com.emc.mongoose.base.Constants.APP_NAME;
import static org.junit.Assert.fail;

import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.env.Extension;
import com.emc.mongoose.base.storage.Credential;
import com.emc.mongoose.storage.driver.pravega.PravegaStorageDriver;
import com.emc.mongoose.storage.driver.pravega.util.PravegaNode;
import com.emc.mongoose.storage.driver.pravega.util.docker.PravegaNodeContainer;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;

import java.io.IOException;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.val;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CommonTest {

	private final PravegaStorageDriver pravegaStorageDriver;
	private static final DataInput DATA_INPUT;

	static {
		try {
			DATA_INPUT = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 16);
		} catch(final IOException e) {
			throw new AssertionError(e);
		}
	}

	private static final Credential CREDENTIAL = Credential.getInstance("root", "nope");

	private static Config getConfig() {
		try {
			val configSchemas = Extension.load(Thread.currentThread().getContextClassLoader()).stream().map(
				Extension::schemaProvider).filter(Objects::nonNull).map(schemaProvider -> {
				try {
					return schemaProvider.schema();
				} catch(final Exception e) {
					fail(e.getMessage());
				}
				return null;
			}).filter(Objects::nonNull).collect(Collectors.toList());
			SchemaProvider.resolve(
				APP_NAME, Thread.currentThread().getContextClassLoader()).stream().findFirst().ifPresent(
				configSchemas::add);
			val configSchema = TreeUtil.reduceForest(configSchemas);
			val config = new BasicConfig("-", configSchema);
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
			config.val("storage-net-timeoutMillis", 0);
			config.val("storage-net-node-addrs", PravegaNode.addr());
			config.val("storage-net-node-port", PravegaNode.PORT);
			config.val("storage-net-node-connAttemptsLimit", 0);
			config.val("storage-net-uri-schema", "tcp");
			config.val("storage-auth-uid", CREDENTIAL.getUid());
			config.val("storage-auth-token", null);
			config.val("storage-auth-secret", CREDENTIAL.getSecret());
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
			config.val("storage-driver-limit-concurrency", 1);
			config.val("storage-driver-namespace-scope", "goose");
			return config;
		} catch(final Throwable cause) {
			throw new RuntimeException(cause);
		}
	}

	public CommonTest() {
		this(getConfig());
	}

	private CommonTest(final Config config) {
		pravegaStorageDriver =
			new PravegaStorageDriver("test-data-pravega-driver", DATA_INPUT, config.configVal("storage"), true,
				config.configVal("load").intVal("batch-size")
			);
	}

	@Test
	public final void testExample()
	throws Exception {
	}
}
