package com.emc.mongoose.storage.driver.pravega.integration;

import com.emc.mongoose.storage.driver.pravega.util.docker.PravegaNodeContainer;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;

public class PravegaPathCreateTest {
	private static PravegaNodeContainer PRAVEGA_NODE_CONTAINER;

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
	public void testPathCreate()
		throws Exception {
		final String scope = "Scope";
		final String streamName = "Stream";
		final URI controllerURI = URI.create("tcp://127.0.0.1:9090");
		final StreamManager streamManager = StreamManager.create(controllerURI);

		StreamConfiguration streamConfig = StreamConfiguration.builder()
				.scalingPolicy(ScalingPolicy.fixed(1))
				.build();
		
	}
}
