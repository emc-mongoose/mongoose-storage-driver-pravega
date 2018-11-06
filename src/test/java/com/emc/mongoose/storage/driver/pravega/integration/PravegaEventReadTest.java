package com.emc.mongoose.storage.driver.pravega.integration;

import com.emc.mongoose.storage.driver.pravega.util.docker.PravegaNodeContainer;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PravegaEventReadTest {
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
	public void testEventRead()
			throws Exception {
		/* writing */
		final String scopeName = "Scope";
		final String streamName = "Stream";
		final URI controllerURI = URI.create("tcp://127.0.0.1:9090");
		final String routingKey = "RoutingKey";
		final String testEvent = "TestEvent";
		final int readerTimeoutMs = 100;
		final StreamManager streamManager = StreamManager.create(controllerURI);
		final boolean scopeIsNew = streamManager.createScope(scopeName);
		StreamConfiguration streamConfig = StreamConfiguration.builder()
				.scalingPolicy(ScalingPolicy.fixed(1))
				.build();
		final boolean streamIsNew = streamManager.createStream(scopeName, streamName, streamConfig);

		try (final ClientFactory clientFactory = ClientFactory.withScope(scopeName, controllerURI);
			 EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
					 new JavaSerializer<String>(),
					 EventWriterConfig.builder().build())) {
			writer.writeEvent(routingKey, testEvent);
			System.out.format("Writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n",
					testEvent, routingKey, scopeName, streamName);
		}
		/*end of writing*/

		/*reading*/
		final String readerGroup = UUID.randomUUID().toString().replace("-", "");
		final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
				.stream(Stream.of(scopeName, streamName))
				.build();
		try (final ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, controllerURI)) {
			readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
		}
		try (final ClientFactory clientFactory = ClientFactory.withScope(scopeName, controllerURI);
			 EventStreamReader<String> reader = clientFactory.createReader("reader",
					 readerGroup,
					 new JavaSerializer<String>(),
					 ReaderConfig.builder().build())) {
			EventRead<String> event = null;
			event = reader.readNextEvent(readerTimeoutMs);
			if (event.getEvent() != null) {
				System.out.format("Read event '%s'%n", event.getEvent());
				assertEquals("we didn't read the event string we had put into stream",
						"TestEvent", event.getEvent());
			}
			event = reader.readNextEvent(readerTimeoutMs);
			assertNull("there should't be anything else in the stream", event.getEvent());
			System.out.format("No more events from %s/%s%n", scopeName, streamName);
		}
	}

}
