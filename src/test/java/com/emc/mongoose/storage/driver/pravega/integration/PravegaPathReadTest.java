package com.emc.mongoose.storage.driver.pravega.integration;

import com.emc.mongoose.storage.driver.pravega.util.PravegaNode;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URI;
import java.util.UUID;

import static org.junit.Assert.*;

public class PravegaPathReadTest {

	//we put three strings into the stream, then we read it and check if we've read them in the
	//correct order and that there is nothing else in the stream.
	@Test
	public void testPathRead()
			throws Exception {
		final String scope = "PathReadTestScope";
		final String streamName = "PathReadTestStream";
		final var controllerURI = URI.create("tcp://" + PravegaNode.addr() + ":" + PravegaNode.PORT);
		final String routingKey = "RoutingKey";
		final String message1 = "message1";
		final String message2 = "message2";
		final String message3 = "message3";
		final int readerTimeoutMs = 100;
		final StreamManager streamManager = StreamManager.create(controllerURI);
		final boolean scopeIsNew = streamManager.createScope(scope);
		/*writer*/
		StreamConfiguration streamConfig = StreamConfiguration.builder()
				.scalingPolicy(ScalingPolicy.fixed(1))
				.build();
		final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);

		try (final ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
			 EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
					 new JavaSerializer<String>(),
					 EventWriterConfig.builder().build())) {
			writer.writeEvent(routingKey, message1);
			writer.writeEvent(routingKey, message2);
			writer.writeEvent(routingKey, message3);
			System.out.format("Writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n",
					message1, routingKey, scope, streamName);
			System.out.format("Writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n",
					message2, routingKey, scope, streamName);
			System.out.format("Writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n",
					message3, routingKey, scope, streamName);
		}
		/*end of writer*/
		/*start of reader*/
		//it's random for now, but to use offsets we'll need to use the same readerGroup name.
		final String readerGroup = UUID.randomUUID().toString().replace("-", "");
		final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
				.stream(Stream.of(scope, streamName))
				.build();
		try (final ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
			readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
		}

		try (final ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
			 EventStreamReader<String> reader = clientFactory.createReader("reader",
					 readerGroup,
					 new JavaSerializer<String>(),
					 ReaderConfig.builder().build())) {
			System.out.format("Reading all the events from %s/%s%n", scope, streamName);
			EventRead<String> event = null;
			event = reader.readNextEvent(readerTimeoutMs);
			if (event.getEvent() != null) {
				System.out.format("Read event '%s'%n", event.getEvent());
				assertEquals("we didn't read the event string we had put into stream",
						"message1", event.getEvent());
			}
			event = reader.readNextEvent(readerTimeoutMs);
			if (event.getEvent() != null) {
				System.out.format("Read event '%s'%n", event.getEvent());
				assertEquals("we didn't read the event string we had put into stream",
						"message2", event.getEvent());
			}
			event = reader.readNextEvent(readerTimeoutMs);
			if (event.getEvent() != null) {
				System.out.format("Read event '%s'%n", event.getEvent());
				assertEquals("we didn't read the event string we had put into stream",
						"message3", event.getEvent());
			}
			event = reader.readNextEvent(readerTimeoutMs);
			assertNull("there should't be anything else in the stream", event.getEvent());
			System.out.format("No more events from %s/%s%n", scope, streamName);
		}
		streamManager.deleteScope("TestScope");
	}
}
