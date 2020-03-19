package com.emc.mongoose.storage.driver.pravega.integration;

import com.emc.mongoose.storage.driver.pravega.util.PravegaNode;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.val;
import org.junit.Test;

import java.net.URI;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PravegaEventReadTest {

	@Test
	public void testEventRead()
					throws Exception {
		/* writing */
		val scopeName = getClass().getSimpleName() + "Scope";
		val streamName = getClass().getSimpleName() + "Stream";
		val controllerURI = URI.create("tcp://" + PravegaNode.addr() + ":" + PravegaNode.PORT);
		val routingKey = "RoutingKey";
		val testEvent = "TestEvent";
		val readerTimeoutMs = 100;
		val streamManager = StreamManager.create(controllerURI);
		val scopeIsNew = streamManager.createScope(scopeName);
		val streamConfig = StreamConfiguration.builder()
						.scalingPolicy(ScalingPolicy.fixed(1))
						.build();
		streamManager.createStream(scopeName, streamName, streamConfig);
		try (
						val clientFactory = EventStreamClientFactory.withScope(scopeName,
								ClientConfig.builder().controllerURI(controllerURI).build());
						val writer = clientFactory.createEventWriter(
										streamName, new JavaSerializer<>(), EventWriterConfig.builder().build())) {
			writer.writeEvent(routingKey, testEvent);
			System.out.format(
							"Writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n", testEvent, routingKey, scopeName,
							streamName);
		} catch (final Throwable thrown) {
			thrown.printStackTrace(System.err);
		}
		/*end of writing*/

		/*reading*/
		val readerGroup = UUID.randomUUID().toString().replace("-", "");
		val readerGroupConfig = ReaderGroupConfig.builder()
						.stream(Stream.of(scopeName, streamName))
						.build();
		try (val readerGroupManager = ReaderGroupManager.withScope(scopeName, controllerURI)) {
			readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
		}
		try (
				val clientFactory = EventStreamClientFactory.withScope(scopeName,
						ClientConfig.builder().controllerURI(controllerURI).build());
						val reader = clientFactory.createReader(
										"reader", readerGroup, new JavaSerializer<>(), ReaderConfig.builder().build())) {
			val event1 = reader.readNextEvent(readerTimeoutMs);
			if (event1.getEvent() != null) {
				System.out.format("Read event '%s'%n", event1.getEvent());
				assertEquals(
								"we didn't read the event string we had put into stream", "TestEvent", event1.getEvent());
			}
			val event2 = reader.readNextEvent(readerTimeoutMs);
			assertNull("there should't be anything else in the stream", event2.getEvent());
			System.out.format("No more events from %s/%s%n", scopeName, streamName);
		}
	}

}
