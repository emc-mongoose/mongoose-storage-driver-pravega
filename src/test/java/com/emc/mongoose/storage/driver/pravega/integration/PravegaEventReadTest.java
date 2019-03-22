package com.emc.mongoose.storage.driver.pravega.integration;

import com.emc.mongoose.storage.driver.pravega.util.PravegaNode;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.val;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PravegaEventReadTest {

	@Test
	public void testEventRead()
			throws Exception {
		/* writing */
		val scopeName = "Scope";
		val streamName = "Stream";
		val controllerURI = URI.create("tcp://" + PravegaNode.addr() + ":" + PravegaNode.PORT);
		System.out.println("Controller uri: " + controllerURI);
		val routingKey = "RoutingKey";
		val testEvent = "TestEvent";
		val readerTimeoutMs = 100;
		System.out.println("Stream manager creating...");
		val streamManager = StreamManager.create(controllerURI);
		System.out.println("Stream manager created");
		val scopeIsNew = streamManager.createScope(scopeName);
		System.out.println("Scope created");
		val streamConfig = StreamConfiguration.builder()
			.scalingPolicy(ScalingPolicy.fixed(1))
			.build();
		System.out.println("Stream config created");
		streamManager.createStream(scopeName, streamName, streamConfig);
		System.out.println("Stream created");
		try(
			val clientFactory = ClientFactory.withScope(scopeName, controllerURI);
			val writer = clientFactory.createEventWriter(
				streamName, new JavaSerializer<>(), EventWriterConfig.builder().build()
			)
		) {
			System.out.println("Writing the event...");
			final var future = writer.writeEvent(routingKey, testEvent);
			future.handle(
				(v, t) -> {
					System.out.println("Writing the event: " + v + ", " + t);
					return future;
				}
			);
			while(true) {
				if(future.isCancelled()) {
					System.out.println("Writing the event cancelled");
					break;
				}
				if(future.isCompletedExceptionally()) {
					System.out.println("Writing the event completed exceptionally");
					break;
				}
				if(future.isDone()) {
					System.out.println("Writing the event done");
					break;
				}
				TimeUnit.SECONDS.sleep(5);
			}
			System.out.format(
				"Writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n", testEvent, routingKey, scopeName,
				streamName
			);
		} catch(final Throwable thrown) {
			thrown.printStackTrace(System.err);
		}
		/*end of writing*/

		/*reading*/
		val readerGroup = UUID.randomUUID().toString().replace("-", "");
		val readerGroupConfig = ReaderGroupConfig.builder()
			.stream(Stream.of(scopeName, streamName))
			.build();
		try(val readerGroupManager = ReaderGroupManager.withScope(scopeName, controllerURI)) {
			readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
		}
		try(
			val clientFactory = ClientFactory.withScope(scopeName, controllerURI);
			val reader = clientFactory.createReader(
				"reader", readerGroup, new JavaSerializer<>(), ReaderConfig.builder().build()
			)
		) {
			val event1 = reader.readNextEvent(readerTimeoutMs);
			if(event1.getEvent() != null) {
				System.out.format("Read event '%s'%n", event1.getEvent());
				assertEquals(
					"we didn't read the event string we had put into stream", "TestEvent", event1.getEvent()
				);
			}
			val event2 = reader.readNextEvent(readerTimeoutMs);
			assertNull("there should't be anything else in the stream", event2.getEvent());
			System.out.format("No more events from %s/%s%n", scopeName, streamName);
		}
	}

}
