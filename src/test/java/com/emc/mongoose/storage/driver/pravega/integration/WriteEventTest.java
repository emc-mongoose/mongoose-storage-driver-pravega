package com.emc.mongoose.storage.driver.pravega.integration;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ByteArraySerializer;
import lombok.val;
import org.junit.Test;

import java.net.URI;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertNull;

public class WriteEventTest {

    @Test
    public void test()
    throws Exception {
        val controllerUri = new URI("tcp://10.233.105.4:9090");
        val clientConfig = ClientConfig
                .builder()
                .controllerURI(controllerUri)
                .build();
        try(val streamMgr = StreamManager.create(clientConfig)) {
            streamMgr.createScope("scope1");
            streamMgr.createStream("scope1", "stream1", StreamConfiguration.builder().build());
        }
        val serializer = new ByteArraySerializer();
        val evtWriterConfig = EventWriterConfig
                .builder()
                .build();
        val data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        try(val clientFactory = EventStreamClientFactory.withScope("scope1", clientConfig)) {
            try(val evtWriter = clientFactory.createEventWriter("stream1", serializer, evtWriterConfig)) {
                assertNull(
                        evtWriter
                                .writeEvent(data)
                                .handle(
                                        (v, thrown) -> {
                                            assertNull(thrown);
                                            return null;
                                        }
                                )
                                .get(10, SECONDS)
                );
            }
        }
    }
}