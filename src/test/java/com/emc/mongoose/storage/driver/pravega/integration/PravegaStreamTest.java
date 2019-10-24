package com.emc.mongoose.storage.driver.pravega.integration;

import com.emc.mongoose.storage.driver.pravega.util.PravegaNode;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import org.junit.*;

import java.net.URI;

import static org.junit.Assert.assertTrue;

@Ignore
public class PravegaStreamTest {

	private StreamManager streamManager;
	private final URI controllerURI = URI.create("tcp://" + PravegaNode.addr() + ":" + PravegaNode.PORT);
	private final String scopeName = "TestScope";
	private final StreamConfiguration streamConfig = StreamConfiguration.builder()
					.scalingPolicy(ScalingPolicy.fixed(1))
					.build();

	@Before
	public void initTest() {
		streamManager = StreamManager.create(controllerURI);
		streamManager.createScope(scopeName);
	}

	@After
	public void closeTest() {
		streamManager.deleteScope(scopeName);
		streamManager.close();
	}

	@Test
	public void testCreateStream()
					throws Exception {
		final String streamNameTmp = new Object() {}.getClass().getEnclosingMethod().getName();

		assertTrue("Can't create a new stream",
						streamManager.createStream(scopeName, streamNameTmp, streamConfig));
		streamManager.sealStream(scopeName, streamNameTmp);
		streamManager.deleteStream(scopeName, streamNameTmp);
	}

	@Test
	public void testSealStream()
					throws Exception {
		final String streamNameTmp = new Object() {}.getClass().getEnclosingMethod().getName();

		streamManager.createStream(scopeName, streamNameTmp, streamConfig);
		assertTrue("Can't seal a stream", streamManager.sealStream(scopeName, streamNameTmp));
		streamManager.deleteStream(scopeName, streamNameTmp);
	}

	@Test
	public void testDeleteStream()
					throws Exception {
		final String streamNameTmp = new Object() {}.getClass().getEnclosingMethod().getName();

		streamManager.createStream(scopeName, streamNameTmp, streamConfig);
		streamManager.sealStream(scopeName, streamNameTmp);
		assertTrue("Can't delete a stream", streamManager.deleteStream(scopeName, streamNameTmp));
	}

}
