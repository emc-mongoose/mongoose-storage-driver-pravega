package com.emc.mongoose.storage.driver.pravega.integration;

import com.emc.mongoose.storage.driver.pravega.util.docker.PravegaNodeContainer;

import io.pravega.client.admin.StreamManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class PravegaScopeTest {

	private StreamManager streamManager;
	private static PravegaNodeContainer PRAVEGA_NODE_CONTAINER;

	@BeforeClass
	public static void setUpClass()
			throws Exception {
		try {
			PRAVEGA_NODE_CONTAINER = new PravegaNodeContainer();
		} catch(final Exception e) {
			throw new AssertionError(e);
		}
	}

	@AfterClass
	public static void tearDownClass()
			throws Exception {
		PRAVEGA_NODE_CONTAINER.close();
	}

	@Before
	public void initTest(){
		streamManager = StreamManager.create(URI.create("tcp://127.0.0.1:9090"));
	}

    @After
	public void closeTest(){
		streamManager.close();
	}

	@Test
	public void testNewScopeCreation()
			throws Exception {
		assertTrue("Can't create a new scope", streamManager.createScope("TestScope"));
		streamManager.deleteScope("TestScope");
	}

	@Test
	public void testScopeCreationWithSameName()
			throws Exception {
		streamManager.createScope("TestScope");
		assertFalse("The scope shouldn't have been new", streamManager.createScope("TestScope"));
		streamManager.deleteScope("TestScope");
	}

	@Test
	public void testScopeDeletion()
			throws Exception {
		streamManager.createScope("TestScope");
		assertTrue("Can't delete scope", streamManager.deleteScope("TestScope"));
	}

}
