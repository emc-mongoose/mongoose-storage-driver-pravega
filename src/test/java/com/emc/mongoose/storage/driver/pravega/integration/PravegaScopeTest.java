package com.emc.mongoose.storage.driver.pravega.integration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.emc.mongoose.storage.driver.pravega.util.PravegaNode;
import io.pravega.client.admin.StreamManager;
import java.net.URI;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PravegaScopeTest {

  private StreamManager streamManager;

  @Before
  public void initTest() {
    streamManager =
        StreamManager.create(URI.create("tcp://" + PravegaNode.addr() + ":" + PravegaNode.PORT));
  }

  @After
  public void closeTest() {
    streamManager.close();
  }

  @Test
  public void testNewScopeCreation() throws Exception {
    assertTrue("Can't create a new scope", streamManager.createScope("TestScope"));
    streamManager.deleteScope("TestScope");
  }

  @Test
  public void testScopeCreationWithSameName() throws Exception {
    streamManager.createScope("TestScope");
    assertFalse("The scope shouldn't have been new", streamManager.createScope("TestScope"));
    streamManager.deleteScope("TestScope");
  }

  @Test
  public void testScopeDeletion() throws Exception {
    streamManager.createScope("TestScope");
    assertTrue("Can't delete scope", streamManager.deleteScope("TestScope"));
  }
}
