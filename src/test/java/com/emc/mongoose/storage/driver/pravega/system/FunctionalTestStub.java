package com.emc.mongoose.storage.driver.pravega.system;


import com.emc.mongoose.storage.driver.pravega.util.EnvUtil;
import com.emc.mongoose.storage.driver.pravega.util.docker.MongooseContainer;
import com.emc.mongoose.storage.driver.pravega.util.docker.PravegaNodeContainer;
import com.github.akurilov.commons.system.SizeInBytes;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static com.emc.mongoose.Constants.MIB;
import static com.emc.mongoose.storage.driver.pravega.util.docker.MongooseContainer.CONTAINER_SHARE_PATH;
import static com.emc.mongoose.storage.driver.pravega.util.docker.MongooseContainer.HOST_SHARE_PATH;

public class FunctionalTestStub {
	private static final String SCENARIO_FILE = null; //"scenario" + File.separator + "*.js";
	/*TODO: add all settings fields*/
//	private static final String ITEM_LIST_FILE = CONTAINER_SHARE_PATH + File.separator + "*.cvs";
//	private static final String ITEM_OUTPUT_PATH = "/" + FunctionalTestStub.class.getSimpleName();
	private static final String STEP_ID = FunctionalTestStub.class.getSimpleName();
	private static final int STEP_LIMIT_COUNT = 1000;
	private static final int RUN_MODE = 1;
	private static final int CONCURRENCY = 10;

	private static PravegaNodeContainer PRAVEGA_NODE_CONTAINER;
	private static MongooseContainer MONGOOSE_CONTAINER;
	private static String STD_OUTPUT;

	@BeforeClass
	public static void setUpClass()
	throws Exception {
		final String resourceScenarioPath = FunctionalTestStub.class
				.getClassLoader()
				.getResource(SCENARIO_FILE)
				.getPath();
		final Path hostScenarioPath = Paths.get(HOST_SHARE_PATH.toString(), SCENARIO_FILE);
		Files.createDirectories(hostScenarioPath.getParent());
		if(Files.exists(hostScenarioPath)) {
			Files.delete(hostScenarioPath);
		}
		Files.copy(Paths.get(resourceScenarioPath), hostScenarioPath);
		final List<String> args = new ArrayList<>();
		args.add("--load-step-id=" + STEP_ID);
		args.add("--run-scenario=" + hostScenarioPath);
		//TODO: add args
		EnvUtil.set("STEP_LIMIT_COUNT", Integer.toString(STEP_LIMIT_COUNT));
		//TODO: EnvUtil

		try {
			PRAVEGA_NODE_CONTAINER = new PravegaNodeContainer();
			MONGOOSE_CONTAINER = new MongooseContainer(args, 1000);
		} catch (final Exception e) {
			throw new AssertionError(e);
		}
		MONGOOSE_CONTAINER.clearLogs(STEP_ID);
		MONGOOSE_CONTAINER.run();
		STD_OUTPUT = MONGOOSE_CONTAINER.getStdOutput();

	}

	@AfterClass
	public static void tearDownClass()
	throws IOException {
		PRAVEGA_NODE_CONTAINER.close();
		MONGOOSE_CONTAINER.close();
	}

	@Test
	public final void test()
	throws Exception {
		//TODO test the results
	}


}