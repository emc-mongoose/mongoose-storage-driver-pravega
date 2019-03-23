package com.emc.mongoose.storage.driver.pravega.util;

public interface PravegaNode {

	int PORT = 9090;

	static String addr() {
		final boolean ciFlag = null != System.getenv("CI");
		if(ciFlag) {
			System.out.println("Running under CI, using \"storage\" host for the testing");
			return "storage";
		} else {
			System.out.println("Running under CI, using \"localhost\" for the testing");
			return "localhost";
		}
	}
}
