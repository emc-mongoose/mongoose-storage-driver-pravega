package com.emc.mongoose.storage.driver.pravega.util;

public interface PravegaNode {

	int PORT = 9090;

	static String addr() {
		/*final boolean ciFlag = null != System.getenv("CI");
		if(ciFlag) {
			return "pravega";
		} else {*/
			return "localhost";
		//}
	}
}
