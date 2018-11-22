var sharedConfig = {
	"item" : {
		"output" : {
			"path" : STREAM_NAME // NOTE: requires to set the environment variable "STREAM_NAME" to a specific value
		}
	},
	"storage" : {
		"driver" : {
			"type" : "pravega"
		},
		"net" : {
			"node" : {
				"port" : 9090
			}
		}
	}
}

function scaleConfig(segmentCount) {
	return {
		"storage" : {
			"driver" : {
				"scaling" : {
					"segments" : segmentCount
				}
			}
		}
	}
}

var writeEventsStep = Load
	.config(sharedConfig)
	.config(scaleConfig(1))
	.config(
		{
			"load" : {
				"step" : {
					"id" : "pravega_manual_scaling"
				}
			}
		}
	)

var segmentCountsToScale = [
	10, 20, 50, // scale up at 1st
	20, 10, 1, // then scale down
]

var fakeLoadConfig = {
	"item" : {
		"data" : {
			"size" : 1
		}
	},
	"load" : {
		"op" : {
			"limit" : {
				"count" : 1
			}
		}
	},
	"storage" : {
		"driver" : {
			"limit" : {
				"concurrency" : 1
			}
		}
	}
}

var cmdRunPravega = new java.lang.ProcessBuilder()
	.command("sh", "-c", "docker run -d --name pravega_standalone --network host pravega/pravega standalone")
	.start();
print("Run the Pravega standalone...")
cmdRunPravega.waitFor()
java.util.concurrent.TimeUnit.SECONDS.sleep(10)
print("OK")

writeEventsStep.start()

for(var i = 0; i < segmentCountsToScale.length; i ++) {
	java.util.concurrent.TimeUnit.MINUTES.sleep(1)
	var segmentCountToScale = segmentCountsToScale[i]
	Load
		.config(sharedConfig)
		.config(scaleConfig(segmentCountToScale))
		.config(fakeLoadConfig)
		.run()
}

java.util.concurrent.TimeUnit.MINUTES.sleep(1)

writeEventsStep.close()

var cmdStopPravega = new java.lang.ProcessBuilder()
	.command("sh", "-c", "docker stop pravega_standalone")
	.start()
print("Stop the Pravega container...")
cmdStopPravega.waitFor();
print("OK")

var cmdRemovePravegaContainer = new java.lang.ProcessBuilder()
	.command("sh", "-c", "docker rm pravega_standalone")
	.start()
print("Remove the Pravega container...")
cmdRemovePravegaContainer.waitFor();
print("OK")
