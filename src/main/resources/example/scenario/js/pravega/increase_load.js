var timeToAddNewLoadStepSeconds = 30
var loadStepCount = 10

var sharedConfig = {
	"item" : {
		"data" : {
			"size" : 1000
		},
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

var cmdRunPravega = new java.lang.ProcessBuilder()
	.command("sh", "-c", "docker run -d --name pravega_standalone --network host pravega/pravega standalone")
	.start();
print("Run the Pravega standalone...")
cmdRunPravega.waitFor()
java.util.concurrent.TimeUnit.SECONDS.sleep(10)
print("OK")

var loadSteps = [];

for(var i = 0; i < loadStepCount; i ++) {
	var loadStep = Load
		.config(sharedConfig)
		.config(
			{
				"load" : {
					"step" : {
						"id" : "pravega_increase_load_" + STREAM_NAME + "_step_" + i
					}
				},
				"run" : {
					"port" : ~~(10000 + i)
				}
			}
		)
		.start()
	loadSteps[loadSteps.length] = loadStep
	java.util.concurrent.TimeUnit.SECONDS.sleep(timeToAddNewLoadStepSeconds)
}

java.util.concurrent.TimeUnit.MINUTES.sleep(1)

for(var i = 0; loadStepCount; i ++) {
	loadSteps[i].close()
}

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
