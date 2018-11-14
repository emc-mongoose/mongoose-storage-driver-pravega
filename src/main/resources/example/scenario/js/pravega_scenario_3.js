var concurrencyLimits = [
	1, 10, 20, 30, 40, 50
]
var eventPayloadSize = 1000
var timeLimitPerStep = "1m"

function writeEventsLoadStepConfig(c) {
	return {
		"item" : {
			"data" : {
				"size" : eventPayloadSize
			},
			"output" : {
				"path" : "stream-" + c
			}
		},
		"load" : {
			"step" : {
				"id" : stepId,
				"limit" : {
					"time" : timeLimitPerStep
				}
			}
		},
		"storage" : {
			"driver" : {
				"limit" : {
					"concurrency" : c
				},
				"scaling" : {
					"segments" : 50
				},
				"type" : "pravega"
			},
			"net" : {
				"node" : {
					"port" : 9090
				}
			}
		}
	}
}

var cmdRunPravega = new java.lang.ProcessBuilder()
	.command("sh", "-c", "docker run -d --name pravega_standalone --network host pravega/pravega standalone")
	.start();
print("Run the Pravega standalone...")
cmdRunPravega.waitFor();
print("OK")

print("Run the test...")
for(var i = 0; i < concurrencyLimits.length; i ++) {
	concurrencyLimit = concurrencyLimits[i]
	print("Run the load step using the concurrency limit = " + concurrencyLimit)
	var stepId = "pravega_scenario_3_concurrency_" + concurrencyLimit
	Load
		.config(writeEventsLoadStepConfig(concurrencyLimit))
		.run();
}

var cmdStopPravega = new java.lang.ProcessBuilder()
	.command("sh", "-c", "docker stop pravega_standalone")
	.inheritIO()
	.start();
print("Stop the Pravega container...")
cmdStopPravega.waitFor();
print("OK")
var cmdRemovePravegaContainer = new java.lang.ProcessBuilder()
	.command("sh", "-c", "docker rm pravega_standalone")
	.inheritIO()
	.start();
print("Remove the Pravega container...")
cmdRemovePravegaContainer.waitFor();
print("OK")
