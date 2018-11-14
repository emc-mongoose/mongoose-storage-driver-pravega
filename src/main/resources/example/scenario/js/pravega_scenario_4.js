var additionalSegmentCount = 100

var sharedConfig = {
	"item" : {
		"data" : {
			"size" : 1000
		},
		"output" : {
			"path" : "stream-4"
		}
	},
	"load" : {
		"op" : {
			"limit" : {
				"rate" : 300000
			}
		},
		"step" : {
			"limit" : {
				"time" : 100
			}
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
cmdRunPravega.waitFor();
print("OK")

print("Create the stream and fill it with some events...")
Load
	.config(sharedConfig)
	.config(
		{
			"load" : {
				"step" : {
					"id" : "pravega_scenario_4_create_stream"
				}
			}
		}
	)
	.run()

print("Update the stream from the previous load step to more segments...")
Load
	.config(sharedConfig)
	.config(
		{
			"load" : {
				"step" : {
					"id" : "pravega_scenario_4_update_stream_to_more_segments"
				}
			},
			"storage" : {
				"driver" : {
					"create" : {
						"key" : {
							"count" : additionalSegmentCount
						}
					}
				}
			}
		}
	)
	.run()

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
