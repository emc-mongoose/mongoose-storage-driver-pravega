var concurrencyLimits = [
	1, 10, 20, 30, 40, 50
]
var eventPayloadSize = 1000
var rateLimit = 300000
var timeLimitPerStep = "100s"

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
			"op" : {
				"limit" : {
					"rate" : rateLimit
				}
			},
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
				"type" : "pravega"
			}
		}
	}
}

for(int i = 0; i < concurrencyLimits.lenght; i ++) {
	concurrencyLimit = concurrencyLimits[i]
	print("Run the load step using the concurrency limit = " + limitConcurrency)
	var stepId = "pravega_scenario_1_concurrency_" + concurrencyLimit
	Load
		.config(writeEventsLoadStepConfig(concurrencyLimit))
		.run();
}
