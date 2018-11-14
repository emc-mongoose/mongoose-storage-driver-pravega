var concurrencyLimit = 100

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
			"limit" : {
				"concurrency" : concurrencyLimit
			},
			"type" : "pravega"
		}
	}
}

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
							"count" : concurrencyLimit
						}
					}
				}
			}
		}
	)
	.run()
