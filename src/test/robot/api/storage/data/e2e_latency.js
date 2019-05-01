var sharedConfig = {
	"storage": {
		"driver": {
			"type": "pravega"
		}
	},
	"output": {
		"metrics": {
			"trace": {
				"persist": true
			}
		}
	}
}

var readConfig = {
	"load": {
		"op": {
			"type": "read"
		}
	},
	"storage": {
		"driver": {
			"event": {
				"timeoutMillis": 2000000000
			}
		}
	}
}

PipelineLoad
	.config(sharedConfig)
	.append({})
	.append(readConfig)
	.run()
