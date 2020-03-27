var sharedConfig = {
	"storage": {
		"namespace": SCOPE_NAME
	}
}
PreconditionLoad
	.config(sharedConfig)
	.config({
		"item" : {
			"output" : {
				"path" : ITEM_PATH
			},
			"data": {
				"size": "1KB"
			}
		},
		"load": {
			"op": {
				"limit": {
					"rate": 200
				}
		 	}
		}
	})
	.run();

ReadLoad
	.config(sharedConfig)
	.config({
		"item" : {
			"input" : {
				"path" : ITEM_PATH
			}
		},
		"load": {
			"op": {
				"limit": {
					"rate": 200
				}
		 	}
		}
	})
	.run();