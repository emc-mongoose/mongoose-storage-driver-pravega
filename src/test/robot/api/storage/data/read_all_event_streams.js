var sharedConfig = {
	"storage": {
		"namespace": SCOPE_NAME
	},
	"load": {
		 "op": {
			"limit": {
				"rate": 2000
			}
		 }
	},
	"item": {
		"data": {
			"size": "1KB"
		}
	}
}

PreconditionLoad
	.config(sharedConfig)
	.run();

ReadLoad
	.config(sharedConfig)
	.config({
		"item" : {
			"input" : {
				"path" : SCOPE_NAME
			}
		},
		"load": {
			"op": {
				"recycle": true
			}
		}
	})
	.run();