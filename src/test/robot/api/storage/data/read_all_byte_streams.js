var sharedConfig = {
	"storage": {
		"driver": {
			"stream": {
				"data": "bytes"
			}
		},
		"namespace": SCOPE_NAME
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
		}
	})
	.run();
