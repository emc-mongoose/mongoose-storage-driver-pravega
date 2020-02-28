var sharedConfig = {
	"namespace": SCOPE_NAME
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