var sharedConfig = {
	"namespace": SCOPE_NAME
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