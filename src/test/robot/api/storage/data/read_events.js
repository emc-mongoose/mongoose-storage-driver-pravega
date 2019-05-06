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
                "path" : SCOPE_NAME
            }
	}})
	.run();

ReadLoad
    .config(sharedConfig)
	.config({
	    "item" : {
	        "input" : {
	            "path" : SCOPE_NAME
	        }
	    },   
	    "load" : {
	        "op" : {
	            "type" : "read"
	        }
	    }
	})
	.run();
