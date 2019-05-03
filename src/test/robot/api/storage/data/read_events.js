var sharedConfig = {
    "storage": {
        "namespace": SCOPE_NAME
    }
}

PreconditionLoad
	.config(sharedConfig)
	.config({})
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
