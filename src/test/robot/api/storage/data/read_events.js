var sharedConfig = {
    "storage": {
        "namespace": SCOPE_NAME
    }
}

PreconditionLoad
	.config(sharedConfig)
	.config({
	    "load" : {
        	"op" : {
            	"limit" : {
                	"count" : 1000
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
