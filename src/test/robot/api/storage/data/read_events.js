PreconditionLoad
	.config({
        "item" : {
            "output" : {
                "path" : STREAM_NAME
            },
            "load" : {
                "op" : {
                    "limit" : {
                        "count" : COUNT_LIMIT
                    }
                }
            }
	}})
	.run();

ReadLoad
	.config({
	    "item" : {
	        "input" : {
	            "path" : STREAM_NAME
	        }
	    },   
	    "load" : {
	        "op" : {
	            "type" : "read"
	        }
	    }
	})
	.run();
