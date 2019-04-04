PreconditionLoad
	.config({
		"item" : {
			"type" : "data",
			"output" : {
				"path" : "test"
			}
	},
	    "load" : {
        	"op" : {
            	"limit" : {
                	"count" : 5
            	}
        }
    }
	})
	.run();

ReadLoad
	.config({
	    "item" : {
	        "type" : "data",
	        "input" : {
	            "path" : "test"
	        }
	    },   
	    "load" : {
	        "op" : {
	            "type" : "read",
	            "limit" : {
	                "count" : 5
	            }
	        }
	    }
	})
	.run();