PreconditionLoad
	.config({
		"item" : {
			"type" : "data",
			"output" : {
				"path" : "test"
			},
            "data" : {
                "size" : "1000KB"
            }
		},
	    "load" : {
        	"op" : {
            	"limit" : {
                	"count" : 1000
            	}
        	}
    	},
        "storage" : {
            "namespace" : "goose"
		}
	})
	.run();

ReadLoad
	.config({
	    "item" : {
	        "type" : "data",
	        "input" : {
	            "path" : "test"
	        },
            "data" : {
                "size" : "1000KB"
            }
	    },   
	    "load" : {
	        "op" : {
	            "type" : "read",
	            "limit" : {
	                "count" : 1000
	            }
	        }
	    },
        "storage" : {
            "namespace" : "goose"
        }
	})
	.run();
