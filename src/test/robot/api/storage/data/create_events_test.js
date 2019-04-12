CreateLoad
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
	            "type" : "create",
	            "limit" : {
	                "count" : 10
	            }
	        }
	    },
	    "storage" : {
	    	"namespace" : "goose"
	    }
	})
	.run();
