PreconditionLoad
	.config({
		"item" : {
			"output" : {
				"path" : ITEM_PATH
			}
		},
		"load" : {
        	"op" : {
        	    "limit": {
        	        "count": OP_LIMIT
        	    }
        	}
        }
	})
	.run();

ReadLoad
	.config({
		"item" : {
			"input" : {
				"path" : ITEM_PATH
			}
		},
		"load" : {
			"op" : {
				"recycle" : true
			},
			"step": {
			    "limit": {
			        "time": TIME_LIMIT
			}
			}
		}
	})
	.run();
