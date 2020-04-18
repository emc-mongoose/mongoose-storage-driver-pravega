PreconditionLoad
	.config({
		"item" : {
			"output" : {
				"path" : ITEM_LIST_FILE
			}
		}
	})
	.run();

ReadLoad
	.config({
		"item" : {
			"input" : {
				"path" : ITEM_LIST_FILE
			}
		},
		"load" : {
			"op" : {
				"recycle" : true
			}
		}
	})
	.run();
