PreconditionLoad
	.config({
		"item" : {
			"output" : {
				"file" : ITEM_LIST_FILE
			}
		}
	})
	.run();

ReadLoad
	.config({
		"item" : {
			"input" : {
				"file" : ITEM_LIST_FILE
			}
		},
		"load" : {
			"op" : {
				"recycle" : true
			}
		}
	})
	.run();
