PreconditionLoad
	.config({
		"item" : {
			"output" : {
				"path" : ITEM_PATH
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
			}
		}
	})
	.run();
