PreconditionLoad
	.config({
		"item" : {
			"output" : {
				"file" : ITEM_LIST_FILE
			},
			"data": {
				"size": "1KB"
			}
		},
		"load": {
			"op": {
				"limit": {
					"rate": 2000
				}
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
		"load": {
			"op": {
				"limit": {
					"rate": 2000
				}
		 	}
		}
	})
	.run();
