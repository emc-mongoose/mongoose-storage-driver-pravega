Load
    .config(
        {
            "item" : {
                "data" : {
                    "size" : ITEM_DATA_SIZE
                }
            },
            "load": {
                "op": {
                    "limit": {
                        "count": TEST_STEP_LIMIT_COUNT
                    }
                }
            },
            "output": {
                "metrics": {
                    "average": {
                        "persist": false
                    },
                    "summary": {
                        "persist": false
                    },
                    "trace": {
                        "persist": false
                    }
                }
            }
        }
    )
    .run();