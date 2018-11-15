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
            }
        }
    )
    .run();