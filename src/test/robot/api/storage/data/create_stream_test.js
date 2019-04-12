CreateLoad
    .config({
        "item" : {
            "output" : {
                "path" : "streamtest"
            },
            "data" : {
                "size" : "1KB"
            }
        },
        "load" : {
            "op" : {
                "limit" : {
                    "count" : 1
                }
            }
        },
        "storage" : {
            "namespace" : "goose"
        }
    })
    .run();
