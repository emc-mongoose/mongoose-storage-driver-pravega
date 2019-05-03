package com.emc.mongoose.storage.driver.pravega.cache;

import java.util.function.Function;

/**
 A function to create the reader group using the scope name as the function argument
 */

public interface ReaderGroupCreateFunction
				extends Function<String, String> {

        /**
         @param readerGroupName the reader group name
         @return the created reader group
         */

        @Override
        String apply (final String readerGroupName);
    }

