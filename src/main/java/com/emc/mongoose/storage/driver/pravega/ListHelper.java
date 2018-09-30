package com.emc.mongoose.storage.driver.pravega;

import com.emc.mongoose.item.DataItemFactory;
import com.emc.mongoose.item.Item;
import com.emc.mongoose.item.ItemFactory;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public interface ListHelper {

	static <I extends Item> List<I> list(
			final ItemFactory<I> itemFactory, final String path, final String prefix, final int idRadix,
			final I lastPrevItem, final int count
	) throws IOException {

		final Iterator it = null;
		final List<I> items = new ArrayList<>(count);
		final int prefixLen = prefix == null ? 0 : prefix.length();

		final String lastPrevItemName;
		boolean lastPrevItemNameFound;
		if(lastPrevItem == null) {
			lastPrevItemName = null;
			lastPrevItemNameFound = true;
		} else {
			lastPrevItemName = lastPrevItem.name();
			lastPrevItemNameFound = false;
		}

		long listedCount = 0;

		Object lfs;
		String nextPathStr = null;
		String nextName = null;
		long nextSize;
		long nextId;
		I nextFile;

		while(it.hasNext() && listedCount < count) {


		}

		return items;
	}
}
