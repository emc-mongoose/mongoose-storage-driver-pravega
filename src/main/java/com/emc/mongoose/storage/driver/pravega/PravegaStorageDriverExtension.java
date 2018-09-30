package com.emc.mongoose.storage.driver.pravega;

import com.emc.mongoose.data.DataInput;
import com.emc.mongoose.env.ExtensionBase;
import com.emc.mongoose.exception.OmgShootMyFootException;
import com.emc.mongoose.item.Item;
import com.emc.mongoose.item.op.Operation;
import com.emc.mongoose.storage.driver.StorageDriverFactory;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.io.json.JsonSchemaProviderBase;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.emc.mongoose.Constants.APP_NAME;

public final class PravegaStorageDriverExtension<
		I extends Item, O extends Operation<I>, T extends com.emc.mongoose.storage.driver.pravega.PravegaStorageDriver<I, O>
		>
		extends ExtensionBase
		implements StorageDriverFactory<I, O, T> {

	private static final String NAME = "pravega";

	private static final SchemaProvider SCHEMA_PROVIDER = new JsonSchemaProviderBase() {

		@Override
		protected final InputStream schemaInputStream() {
			return getClass().getResourceAsStream("/config-schema-storage-net.json");
		}

		@Override
		public final String id() {
			return APP_NAME;
		}
	};

	private static final String DEFAULTS_FILE_NAME = "defaults-storage-net.json";

	private static final List<String> RES_INSTALL_FILES = Collections.unmodifiableList(
			Arrays.asList(
					"config/" + DEFAULTS_FILE_NAME
			)
	);

	@Override
	public final String id() {
		return NAME;
	}

	@Override
	protected final String defaultsFileName() {
		return DEFAULTS_FILE_NAME;
	}

	@Override
	public final SchemaProvider schemaProvider() {
		return SCHEMA_PROVIDER;
	}

	@Override
	protected final List<String> resourceFilesToInstall() {
		return RES_INSTALL_FILES;
	}

	@Override
	public T create(
			final String stepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
			final int batchSize
	) throws OmgShootMyFootException, InterruptedException {
		return (T) new com.emc.mongoose.storage.driver.pravega.PravegaStorageDriver<I, O>(
				NAME, stepId, dataInput, storageConfig, verifyFlag, batchSize
		);
	}
}
