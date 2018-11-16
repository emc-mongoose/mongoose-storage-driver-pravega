package com.emc.mongoose.storage.driver.pravega;

import static com.emc.mongoose.Constants.APP_NAME;
import static com.emc.mongoose.storage.driver.pravega.PravegaConstants.DRIVER_NAME;
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

public final class PravegaStorageDriverExtension<
	I extends Item, O extends Operation<I>, T extends PravegaStorageDriver<I, O>
>
extends ExtensionBase
implements StorageDriverFactory<I, O, T> {

	private static final SchemaProvider SCHEMA_PROVIDER = new JsonSchemaProviderBase() {

		@Override
		protected final InputStream schemaInputStream() {
			return getClass().getResourceAsStream("/config-schema-storage-" + DRIVER_NAME + ".json");
		}

		@Override
		public final String id() {
			return APP_NAME;
		}
	};

	private static final String DEFAULTS_FILE_NAME = "defaults-storage-" + DRIVER_NAME + ".json";

	private static final List<String> RES_INSTALL_FILES = Collections.unmodifiableList(
		Arrays.asList(
			"config/" + DEFAULTS_FILE_NAME,
			"example/scenario/js/pravega/increase_load.js",
			"example/scenario/js/pravega/manual_scaling.js",
			"example/scenario/js/pravega/scenario_1.js",
			"example/scenario/js/pravega/scenario_2.js",
			"example/scenario/js/pravega/scenario_3.js"
		)
	);

	@Override
	public final String id() {
		return DRIVER_NAME;
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
		return (T) new PravegaStorageDriver<I, O>(
			stepId, dataInput, storageConfig, verifyFlag, batchSize
		);
	}
}
