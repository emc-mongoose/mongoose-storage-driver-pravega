package com.emc.mongoose.storage.driver.pravega;

import com.emc.mongoose.data.DataInput;
import com.emc.mongoose.exception.InterruptRunException;
import com.emc.mongoose.exception.OmgShootMyFootException;
import com.emc.mongoose.item.DataItem;
import com.emc.mongoose.item.Item;
import com.emc.mongoose.item.ItemFactory;
import com.emc.mongoose.item.op.OpType;
import com.emc.mongoose.item.op.Operation;
import com.emc.mongoose.logging.LogUtil;
import com.emc.mongoose.logging.Loggers;
import com.emc.mongoose.storage.Credential;
import com.emc.mongoose.storage.driver.coop.CoopStorageDriverBase;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import org.apache.logging.log4j.Level;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PravegaStorageDriver<I extends Item, O extends Operation<I>>
		extends CoopStorageDriverBase<I, O> {

	protected final String uriSchema;

	protected final String[] endpointAddrs;
	protected final int nodePort;
	private final AtomicInteger rrc = new AtomicInteger(0);

	protected int inBuffSize = BUFF_SIZE_MIN;
	protected int outBuffSize = BUFF_SIZE_MAX;

	public PravegaStorageDriver(
			final String uriSchema, final String testStepId, final DataInput dataInput,
			final Config storageConfig, final boolean verifyFlag, final int batchSize
	)
			throws OmgShootMyFootException {
		super(testStepId, dataInput, storageConfig, verifyFlag, batchSize);
		this.uriSchema = uriSchema;
		final String uid = credential == null ? null : credential.getUid();
		final Config nodeConfig = storageConfig.configVal("net-node");
		nodePort = storageConfig.intVal("net-node-port");
		final List<String> endpointAddrList = nodeConfig.listVal("addrs");
		endpointAddrs = endpointAddrList.toArray(new String[endpointAddrList.size()]);
		requestAuthTokenFunc = null; // do not use
		requestNewPathFunc = null; // do not use
	}

	@Override
	protected String requestNewPath(final String path) {
		throw new AssertionError("Should not be invoked");
	}

	@Override
	protected String requestNewAuthToken(final Credential credential) {
		throw new AssertionError("Should not be invoked");
	}

	@Override
	public List<I> list(
			final ItemFactory<I> itemFactory, final String path, final String prefix, final int idRadix,
			final I lastPrevItem, final int count
	)
			throws IOException {
		return null;
	}

	@Override
	public void adjustIoBuffers(final long avgTransferSize, final OpType opType) {
		int size;
		if(avgTransferSize < BUFF_SIZE_MIN) {
			size = BUFF_SIZE_MIN;
		} else if(BUFF_SIZE_MAX < avgTransferSize) {
			size = BUFF_SIZE_MAX;
		} else {
			size = (int) avgTransferSize;
		}
		if(OpType.CREATE.equals(opType)) {
			Loggers.MSG.info("Adjust output buffer size: {}", SizeInBytes.formatFixedSize(size));
			outBuffSize = size;
		} else if(OpType.READ.equals(opType)) {
			Loggers.MSG.info("Adjust input buffer size: {}", SizeInBytes.formatFixedSize(size));
			inBuffSize = size;
		}
	}

	@Override
	protected boolean submit(O op) throws InterruptRunException, IllegalStateException {
		return false;
	}

	@Override
	protected int submit(List<O> ops, int from, int to) throws InterruptRunException, IllegalStateException {
		return 0;
	}

	@Override
	protected int submit(List<O> ops) throws InterruptRunException, IllegalStateException {
		final OpType opType = OpType.NOOP;
		final DataItem fileItem;

		try {
			switch (opType) {
				case NOOP:

					break;
				case CREATE:

					break;
				case READ:

					break;
				case UPDATE:

					break;
				case DELETE:

					break;
				case LIST:
				default:
					throw new AssertionError("\"" + opType + "\" operation isn't implemented");
			}
		} catch (final RuntimeException e) {
			final Throwable cause = e.getCause();

			if (cause instanceof IOException) {
				LogUtil.exception(
						Level.DEBUG, cause, "Failed open the file: {}"
				);
			}  else if (cause != null) {
				LogUtil.exception(Level.DEBUG, cause, "Unexpected failure");
			} else {
				LogUtil.exception(Level.DEBUG, e, "Unexpected failure");
			}
		}

		return 0;
	}

	@Override
	protected void doClose()
			throws IOException {
		super.doClose();
	}

	@Override
	public String toString() {
		return String.format(super.toString(), "pravega");
	}
}
