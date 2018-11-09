package com.emc.mongoose.storage.driver.pravega;

import com.emc.mongoose.data.DataInput;
import com.emc.mongoose.exception.OmgShootMyFootException;
import com.emc.mongoose.item.DataItem;
import com.emc.mongoose.item.Item;
import com.emc.mongoose.item.ItemFactory;
import com.emc.mongoose.item.PathItem;
import com.emc.mongoose.item.op.OpType;
import com.emc.mongoose.item.op.Operation;
import com.emc.mongoose.item.op.data.DataOperation;
import com.emc.mongoose.item.op.path.PathOperation;
import com.emc.mongoose.logging.LogUtil;
import com.emc.mongoose.logging.Loggers;
import com.emc.mongoose.storage.Credential;
import com.emc.mongoose.storage.driver.coop.nio.NioStorageDriverBase;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import org.apache.logging.log4j.Level;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class PravegaStorageDriver<I extends Item, O extends Operation<I>>
		extends NioStorageDriverBase<I, O> {

	protected final String uriSchema;

	protected final String[] endpointAddrs;
	protected final int nodePort;
	private final AtomicInteger rrc = new AtomicInteger(0);


	protected final int readerTimeoutMs;

	protected int inBuffSize = BUFF_SIZE_MIN;
	protected int outBuffSize = BUFF_SIZE_MAX;

	private StreamManager streamManager;
	private Map<String, Map<String, Stream>> scopeMap = new HashMap<>();

	public PravegaStorageDriver(
			final String uriSchema, final String testStepId, final DataInput dataInput,
			final Config storageConfig, final boolean verifyFlag, final int batchSize
	)
			throws OmgShootMyFootException {
		super(testStepId, dataInput, storageConfig, verifyFlag, batchSize);
		this.uriSchema = uriSchema;
		streamManager = StreamManager.create(URI.create(uriSchema));

		final String uid = credential == null ? null : credential.getUid();
		final Config nodeConfig = storageConfig.configVal("net-node");
		nodePort = storageConfig.intVal("net-node-port");
		final List<String> endpointAddrList = nodeConfig.listVal("addrs");
		readerTimeoutMs = storageConfig.intVal("storage-item-input-readerTimeout");

		endpointAddrs = endpointAddrList.toArray(new String[endpointAddrList.size()]);
		requestAuthTokenFunc = null; // do not use
		requestNewPathFunc = null; // do not use
	}

	protected StreamManager getEndpoint(final String nodeAddr) {
		try {
			final String addr;
			final int port;
			int portSepPos = nodeAddr.lastIndexOf(':');
			if (portSepPos > 0) {
				addr = nodeAddr.substring(0, portSepPos);
				port = Integer.parseInt(nodeAddr.substring(portSepPos + 1));
			} else {
				addr = nodeAddr;
				port = nodePort;
			}
			final String uid = credential == null ? null : credential.getUid();
			final URI endpointUri = new URI(uriSchema, uid, addr, port, "/", null, null);
			// set the temporary thread's context classloader
			Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
			return StreamManager.create(endpointUri);
		} catch (final URISyntaxException e) {
			throw new RuntimeException(e);
		} finally {
			// set the thread's context classloader back
			Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
		}
	}

	protected final String getNextEndpointAddr() {
		return endpointAddrs[rrc.getAndIncrement() % endpointAddrs.length];
	}

	@Override
	protected void prepare(final O operation) {
		super.prepare(operation);
		String endpointAddr = operation.nodeAddr();
		if (endpointAddr == null) {
			endpointAddr = getNextEndpointAddr();
			operation.nodeAddr(endpointAddr);
		}
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
		if (avgTransferSize < BUFF_SIZE_MIN) {
			size = BUFF_SIZE_MIN;
		} else if (BUFF_SIZE_MAX < avgTransferSize) {
			size = BUFF_SIZE_MAX;
		} else {
			size = (int) avgTransferSize;
		}
		if (OpType.CREATE.equals(opType)) {
			Loggers.MSG.info("Adjust output buffer size: {}", SizeInBytes.formatFixedSize(size));
			outBuffSize = size;
		} else if (OpType.READ.equals(opType)) {
			Loggers.MSG.info("Adjust input buffer size: {}", SizeInBytes.formatFixedSize(size));
			inBuffSize = size;
		}
	}

	@Override
	protected final void invokeNio(final O operation) {
		if (operation instanceof DataOperation) {
			invokeFileNio((DataOperation<? extends DataItem>) operation);
		} else if (operation instanceof PathOperation) {
			invokeDirectoryNio((PathOperation<? extends PathItem>) operation);
		} else {
			throw new AssertionError("Not implemented");
		}
	}

	private void invokeFileNio(DataOperation<? extends DataItem> fileOperation) {
		final OpType opType = fileOperation.type();
		final DataItem fileItem = fileOperation.item();
		final String path = fileOperation.dstPath();
		final String scopeName = path.substring(0, path.indexOf("/"));
		final String streamName = path.substring(path.indexOf("/") + 1);
		try {
			switch (opType) {
				case NOOP:
					//TODO
					break;
				case CREATE:
					//TODO: EventStreamWriter<ByteBuffer>  ->  writeEvent
					break;
				case READ:
					if ((null != scopeMap.get(scopeName) && null != scopeMap.get(scopeName).get(streamName))) {
						if (invokeDataRead(fileOperation, scopeName, streamName)) {
						}
					} else {
						Loggers.ERR.debug(
								"Failed to read the stream {} in the scope {}", streamName, scopeName);
						fileOperation.status(Operation.Status.RESP_FAIL_UNKNOWN);
					}
					break;
				default:
					throw new AssertionError("Operation " + opType + " isn't supported");
			}

		} catch (final RuntimeException e) {
			final Throwable cause = e.getCause();
			if (cause instanceof IOException) {
				LogUtil.exception(
						Level.DEBUG, cause, "Failed IO"
				);
			} else if (cause != null) {
				LogUtil.exception(Level.DEBUG, cause, "Unexpected failure");
			} else {
				LogUtil.exception(Level.DEBUG, e, "Unexpected failure");
			}
		}
	}

	private void invokeDirectoryNio(PathOperation<? extends PathItem> pathOperation) {
		final OpType opType = pathOperation.type();
		final PathItem pathItem = pathOperation.item();
		try {
			switch (opType) {
				case NOOP:
					//TODO
					break;
				case CREATE:
					//TODO: StreamManager.createStream
					break;
				case READ:
					final String path = pathOperation.dstPath();
					final String scopeName = path.substring(0, path.indexOf("/"));
					final String streamName = path.substring(path.indexOf("/") + 1);
					if ((scopeMap.get(scopeName) == null) || (scopeMap.get(scopeName).get(streamName) == null)) {
						//instead of this check there will be an http request, apparently.
						Loggers.ERR.debug(
								"Failed to delete the stream {} in the scope {}", streamName, scopeName);
						pathOperation.status(Operation.Status.RESP_FAIL_UNKNOWN);
					}
					if (invokePathRead(pathOperation)) {
						System.out.format("Path Op $op was made successfully", pathOperation);
					}
					break;
				case DELETE:
					if (invokePathDelete(pathOperation)) {
					}
					break;
				default:
					throw new AssertionError("Operation " + opType + "  isn't supported");
			}
		} catch (final RuntimeException e) {
			final Throwable cause = e.getCause();
			if (cause instanceof IOException) {
				LogUtil.exception(
						Level.DEBUG, cause, "Failed IO"
				);
			} else if (cause != null) {
				LogUtil.exception(Level.DEBUG, cause, "Unexpected failure");
			} else {
				LogUtil.exception(Level.DEBUG, e, "Unexpected failure");
			}
		}
	}

	private boolean invokeDataRead(final DataOperation<? extends DataItem> dataOp, String scopeName, String streamName) {
		final URI controllerURI = URI.create(uriSchema);

		final String readerGroup = UUID.randomUUID().toString().replace("-", "");
		final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
				.stream(Stream.of(scopeName, streamName))
				.build();
		try (final ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, controllerURI)) {
			readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
		}

		try (final ClientFactory clientFactory = ClientFactory.withScope(scopeName, controllerURI);
		     final EventStreamReader<String> reader = clientFactory.createReader("reader",
				     readerGroup,
				     new JavaSerializer<String>(),
				     ReaderConfig.builder().build())) {
			System.out.format("Reading all the events from %s/%s%n", scopeName, streamName);
			EventRead<String> event = null;
			try {
				for (event = reader.readNextEvent(readerTimeoutMs); null != event.getEvent(); ) {
					if (event.getEvent() != null) {
						System.out.format("Read event '%s'%n", event.getEvent());
					}
				}
			} catch (ReinitializationRequiredException e) {
				e.printStackTrace();
			}
			System.out.format("No more events from %s/%s%n", scopeName, streamName);
		}
		return true;
	}

	private boolean invokePathRead(final PathOperation<? extends PathItem> pathOp) {
		//for now we consider that we use one StreamManager only
		//StreamManager streamManager = StreamManager.create(controllerURI);

		final String path = pathOp.dstPath();
		final String scope = path.substring(0, path.indexOf("/"));
		final String streamName = path.substring(path.indexOf("/") + 1);
		final URI controllerURI = URI.create(uriSchema);

		//it's random for now, but to use offsets we'll need to use the same readerGroup name.
		final String readerGroup = UUID.randomUUID().toString().replace("-", "");
		final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
				.stream(Stream.of(scope, streamName))
				.build();
		try (final ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
			readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
		}

		try (final ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
		     final EventStreamReader<String> reader = clientFactory.createReader("reader",
				     readerGroup,
				     new JavaSerializer<String>(),
				     ReaderConfig.builder().build())) {
			System.out.format("Reading all the events from %s/%s%n", scope, streamName);
			EventRead<String> event = null;
			try {
				for (event = reader.readNextEvent(readerTimeoutMs); null != event.getEvent(); ) {
					if (event.getEvent() != null) {
						System.out.format("Read event '%s'%n", event.getEvent());
						//should we store events?
					}
				}
			} catch (ReinitializationRequiredException e) {
				//There are certain circumstances where the reader needs to be reinitialized
				//map it to some internal error of Mongoose?
				e.printStackTrace();
			}

			System.out.format("No more events from %s/%s%n", scope, streamName);
		}
		return true;
	}

	protected boolean invokePathDelete(final PathOperation<? extends PathItem> pathOp) {
		final String path = pathOp.dstPath();
		final String scopeName = path.substring(0, path.indexOf("/"));
		final String streamName = path.substring(path.indexOf("/") + 1, path.length());
		if (streamManager.sealStream(scopeName, streamName)) {
			if (streamManager.deleteStream(scopeName, streamName)) {
				return true;
			} else {
				Loggers.ERR.debug(
						"Failed to delete the stream {} in the scope {}", streamName,
						scopeName);
				pathOp.status(Operation.Status.RESP_FAIL_UNKNOWN);
				return false;
			}
		} else {
			Loggers.ERR.debug(
					"Failed to seal the stream {} in the scope {}", streamName,
					scopeName);
		}
		pathOp.status(Operation.Status.RESP_FAIL_UNKNOWN);
		return false;
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
