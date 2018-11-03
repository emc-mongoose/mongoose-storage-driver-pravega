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
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
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
import java.nio.ByteBuffer;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;

public class PravegaStorageDriver<I extends Item, O extends Operation<I>>
		extends NioStorageDriverBase<I, O> {

	protected final String uriSchema;

	protected final String[] endpointAddrs;
	protected final int nodePort;
	private final AtomicInteger rrc = new AtomicInteger(0);


	protected final int readerTimeoutMs;

	protected int inBuffSize = BUFF_SIZE_MIN;
	protected int outBuffSize = BUFF_SIZE_MAX;

	//private StreamManager streamManager;
	private Map<String, Map<String, Stream>> scopeMap = new HashMap<>();

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
		readerTimeoutMs = storageConfig.intVal("item-input-readerTimeout");

		endpointAddrs = endpointAddrList.toArray(new String[endpointAddrList.size()]);
		requestAuthTokenFunc = null; // do not use
		requestNewPathFunc = null; // do not use
	}

	protected StreamManager getEndpoint(final String nodeAddr) {
		try {
			final String addr;
			final int port;
			int portSepPos = nodeAddr.lastIndexOf(':');
			if(portSepPos > 0) {
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
		} catch(final URISyntaxException e) {
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
		if(endpointAddr == null) {
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
		if(operation instanceof DataOperation) {
			invokeFileNio((DataOperation<? extends DataItem>) operation);
		} else if(operation instanceof PathOperation) {
			invokeDirectoryNio((PathOperation<? extends PathItem>) operation);
		} else {
			throw new AssertionError("Not implemented");
		}
	}

	private void invokeFileNio(DataOperation<? extends DataItem> fileOperation) {
		final OpType opType = fileOperation.type();
		final DataItem fileItem = fileOperation.item();
		try {
			switch (opType) {
				case NOOP:
					//TODO
					break;
				case CREATE:
					if (submitItemCreate(fileOperation, fileItem))
					{
//						...
					}
					break;
				case READ:
					//TODO: EventStreamReader<ByteBuffer>  ->  readNextEvent
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
					if (invokePathRead(pathOperation)) {
						//finishOperation?
					}
					break;
				case DELETE:
					if (invokePathDelete(pathOperation)) {
						//finishOperation?
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


	protected boolean submitItemCreate(final DataOperation<? extends DataItem> eventOperation, final DataItem eventItem)
	{
		final String path = eventOperation.dstPath();
		final String scope = path.substring(0, path.indexOf("/"));
		final String streamName = path.substring(path.indexOf("/")+1);

		if (scopeMap.get(scope).get(streamName) == null) {
			Loggers.ERR.debug("Failed to create event: the stream {}  not found in the scope {}", streamName, scope);
			eventOperation.status(Operation.Status.RESP_FAIL_UNKNOWN);
			return false;
		};

		StreamConfiguration streamConfig = StreamConfiguration.builder()
				.scalingPolicy(ScalingPolicy.fixed(1))
				.build();

		streamManager.createStream(scope, streamName, streamConfig);

		try (ClientFactory clientFactory = ClientFactory.withScope(scope, URI.create(uriSchema));
			 EventStreamWriter<ByteBuffer> writer = clientFactory.createEventWriter(streamName,
					 null,
					 EventWriterConfig.builder().build())) {

			final long buffSize;
			try {
				buffSize = eventItem.size();
			} catch (IOException e) {
				eventOperation.status(Operation.Status.FAIL_IO);
				Loggers.ERR.debug("Failed to create event: " + e.getMessage());
				return false;
			}

			final DataInput dataInput = eventItem.dataInput();
			final int dataSize = dataInput.getSize();

			final ByteBuffer outBuff = dataInput
					.getLayer(eventItem.layer())
					.asReadOnlyBuffer();

			outBuff.position((int)eventItem.offset());

			if (dataSize > outBuff.remaining()){
				outBuff.position(0);
				eventItem.offset(0);
			}
			outBuff.limit(dataSize);

			final CompletableFuture writeFuture = writer.writeEvent(outBuff).thenAccept(result -> {
				eventItem.offset((eventItem.offset() + dataSize) % buffSize);
				eventOperation.countBytesDone(dataSize);
				eventOperation.status(Operation.Status.SUCC);
			});

			try {
				writeFuture.get();
			} catch ( Exception e) {
				Loggers.ERR.debug("Failed to create event: " + e.getMessage());
				eventOperation.status(Operation.Status.RESP_FAIL_CLIENT);
				return false;
			}

			return true;
		}
	}

	private boolean invokePathRead(final PathOperation<? extends PathItem> pathOp) {
		//for now we consider that we use one StreamManager only

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
			 EventStreamReader<String> reader = clientFactory.createReader("reader",
					 readerGroup,
					 new JavaSerializer<String>(),
					 ReaderConfig.builder().build())) {
			System.out.format("Reading all the events from %s/%s%n", scope, streamName);
			EventRead<String> event = null;
			do {
				try {
					event = reader.readNextEvent(readerTimeoutMs);
					if (event.getEvent() != null) {
						System.out.format("Read event '%s'%n", event.getEvent());
						//should we store events?
					}
				} catch (ReinitializationRequiredException e) {
					//There are certain circumstances where the reader needs to be reinitialized
					//map it to some internal error of Mongoose?
					e.printStackTrace();
				}
			} while (event.getEvent() != null);
			System.out.format("No more events from %s/%s%n", scope, streamName);
		}
		return true;
	}

	protected boolean invokePathDelete(final PathOperation<? extends PathItem> pathOp) {
		final String path = pathOp.dstPath();
		final StreamManager streamManager = StreamManager.create(URI.create(uriSchema));
		final String scopeName = path.substring(0,path.indexOf("/"));
		final String streamName = path.substring(path.indexOf("/")+1);
		if(streamManager.sealStream(scopeName, streamName)){
			if(streamManager.deleteStream(scopeName, streamName)) {
				return true;
			}
			else {
				Loggers.ERR.debug(
						"Failed to delete the stream {} in the scope {}", streamName,
						scopeName);
				pathOp.status(Operation.Status.RESP_FAIL_UNKNOWN);
				return false;
			}
		}
		else {
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
