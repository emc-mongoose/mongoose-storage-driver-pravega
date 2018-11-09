package com.emc.mongoose.storage.driver.pravega;

import static com.emc.mongoose.item.op.OpType.NOOP;
import static com.emc.mongoose.item.op.Operation.Status.FAIL_UNKNOWN;
import static com.emc.mongoose.item.op.Operation.Status.SUCC;
import static com.emc.mongoose.storage.driver.pravega.PravegaConstants.DEFAULT_SCOPE;
import static com.emc.mongoose.storage.driver.pravega.PravegaConstants.DEFAULT_URI_SCHEMA;
import static com.emc.mongoose.storage.driver.pravega.PravegaConstants.DRIVER_NAME;
import com.emc.mongoose.data.DataInput;
import com.emc.mongoose.exception.InterruptRunException;
import com.emc.mongoose.exception.OmgShootMyFootException;
import com.emc.mongoose.item.DataItem;
import com.emc.mongoose.item.Item;
import com.emc.mongoose.item.ItemFactory;
import com.emc.mongoose.item.op.OpType;
import com.emc.mongoose.item.op.Operation;
import com.emc.mongoose.item.op.data.DataOperation;
import com.emc.mongoose.item.op.path.PathOperation;
import com.emc.mongoose.logging.LogUtil;
import com.emc.mongoose.logging.Loggers;
import com.emc.mongoose.storage.Credential;
import com.emc.mongoose.storage.driver.coop.CoopStorageDriverBase;

import com.emc.mongoose.storage.driver.pravega.io.DataItemSerializer;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import org.apache.logging.log4j.Level;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class PravegaStorageDriver<I extends Item, O extends Operation<I>>
extends CoopStorageDriverBase<I, O>  {

	protected final String uriSchema;
	protected final String[] endpointAddrs;
	protected final int nodePort;
	protected final int readTimeoutMillis;
	protected int inBuffSize = BUFF_SIZE_MIN;
	protected int outBuffSize = BUFF_SIZE_MAX;

	private final AtomicInteger rrc = new AtomicInteger(0);
	private final StreamConfiguration streamConfig = StreamConfiguration
		.builder()
		.scalingPolicy(ScalingPolicy.fixed(1))
		.build();
	private final EventWriterConfig evtWriterConfig = EventWriterConfig.builder().build();
	private final Serializer<DataItem> evtSerializer = new DataItemSerializer(false);

	// caches
	private final Map<String, URI> endpointUriByNodeAddr = new ConcurrentHashMap<>();
	private final Map<URI, StreamManager> streamMgrByEndpointUri = new ConcurrentHashMap<>();
	private final Map<URI, Map<String, ClientFactory>> clientFactories = new ConcurrentHashMap<>();
	private final Map<String, Map<String, String>> streamsByScope = new ConcurrentHashMap<>();
	private final Map<String, EventStreamWriter<DataItem>> evtWriterByStream = new ConcurrentHashMap<>();

	public PravegaStorageDriver(
		final String stepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
		final int batchSize
	) throws OmgShootMyFootException {
		super(stepId, dataInput, storageConfig, verifyFlag, batchSize);
		this.uriSchema = DEFAULT_URI_SCHEMA;
		final Config nodeConfig = storageConfig.configVal("net-node");
		nodePort = storageConfig.intVal("net-node-port");
		final List<String> endpointAddrList = nodeConfig.listVal("addrs");
		readTimeoutMillis = storageConfig.intVal("driver-read-timeoutMillis");
		endpointAddrs = endpointAddrList.toArray(new String[endpointAddrList.size()]);
		requestAuthTokenFunc = null; // do not use
		requestNewPathFunc = null; // do not use
	}

	URI makeEndpointUri(final String nodeAddr) {
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
			return new URI(uriSchema, uid, addr, port, "/", null, null);
		} catch (final URISyntaxException e) {
			throw new RuntimeException(e);
		}
	}

	String nextEndpointAddr() {
		return endpointAddrs[rrc.getAndIncrement() % endpointAddrs.length];
	}

	Map<String, String> createScope(final StreamManager streamMgr, final String scopeName) {
		streamMgr.createScope(scopeName);
		return new ConcurrentHashMap<>();
	}

	String createStream(
		final StreamManager streamMgr, final StreamConfiguration streamConfig, final String scopeName,
		final String streamName
	) {
		streamMgr.createStream(scopeName, streamName, streamConfig);
		return streamName;
	}

	EventStreamWriter<DataItem> createEventStreamWriter(final ClientFactory clientFactory, final String streamName) {
		return clientFactory.createEventWriter(streamName, evtSerializer, evtWriterConfig);
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
	) throws IOException {
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
		if(OpType.CREATE.equals(opType)) {
			Loggers.MSG.info("Adjust output buffer size: {}", SizeInBytes.formatFixedSize(size));
			outBuffSize = size;
		} else if (OpType.READ.equals(opType)) {
			Loggers.MSG.info("Adjust input buffer size: {}", SizeInBytes.formatFixedSize(size));
			inBuffSize = size;
		}
	}

	@Override
	protected void prepare(final O operation) {
		super.prepare(operation);
		String endpointAddr = operation.nodeAddr();
		if(endpointAddr == null) {
			endpointAddr = nextEndpointAddr();
			operation.nodeAddr(endpointAddr);
		}
	}

	@Override
	protected final boolean submit(final O op)
	throws InterruptRunException, IllegalStateException {
		if(concurrencyThrottle.tryAcquire()) {
			final OpType opType = op.type();
			if(NOOP.equals(opType)) {
				submitNoop(op);
			} else {
				if(op instanceof DataOperation) {
					submitEventOperation((DataOperation) op, opType);
				} else if(op instanceof PathOperation) {
					submitStreamOperation((PathOperation) op, opType);
				} else {
					throw new AssertionError(DRIVER_NAME + " storage driver doesn't support the token operations");
				}
			}
			return true;
		} else {
			return false;
		}
	}

	@Override
	protected final int submit(final List<O> ops, final int from, final int to)
	throws InterruptRunException, IllegalStateException {
		for(int i = from; i < to; i ++) {
			if(!submit(ops.get(i))) {
				return i - from;
			}
		}
		return to - from;
	}

	@Override
	protected final int submit(final List<O> ops)
	throws InterruptRunException, IllegalStateException {
		final int opsCount = ops.size();
		for(int i = 0; i < opsCount; i ++) {
			if(!submit(ops.get(i))) {
				return i;
			}
		}
		return opsCount;
	}

	void submitNoop(final O op) {
		op.startRequest();
		completeOperation(op, null);
	}

	boolean completeOperation(final O op, final Throwable thrown) {
		concurrencyThrottle.release();
		op.finishRequest();
		op.startResponse();
		op.finishResponse();
		if(null == thrown) {
			op.status(SUCC);
		} else {
			LogUtil.exception(Level.WARN, thrown, "Load operation failure");
			op.status(FAIL_UNKNOWN);
		}
		return handleCompleted(op);
	}

	void submitEventOperation(final DataOperation evtOp, final OpType opType) {
		final String nodeAddr = evtOp.nodeAddr();
		switch(opType) {
			case CREATE:
				submitCreateEventOperation(evtOp, nodeAddr);
				break;
			case READ:
				throw new AssertionError("Not implemented");
			case UPDATE:
				throw new AssertionError("Not implemented");
			case DELETE:
				throw new AssertionError("Not implemented");
			case LIST:
				throw new AssertionError("Not implemented");
			default:
				throw new AssertionError("Not implemented");
		}
	}

	void submitCreateEventOperation(final DataOperation evtOp, final String nodeAddr) {
		// prepare
		final URI endpointUri = endpointUriByNodeAddr.computeIfAbsent(nodeAddr, this::makeEndpointUri);
		final StreamManager streamMgr = streamMgrByEndpointUri.computeIfAbsent(endpointUri, StreamManager::create);
		final String scopeName = DEFAULT_SCOPE; // TODO make this configurable
		final String streamName = evtOp.dstPath();
		streamsByScope
			.computeIfAbsent(scopeName, name -> createScope(streamMgr, scopeName))
			.computeIfAbsent(streamName, name -> createStream(streamMgr, streamConfig, scopeName, name));
		final ClientFactory clientFactory = clientFactories
			.computeIfAbsent(endpointUri, u -> new ConcurrentHashMap<>())
			.computeIfAbsent(scopeName, name -> ClientFactory.withScope(name, endpointUri));
		final EventStreamWriter<DataItem> evtWriter = evtWriterByStream
			.computeIfAbsent(streamName, name -> createEventStreamWriter(clientFactory, name));
		final DataItem evtItem = evtOp.item();
		// submit the event writing
		final CompletionStage<Void> writeEvtFuture = evtWriter.writeEvent(evtItem);
		evtOp.startRequest();
		writeEvtFuture.handle((returned, thrown) -> completeOperation((O) evtOp, thrown));
	}

	void submitStreamOperation(final PathOperation streamOp, final OpType opType) {
		final String nodeAddr = streamOp.nodeAddr();
		switch(opType) {
			case CREATE:
				submitCreateStreamOperation(streamOp, nodeAddr);
				break;
			case READ:
				submitReadStreamOperation(streamOp, nodeAddr);
				break;
			case UPDATE:
				throw new AssertionError("Not implemented");
			case DELETE:
				submitDeleteStreamOperation(streamOp, nodeAddr);
				break;
			case LIST:
				throw new AssertionError("Not implemented");
			default:
				throw new AssertionError("Not implemented");
		}
	}

	void submitReadStreamOperation(final PathOperation streamOp, final String nodeAddr) {
		// TODO
	}

	void submitDeleteStreamOperation(final PathOperation streamOp, final String nodeAddr) {
		// TODO
	}

	void submitCreateStreamOperation(final PathOperation streamOp, final String nodeAddr) {
		// TODO
	}

	@Override
	protected void doClose()
	throws IOException {
		super.doClose();
		clientFactories.values().forEach(m -> m.values().forEach(ClientFactory::close));
		clientFactories.clear();
		streamsByScope.values().forEach(Map::clear);
		streamsByScope.clear();
		streamMgrByEndpointUri.values().forEach(StreamManager::close);
		streamMgrByEndpointUri.clear();
		endpointUriByNodeAddr.clear();
		evtWriterByStream.values().forEach(EventStreamWriter::close);
		evtWriterByStream.clear();
	}

	@Override
	public String toString() {
		return String.format(super.toString(), DRIVER_NAME);
	}
}
