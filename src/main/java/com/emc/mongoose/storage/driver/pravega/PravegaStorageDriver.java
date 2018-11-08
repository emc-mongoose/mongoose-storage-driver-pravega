package com.emc.mongoose.storage.driver.pravega;

import static com.emc.mongoose.item.op.OpType.NOOP;
import static com.emc.mongoose.storage.driver.pravega.PravegaConstants.DEFAULT_SCOPE;
import static com.emc.mongoose.storage.driver.pravega.PravegaConstants.DEFAULT_URI_SCHEMA;
import static com.emc.mongoose.storage.driver.pravega.PravegaConstants.DRIVER_NAME;
import com.emc.mongoose.data.DataInput;
import com.emc.mongoose.exception.InterruptRunException;
import com.emc.mongoose.exception.OmgShootMyFootException;
import com.emc.mongoose.item.DataItem;
import com.emc.mongoose.item.Item;
import com.emc.mongoose.item.ItemFactory;
import com.emc.mongoose.item.PathItem;
import com.emc.mongoose.item.op.OpType;
import com.emc.mongoose.item.op.Operation;
import com.emc.mongoose.item.op.data.DataOperation;
import com.emc.mongoose.item.op.path.PathOperation;
import com.emc.mongoose.logging.Loggers;
import com.emc.mongoose.storage.Credential;
import com.emc.mongoose.storage.driver.coop.CoopStorageDriverBase;

import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
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
	private final Map<String, URI> endpointUriByNodeAddr = new ConcurrentHashMap<>();
	private final Map<URI, StreamManager> streamMgrsByEndpointUri = new ConcurrentHashMap<>();
	private final Map<StreamManager, String> scopesByStreamMgr = new ConcurrentHashMap<>();
	private final Map<URI, Map<String, ClientFactory>> clientFactories = new ConcurrentHashMap<>();
	private final Map<String, Map<String, String>> streamsByScope = new ConcurrentHashMap<>();
	private final StreamConfiguration streamConfig = StreamConfiguration
		.builder()
		.scalingPolicy(ScalingPolicy.fixed(1))
		.build();
	private final EventWriterConfig evtWriterConfig = EventWriterConfig.builder().build();

	public PravegaStorageDriver(
		final String stepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
		final int batchSize
	) throws OmgShootMyFootException {
		super(stepId, dataInput, storageConfig, verifyFlag, batchSize);
		this.uriSchema = DEFAULT_URI_SCHEMA;
		final Config nodeConfig = storageConfig.configVal("net-node");
		nodePort = storageConfig.intVal("net-node-port");
		final List<String> endpointAddrList = nodeConfig.listVal("addrs");
		readTimeoutMillis = storageConfig.intVal("storage-driver-read-timeoutMillis");
		endpointAddrs = endpointAddrList.toArray(new String[endpointAddrList.size()]);
		requestAuthTokenFunc = null; // do not use
		requestNewPathFunc = null; // do not use
	}

	URI endpointUri(final String nodeAddr) {
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

	private final String nextEndpointAddr() {
		return endpointAddrs[rrc.getAndIncrement() % endpointAddrs.length];
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
		final OpType opType = op.type();
		if(NOOP.equals(opType)) {
			submitNoop(op);
		} else {
			final String nodeAddr = op.nodeAddr();
			if(op instanceof DataOperation) {
				final String stream = op.dstPath();
				final DataItem evtItem = ((DataOperation) op).item();
				submitEventOperation(evtItem, opType, nodeAddr, stream);
			} else if(op instanceof PathOperation) {
				final PathItem streamItem = ((PathOperation) op).item();
				submitStreamOperation(streamItem, opType, nodeAddr);
			} else {
				throw new AssertionError(DRIVER_NAME + " storage driver doesn't support the token operations");
			}
		}
		return true;
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
		op.finishRequest();
		op.startResponse();
		op.finishResponse();
		op.status(Operation.Status.SUCC);
		handleCompleted(op);
	}

	void submitEventOperation(final DataItem evtItem, final OpType opType, final String nodeAddr, final String stream) {
		switch(opType) {
			case CREATE:
				submitCreateEventOperation(evtItem, nodeAddr, stream);
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

	void submitCreateEventOperation(final DataItem evtItem, final String nodeAddr, final String streamName) {
		final URI endpointUri = endpointUriByNodeAddr.computeIfAbsent(nodeAddr, this::endpointUri);
		final StreamManager streamMgr = streamMgrsByEndpointUri.computeIfAbsent(endpointUri, StreamManager::create);
		final String scopeName = DEFAULT_SCOPE; // TODO make this configurable
		final Map<String, String> scopeStreams = streamsByScope.computeIfAbsent(
			scopeName,
			name -> {
				streamMgr.createScope(name);
				return new ConcurrentHashMap<String, String>();
			}
		);
		scopeStreams.computeIfAbsent(
			streamName,
			name -> {
				streamMgr.createStream(scopeName, name, streamConfig);
				return streamName;
			}
		);

	}

	void submitStreamOperation(final PathItem streamItem, final OpType opType, final String nodeAddr) {
		switch(opType) {
			case CREATE:
				submitCreateStreamOperation(streamItem, nodeAddr);
				break;
			case READ:
				submitReadStreamOperation(streamItem, nodeAddr);
				break;
			case UPDATE:
				throw new AssertionError("Not implemented");
			case DELETE:
				submitDeleteStreamOperation(streamItem, nodeAddr);
				break;
			case LIST:
				throw new AssertionError("Not implemented");
			default:
				throw new AssertionError("Not implemented");
		}
	}

	void submitReadStreamOperation(final PathItem streamItem, final String nodeAddr) {
	}

	void submitDeleteStreamOperation(final PathItem streamItem, final String nodeAddr) {
	}

	void submitCreateStreamOperation(final PathItem streamItem, final String nodeAddr) {
	}

	@Override
	protected void doClose()
	throws IOException {
		super.doClose();
		streamsByScope
			.values()
			.forEach(Map::clear);
		streamsByScope.clear();
		streamMgrsByEndpointUri
			.values()
			.forEach(StreamManager::close);
		streamMgrsByEndpointUri.clear();
	}

	@Override
	public String toString() {
		return String.format(super.toString(), DRIVER_NAME);
	}
}
