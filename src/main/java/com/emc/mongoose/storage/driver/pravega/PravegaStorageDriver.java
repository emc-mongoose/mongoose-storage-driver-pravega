package com.emc.mongoose.storage.driver.pravega;

import static com.emc.mongoose.item.op.OpType.NOOP;
import static com.emc.mongoose.item.op.Operation.Status.FAIL_UNKNOWN;
import static com.emc.mongoose.item.op.Operation.Status.INTERRUPTED;
import static com.emc.mongoose.item.op.Operation.Status.RESP_FAIL_CLIENT;
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
import com.emc.mongoose.item.op.Operation.Status;
import com.emc.mongoose.item.op.data.DataOperation;
import com.emc.mongoose.item.op.path.PathOperation;
import com.emc.mongoose.logging.LogUtil;
import com.emc.mongoose.logging.Loggers;
import com.emc.mongoose.storage.Credential;
import com.emc.mongoose.storage.driver.coop.CoopStorageDriverBase;
import com.emc.mongoose.storage.driver.pravega.cache.ClientFactoryCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ClientFactoryCreateFunctionImpl;
import com.emc.mongoose.storage.driver.pravega.cache.ScopeCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.StreamCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.EventWriterCreateFunction;
import com.emc.mongoose.storage.driver.pravega.exception.ScopeCreateException;
import com.emc.mongoose.storage.driver.pravega.exception.StreamCreateException;
import com.emc.mongoose.storage.driver.pravega.io.DataItemSerializer;

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
	protected final boolean createRoutingKeys;
	protected final long createRoutingKeysPeriod;
	protected final int readTimeoutMillis;
	protected final Serializer<DataItem> evtSerializer = new DataItemSerializer(false);
	protected final EventWriterConfig evtWriterConfig = EventWriterConfig.builder().build();
	// round-robin counter to select the endpoint node for each load operation in order to distribute them uniformly
	private final AtomicInteger rrc = new AtomicInteger(0);
	private final StreamConfiguration streamConfig;

	// should be an inner class in order to access the stream create function implementation constructor
	final class ScopeCreateFunctionImpl
	implements ScopeCreateFunction {

		private final StreamManager streamMgr;

		public ScopeCreateFunctionImpl(final StreamManager streamMgr) {
			this.streamMgr = streamMgr;
		}

		@Override
		public final StreamCreateFunction apply(final String scopeName)
		throws ScopeCreateException {
			try {
				if(!streamMgr.createScope(scopeName)) {
					Loggers.ERR.warn("Scope \"{}\" was not created, may be already existing before", scopeName);
				}
				return new StreamCreateFunctionImpl(streamMgr, scopeName);
			} catch(final Throwable cause) {
				throw new ScopeCreateException(scopeName, cause);
			}
		}
	}

	// should be an inner class in order to access the storage driver instance stream config field
	final class StreamCreateFunctionImpl
	implements StreamCreateFunction {

		private final StreamManager streamMgr;
		private final String scopeName;

		public StreamCreateFunctionImpl(
			final StreamManager streamMgr, final String scopeName
		) {
			this.streamMgr = streamMgr;
			this.scopeName = scopeName;
		}

		@Override
		public final String apply(final String streamName) {
			try {
				if(!streamMgr.createStream(scopeName, streamName, streamConfig)) {
					Loggers.ERR.warn(
						"Stream \"{}\" was not created @ the scope \"{}\", may be already existing before", streamName,
						scopeName
					);
				}
				return streamName;
			} catch(final Throwable cause) {
				throw new StreamCreateException(streamName, cause);
			}
		}

		@Override
		public final int hashCode() {
			return streamMgr.hashCode() ^ streamConfig.hashCode() ^ scopeName.hashCode();
		}

		@Override
		public final boolean equals(final Object other) {
			if(other instanceof StreamCreateFunction) {
				final StreamCreateFunctionImpl that = (StreamCreateFunctionImpl) other;
				return this.streamMgr.equals(that.streamMgr) && this.scopeName.equals(that.scopeName);
			} else {
				return false;
			}
		}
	}

	// should be an inner class in order to access the storage driver instance's fields (serializer, writer config)
	final class EventWriterCreateFunctionImpl
	implements EventWriterCreateFunction {

		private final ClientFactory clientFactory;

		public EventWriterCreateFunctionImpl(final ClientFactory clientFactory) {
			this.clientFactory = clientFactory;
		}

		@Override
		public final EventStreamWriter<DataItem> apply(final String streamName) {
			return clientFactory.createEventWriter(streamName, evtSerializer, evtWriterConfig);
		}

		@Override
		public final int hashCode() {
			return clientFactory.hashCode();
		}

		@Override
		public final boolean equals(final Object other) {
			if(other instanceof EventWriterCreateFunction) {
				final EventWriterCreateFunctionImpl that = (EventWriterCreateFunctionImpl) other;
				return this.clientFactory.equals(that.clientFactory);
			} else {
				return false;
			}
		}
	}

	// caches allowing the lazy creation of the necessary things:
	// * endpoints
	private final Map<String, URI> endpointCache = new ConcurrentHashMap<>();
	// * stream managers
	private final Map<URI, StreamManager> streamMgrCache = new ConcurrentHashMap<>();
	// * scopes
	private final Map<StreamManager, ScopeCreateFunction> scopeCreateFuncCache = new ConcurrentHashMap<>();
	private final Map<String, StreamCreateFunction> streamCreateFuncCache = new ConcurrentHashMap<>();
	// * streams
	private final Map<String, Map<String, String>> scopeStreamsCache = new ConcurrentHashMap<>();
	// * client factories
	private final Map<URI, ClientFactoryCreateFunction> clientFactoryCreateFuncCache = new ConcurrentHashMap<>();
	private final Map<String, ClientFactory> clientFactoryCache = new ConcurrentHashMap<>();
	// * event writers
	private final Map<ClientFactory, EventWriterCreateFunction> evtWriterCreateFuncCache = new ConcurrentHashMap<>();
	private final Map<String, EventStreamWriter<DataItem>> evtWriterCache = new ConcurrentHashMap<>();

	public PravegaStorageDriver(
		final String stepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
		final int batchSize
	) throws OmgShootMyFootException {
		super(stepId, dataInput, storageConfig, verifyFlag, batchSize);
		this.streamConfig = StreamConfiguration
			.builder()
			.scalingPolicy(ScalingPolicy.fixed(concurrencyLimit))
			.build();
		this.uriSchema = DEFAULT_URI_SCHEMA;
		final Config nodeConfig = storageConfig.configVal("net-node");
		nodePort = storageConfig.intVal("net-node-port");
		final List<String> endpointAddrList = nodeConfig.listVal("addrs");
		final Config driverConfig = storageConfig.configVal("driver");
		final Config createRoutingKeysConfig = driverConfig.configVal("create-key");
		createRoutingKeys = createRoutingKeysConfig.boolVal("enabled");
		createRoutingKeysPeriod = createRoutingKeysConfig.longVal("count");
		readTimeoutMillis = driverConfig.intVal("read-timeoutMillis");
		endpointAddrs = endpointAddrList.toArray(new String[endpointAddrList.size()]);
		requestAuthTokenFunc = null; // do not use
		requestNewPathFunc = null; // do not use
	}

	String nextEndpointAddr() {
		return endpointAddrs[rrc.getAndIncrement() % endpointAddrs.length];
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

	/**
	 Not used in this driver implementation
	 */
	@Override
	protected String requestNewPath(final String path) {
		throw new AssertionError("Should not be invoked");
	}

	/**
	 Not used in this driver implementation
	 */
	@Override
	protected String requestNewAuthToken(final Credential credential) {
		throw new AssertionError("Should not be invoked");
	}

	@Override
	public List<I> list(
		final ItemFactory<I> itemFactory, final String path, final String prefix, final int idRadix,
		final I lastPrevItem, final int count
	) throws IOException {
		// TODO: Evgeny, issue SDP-50
		return null;
	}

	/**
	 Not used in this driver implementation
	 */
	@Override
	public void adjustIoBuffers(final long avgTransferSize, final OpType opType) {
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
		completeOperation(op, SUCC);
	}

	boolean completeOperation(final O op, final Status status) {
		concurrencyThrottle.release();
		op.status(status);
		op.finishRequest();
		op.startResponse();
		op.finishResponse();
		return handleCompleted(op);
	}

	boolean completeFailedOperation(final O op, final Throwable thrown) {
		//LogUtil.exception(Level.WARN, thrown, "{}: unexpected load operation failure", stepId);
		thrown.printStackTrace();
		return completeOperation(op, FAIL_UNKNOWN);
	}

	boolean completeEventCreateOperation(final DataOperation evtOp, final DataItem evtItem, final Throwable thrown) {
		if(null == thrown) {
			try {
				evtOp.countBytesDone(evtItem.size());
			} catch(final IOException ignored) {
			}
			return completeOperation((O) evtOp, SUCC);
		} else {
			return completeFailedOperation((O) evtOp, thrown);
		}
	}

	void submitEventOperation(final DataOperation evtOp, final OpType opType) {
		final String nodeAddr = evtOp.nodeAddr();
		switch(opType) {
			case CREATE:
				submitEventCreateOperation(evtOp, nodeAddr);
				break;
			case READ:
				submitEventReadOperation(evtOp, nodeAddr);
				break;
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

	void submitEventCreateOperation(final DataOperation evtOp, final String nodeAddr) {
		try {
			// prepare
			final URI endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::makeEndpointUri);
			final StreamManager streamMgr = streamMgrCache.computeIfAbsent(endpointUri, StreamManager::create);
			final String scopeName = DEFAULT_SCOPE; // TODO make this configurable
			final ScopeCreateFunction scopeCreateFunc = scopeCreateFuncCache.computeIfAbsent(
				streamMgr, ScopeCreateFunctionImpl::new
			);
			// create the scope if necessary
			final StreamCreateFunction streamCreateFunc = streamCreateFuncCache.computeIfAbsent(
				scopeName, scopeCreateFunc
			);
			final String streamName = evtOp.dstPath();
			scopeStreamsCache
				.computeIfAbsent(scopeName, ScopeCreateFunction::createStreamCache)
				.computeIfAbsent(streamName, streamCreateFunc);
			// create the client factory create function if necessary
			final ClientFactoryCreateFunction clientFactoryCreateFunc = clientFactoryCreateFuncCache.computeIfAbsent(
				endpointUri, ClientFactoryCreateFunctionImpl::new
			);
			// create the client factory if necessary
			final ClientFactory clientFactory = clientFactoryCache.computeIfAbsent(scopeName, clientFactoryCreateFunc);
			// create the event stream writer create function if necessary
			final EventWriterCreateFunction evtWriterCreateFunc = evtWriterCreateFuncCache.computeIfAbsent(
				clientFactory, EventWriterCreateFunctionImpl::new
			);
			// create the event stream writer if necessary
			final EventStreamWriter<DataItem> evtWriter = evtWriterCache.computeIfAbsent(
				streamName, evtWriterCreateFunc
			);
			final DataItem evtItem = evtOp.item();
			// submit the event writing
			final CompletionStage<Void> writeEvtFuture;
			if(createRoutingKeys) {
				final String routingKey = Long.toString(
					createRoutingKeysPeriod > 0 ? evtItem.offset() % createRoutingKeysPeriod : evtItem.offset(),
					Character.MAX_RADIX
				);
				writeEvtFuture = evtWriter.writeEvent(routingKey, evtItem);
			} else {
				writeEvtFuture = evtWriter.writeEvent(evtItem);
			}
			evtOp.startRequest();
			writeEvtFuture.handle((returned, thrown) -> completeEventCreateOperation(evtOp, evtItem, thrown));
		} catch(final ScopeCreateException e) {
			LogUtil.exception(Level.WARN, e.getCause(), "{}: failed to create the scope \"{}\"", stepId, e.scopeName());
			completeOperation((O) evtOp, RESP_FAIL_CLIENT);
		} catch(final StreamCreateException e) {
			LogUtil.exception(
				Level.WARN, e.getCause(), "{}: failed to create the stream \"{}\"", stepId, e.streamName()
			);
			completeOperation((O) evtOp, RESP_FAIL_CLIENT);
		} catch(final NullPointerException e) {
			if(!isStarted()) { // occurs on manual interruption which is normal so should be handled
				completeOperation((O) evtOp, INTERRUPTED);
			} else {
				completeFailedOperation((O) evtOp, e);
			}
		} catch(final Throwable cause) {
			completeFailedOperation((O) evtOp, cause);
		}
	}

	void submitEventReadOperation(final DataOperation evtOp, final String nodeAddr) {
		// TODO: Evgeny, issue SDP-50
	}

	void submitStreamOperation(final PathOperation streamOp, final OpType opType) {
		final String nodeAddr = streamOp.nodeAddr();
		switch(opType) {
			case CREATE:
				submitStreamCreateOperation(streamOp, nodeAddr);
				break;
			case READ:
				submitStreamReadOperation(streamOp, nodeAddr);
				break;
			case UPDATE:
				throw new AssertionError("Not implemented");
			case DELETE:
				submitStreamDeleteOperation(streamOp, nodeAddr);
				break;
			case LIST:
				throw new AssertionError("Not implemented");
			default:
				throw new AssertionError("Not implemented");
		}
	}

	void submitStreamCreateOperation(final PathOperation streamOp, final String nodeAddr) {
		// TODO: Vlad, issue SDP-47
	}

	void submitStreamReadOperation(final PathOperation streamOp, final String nodeAddr) {
		// TODO: Alex, issue SDP-51
	}

	void submitStreamDeleteOperation(final PathOperation streamOp, final String nodeAddr) {
		// TODO: Igor, issue SDP-49
	}

	@Override
	protected void doClose()
	throws IOException {
		super.doClose();
		// clear all caches
		endpointCache.clear();
		streamMgrCache.values().forEach(StreamManager::close);
		streamMgrCache.clear();
		scopeCreateFuncCache.clear();
		streamCreateFuncCache.clear();
		scopeStreamsCache.values().forEach(Map::clear);
		scopeStreamsCache.clear();
		clientFactoryCreateFuncCache.clear();
		clientFactoryCache.values().forEach(ClientFactory::close);
		clientFactoryCache.clear();
		evtWriterCreateFuncCache.clear();
		evtWriterCache.values().forEach(EventStreamWriter::close);
		evtWriterCache.clear();
	}

	@Override
	public String toString() {
		return String.format(super.toString(), DRIVER_NAME);
	}
}
