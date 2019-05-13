package com.emc.mongoose.storage.driver.pravega;

import static com.emc.mongoose.base.Exceptions.throwUncheckedIfInterrupted;
import static com.emc.mongoose.base.item.op.OpType.CREATE;
import static com.emc.mongoose.base.item.op.OpType.NOOP;
import static com.emc.mongoose.base.item.op.Operation.SLASH;
import static com.emc.mongoose.base.item.op.Operation.Status.FAIL_IO;
import static com.emc.mongoose.base.item.op.Operation.Status.FAIL_TIMEOUT;
import static com.emc.mongoose.base.item.op.Operation.Status.FAIL_UNKNOWN;
import static com.emc.mongoose.base.item.op.Operation.Status.INTERRUPTED;
import static com.emc.mongoose.base.item.op.Operation.Status.RESP_FAIL_CORRUPT;
import static com.emc.mongoose.base.item.op.Operation.Status.RESP_FAIL_UNKNOWN;
import static com.emc.mongoose.base.item.op.Operation.Status.SUCC;
import static com.emc.mongoose.storage.driver.pravega.PravegaConstants.DRIVER_NAME;
import static com.emc.mongoose.storage.driver.pravega.PravegaConstants.MAX_BACKOFF_MILLIS;
import static com.emc.mongoose.storage.driver.pravega.io.StreamDataType.BYTES;
import static com.emc.mongoose.storage.driver.pravega.io.StreamDataType.EVENTS;
import static com.emc.mongoose.storage.driver.pravega.io.StreamScaleUtil.scaleToFixedSegmentCount;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.emc.mongoose.base.config.IllegalConfigurationException;
import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.item.DataItem;
import com.emc.mongoose.base.item.ItemFactory;
import com.emc.mongoose.base.item.op.OpType;
import com.emc.mongoose.base.item.op.Operation.Status;
import com.emc.mongoose.base.item.op.data.DataOperation;
import com.emc.mongoose.base.logging.LogContextThreadFactory;
import com.emc.mongoose.base.logging.LogUtil;
import com.emc.mongoose.base.logging.Loggers;
import com.emc.mongoose.base.storage.Credential;
import com.emc.mongoose.base.storage.driver.StorageDriverBase;
import com.emc.mongoose.storage.driver.pravega.cache.ByteStreamClientFactoryCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ByteStreamReaderCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ByteStreamReaderCreateFunctionImpl;
import com.emc.mongoose.storage.driver.pravega.cache.EventStreamClientFactoryCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.EventStreamClientFactoryCreateFunctionImpl;
import com.emc.mongoose.storage.driver.pravega.cache.ReaderCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ReaderGroupManagerCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ReaderGroupManagerCreateFunctionImpl;
import com.emc.mongoose.storage.driver.pravega.cache.ScopeCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ScopeCreateFunctionForStreamConfig;
import com.emc.mongoose.storage.driver.pravega.cache.StreamCreateFunction;
import com.emc.mongoose.storage.driver.pravega.io.ByteBufferSerializer;
import com.emc.mongoose.storage.driver.pravega.io.ByteStreamWriteChannel;
import com.emc.mongoose.storage.driver.pravega.io.DataItemSerializer;
import com.emc.mongoose.storage.driver.pravega.io.StreamDataType;
import com.github.akurilov.commons.system.DirectMemUtil;
import com.github.akurilov.confuse.Config;
import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.byteStream.impl.ByteStreamClientImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.netty.impl.ConnectionPoolImpl;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;

import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.pravega.common.util.AsyncIterator;
import lombok.Value;
import lombok.val;
import org.apache.logging.log4j.Level;

public class PravegaStorageDriver<I extends DataItem, O extends DataOperation<I>>
				extends StorageDriverBase<I, O> {

	private static final int INSTANCE_POOL_SIZE = 1000;

	protected final String uriSchema;
	protected final String scopeName;
	protected final String[] endpointAddrs;
	protected final int nodePort;
	protected final long controlApiTimeoutMillis;
	protected final boolean createBatchMode;
	protected final long readTimeoutMillis;
	protected final Serializer<I> evtSerializer = new DataItemSerializer<>(false);
	protected final Serializer<ByteBuffer> evtDeserializer = new ByteBufferSerializer();
	protected final EventWriterConfig evtWriterConfig = EventWriterConfig.builder().build();
	protected final ReaderConfig evtReaderConfig = ReaderConfig.builder().build();
	protected final String evtReaderGroupName = Long.toString(System.nanoTime());
	protected final ThreadLocal<ReaderGroupConfig.ReaderGroupConfigBuilder>
		evtReaderGroupConfigBuilder = ThreadLocal.withInitial(ReaderGroupConfig::builder);
	protected final ScalingPolicy scalingPolicy;
	protected final StreamDataType streamDataType;
	// round-robin counter to select the endpoint node for each load operation in order to distribute them uniformly
	private final AtomicInteger rrc = new AtomicInteger(0);
	private final ScheduledExecutorService executor;
	private final RoutingKeyFunction<I> routingKeyFunc;
	private volatile Position lastFailedStreamPos = null;
	private final Lock lastFailedStreamPosLock = new ReentrantLock();
	private volatile AsyncIterator<Stream> streamIterator = null;
	protected final Semaphore concurrencyThrottle;
	private final LongAdder scheduledOpCount = new LongAdder();
	private final LongAdder completedOpCount = new LongAdder();

	@Value
	final class ScopeCreateFunctionImpl
					implements ScopeCreateFunction {

		Controller controller;

		@Override
		public final StreamCreateFunction apply(final String scopeName) {
			try {
				if (controller.createScope(scopeName).get(controlApiTimeoutMillis, MILLISECONDS)) {
					Loggers.MSG.trace("Scope \"{}\" was created", scopeName);
				} else {
					Loggers.MSG.info(
									"Scope \"{}\" was not created, may be already existing before", scopeName);
				}
			} catch (final InterruptedException e) {
				throwUnchecked(e);
			} catch (final Throwable cause) {
				LogUtil.exception(
								Level.WARN, cause, "{}: failed to create the scope \"{}\"", stepId, scopeName);
			}
			return new StreamCreateFunctionImpl(controller, scopeName);
		}
	}

	@Value
	final class ScopeCreateFunctionForStreamConfigImpl
					implements ScopeCreateFunctionForStreamConfig {

		Controller controller;

		@Override
		public final StreamConfiguration apply(final String scopeName) {
			final StreamConfiguration streamConfig = StreamConfiguration.builder().scalingPolicy(scalingPolicy).scope(scopeName).build();
			try {
				if (controller.createScope(scopeName).get(controlApiTimeoutMillis, MILLISECONDS)) {
					Loggers.MSG.trace("Scope \"{}\" was created", scopeName);
				} else {
					Loggers.MSG.info(
									"Scope \"{}\" was not created, may be already existing before", scopeName);
				}
			} catch (final InterruptedException e) {
				throwUnchecked(e);
			} catch (final Throwable cause) {
				LogUtil.exception(
								Level.WARN, cause, "{}: failed to create the scope \"{}\"", stepId, scopeName);
			}
			return streamConfig;
		}
	}

	@Value
	final class StreamCreateFunctionImpl
					implements StreamCreateFunction {

		Controller controller;
		String scopeName;

		@Override
		public final StreamConfiguration apply(final String streamName) {
			final StreamConfiguration streamConfig = StreamConfiguration.builder()
							.scalingPolicy(scalingPolicy)
							.build();
			try {
				val createStreamFuture = controller.createStream(scopeName, streamName, streamConfig);
				if (createStreamFuture.get(controlApiTimeoutMillis, MILLISECONDS)) {
					Loggers.MSG.trace(
									"Stream \"{}/{}\" was created using the config: {}",
									scopeName,
									streamName,
									streamConfig);
				} else {
					scaleToFixedSegmentCount(
									controller, controlApiTimeoutMillis, scopeName, streamName, scalingPolicy);
				}
			} catch (final InterruptedException e) {
				throwUnchecked(e);
			} catch (final Throwable cause) {
				LogUtil.exception(
								Level.WARN, cause, "{}: failed to create the stream \"{}\"", stepId, streamName);
			}
			return streamConfig;
		}
	}

	@Value
	final class ReaderCreateFunctionImpl
					implements ReaderCreateFunction {

		EventStreamClientFactory clientFactory;

		@Override
		public EventStreamReader<ByteBuffer> apply(String readerGroup) {
			return clientFactory.createReader("reader", readerGroup, evtDeserializer, evtReaderConfig);
		}
	}

	@Value
	final class ByteStreamClientFactoryCreateFunctionImpl
	implements ByteStreamClientFactoryCreateFunction {

		ConnectionFactory connFactory;

		@Override
		public ByteStreamClientFactory apply(final Controller controller) {
			return new ByteStreamClientImpl(scopeName, controller, connFactory);
		}
	}

	// caches allowing the lazy creation of the necessary things:
	// * endpoints
	private final Map<String, URI> endpointCache = new ConcurrentHashMap<>();
	// * client configs
	private final Map<URI, ClientConfig> clientConfigCache = new ConcurrentHashMap<>();
	// * controllers
	private final Map<ClientConfig, Controller> controllerCache = new ConcurrentHashMap<>();
	// * scopes
	private final Map<Controller, ScopeCreateFunction> scopeCreateFuncCache = new ConcurrentHashMap<>();
	private final Map<String, StreamCreateFunction> streamCreateFuncCache = new ConcurrentHashMap<>();
	// * streams
	private final Map<String, Map<String, StreamConfiguration>> scopeStreamsCache = new ConcurrentHashMap<>();
	// * event stream client factories
	private final Map<ClientConfig, EventStreamClientFactoryCreateFunction> clientFactoryCreateFuncCache = new ConcurrentHashMap<>();
	private final Map<String, EventStreamClientFactory> clientFactoryCache = new ConcurrentHashMap<>();
	// * event stream writers
	private final Map<String, Queue<EventStreamWriter<I>>> evtWriterPoolCache = new ConcurrentHashMap<>();
	// * reader group
	private final Map<String, ReaderGroupConfig> evtReaderGroupConfigCache = new ConcurrentHashMap<>();
	private final Map<URI, ReaderGroupManagerCreateFunction> evtReaderGroupManagerCreateFuncCache = new ConcurrentHashMap<>();
	private final Map<String, ReaderGroupManager> evtReaderGroupManagerCache = new ConcurrentHashMap<>();
	// * event stream reader
	private final Map<EventStreamClientFactory, ReaderCreateFunction> eventStreamReaderCreateFuncCache = new ConcurrentHashMap<>();
	private final Map<String, EventStreamReader<ByteBuffer>> eventStreamReaderCache = new ConcurrentHashMap<>();
	// * scopes with StreamConfigs
	private final Map<Controller, ScopeCreateFunctionForStreamConfig> scopeCreateFuncForStreamConfigCache = new ConcurrentHashMap<>();
	private final Map<String, StreamConfiguration> scopeStreamConfigsCache = new ConcurrentHashMap<>();
	// * connection factory cache
	private final Map<ClientConfig, ConnectionFactory> connFactoryCache = new ConcurrentHashMap<>();
	// * byte stream client cache
	private final Map<ConnectionFactory, ByteStreamClientFactoryCreateFunction> byteStreamClientCreateFuncCache = new ConcurrentHashMap<>();
	private final Map<Controller, ByteStreamClientFactory> byteStreamClientFactoryCache = new ConcurrentHashMap<>();
	// * byte stream writer cache
	private final Map<ByteStreamClientFactory, Queue<ByteStreamWriteChannel>> byteStreamWriteChanPoolCache = new ConcurrentHashMap<>();
	// * byte stream reader cache
	private final Map<ByteStreamClientFactory, ByteStreamReaderCreateFunction> byteStreamReaderCreateFuncCache = new ConcurrentHashMap<>();
	private final Map<String, Queue<ByteStreamReader>> byteStreamReaderPoolCache = new ConcurrentHashMap<>();
	// * batch event writers
	private final Map<EventStreamClientFactory, Queue<TransactionalEventStreamWriter<I>>> batchEvtWriterPoolCache = new ConcurrentHashMap<>();

	public PravegaStorageDriver(
					final String stepId,
					final DataInput dataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize)
					throws IllegalConfigurationException, IllegalArgumentException {
		super(stepId, dataInput, storageConfig, verifyFlag);
		val driverConfig = storageConfig.configVal("driver");
		this.controlApiTimeoutMillis = driverConfig.longVal("control-timeoutMillis");
		val scalingConfig = driverConfig.configVal("scaling");
		this.scalingPolicy = PravegaScalingConfig.scalingPolicy(scalingConfig);
		val netConfig = storageConfig.configVal("net");
		this.uriSchema = netConfig.stringVal("uri-schema");
		this.scopeName = storageConfig.stringVal("namespace");
		if (scopeName == null || scopeName.isEmpty()) {
			Loggers.ERR.warn("Scope name not set, use the \"storage-namespace\" configuration option");
		}
		val nodeConfig = netConfig.configVal("node");
		this.nodePort = nodeConfig.intVal("port");
		val endpointAddrList = nodeConfig.listVal("addrs");
		val eventConfig = driverConfig.configVal("event");
		val createRoutingKeysConfig = eventConfig.configVal("key");
		val createRoutingKeys = createRoutingKeysConfig.boolVal("enabled");
		val createRoutingKeysPeriod = createRoutingKeysConfig.longVal("count");
		this.routingKeyFunc = createRoutingKeys ? new RoutingKeyFunctionImpl<>(createRoutingKeysPeriod) : null;
		this.readTimeoutMillis = eventConfig.longVal("timeoutMillis");
		this.streamDataType = StreamDataType.valueOf(driverConfig.stringVal("stream-data").toUpperCase());
		if(EVENTS.equals(streamDataType)) {
			this.createBatchMode = eventConfig.boolVal("batch");
		} else {
			this.createBatchMode = false;
		}
		this.endpointAddrs = endpointAddrList.toArray(new String[endpointAddrList.size()]);
		this.requestAuthTokenFunc = null; // do not use
		this.requestNewPathFunc = null; // do not use
		this.executor = Executors.newScheduledThreadPool(
						ioWorkerCount,
						new LogContextThreadFactory(toString(), true));
		this.concurrencyThrottle = new Semaphore(concurrencyLimit > 0 ? concurrencyLimit : Integer.MAX_VALUE, true);
	}

	String nextEndpointAddr() {
		return endpointAddrs[rrc.getAndIncrement() % endpointAddrs.length];
	}

	URI createEndpointUri(final String nodeAddr) {
		try {
			final String addr;
			final int port;
			val portSepPos = nodeAddr.lastIndexOf(':');
			if (portSepPos > 0) {
				addr = nodeAddr.substring(0, portSepPos);
				port = Integer.parseInt(nodeAddr.substring(portSepPos + 1));
			} else {
				addr = nodeAddr;
				port = nodePort;
			}
			val uid = credential == null ? null : credential.getUid();
			return new URI(uriSchema, uid, addr, port, "/", null, null);
		} catch (final URISyntaxException e) {
			throw new RuntimeException(e);
		}
	}

	ClientConfig createClientConfig(final URI endpointUri) {
		return ClientConfig.builder()
			.controllerURI(endpointUri)
			.maxConnectionsPerSegmentStore(concurrencyLimit > 0 ? concurrencyLimit : Integer.MAX_VALUE)
			.build();
	}

	Controller createController(final ClientConfig clientConfig) {
		val controllerConfig = ControllerImplConfig
			.builder().clientConfig(clientConfig).maxBackoffMillis(MAX_BACKOFF_MILLIS).build();
		return new ControllerImpl(controllerConfig, executor);
	}

	ConnectionFactory createConnectionFactory(final ClientConfig clientConfig) {
		val connPool = new ConnectionPoolImpl(clientConfig);
		return new ConnectionFactoryImpl(clientConfig, connPool, executor);
	}

	<T> Queue<T> createInstancePool(final Object ignored) {
		return new ArrayBlockingQueue<>(INSTANCE_POOL_SIZE);
	}

	<K, V> Map<K, V> createInstanceCache(final Object ignored) {
		return new ConcurrentHashMap<>();
	}

	@Override
	public final int activeOpCount() {
		if (concurrencyLimit > 0) {
			return concurrencyLimit - concurrencyThrottle.availablePermits();
		} else {
			return Integer.MAX_VALUE - concurrencyThrottle.availablePermits();
		}
	}

	@Override
	public final long scheduledOpCount() {
		return scheduledOpCount.sum();
	}

	@Override
	public final long completedOpCount() {
		return completedOpCount.sum();
	}

	@Override
	public final boolean isIdle() {
		if (concurrencyLimit > 0) {
			return !concurrencyThrottle.hasQueuedThreads()
				&& concurrencyThrottle.availablePermits() >= concurrencyLimit;
		} else {
			return concurrencyThrottle.availablePermits() == Integer.MAX_VALUE;
		}
	}

	/**
	 * Not used in this driver implementation
	 */
	@Override
	protected String requestNewPath(final String path) {
		throw new AssertionError("Should not be invoked");
	}

	/**
	 * Not used in this driver implementation
	 */
	@Override
	protected String requestNewAuthToken(final Credential credential) {
		throw new AssertionError("Should not be invoked");
	}

	@Override
	public List<I> list(
					final ItemFactory<I> itemFactory,
					final String path,
					final String prefix,
					final int idRadix,
					final I lastPrevItem,
					final int count)
					throws EOFException {
		final List<I> items;
		if(BYTES.equals(streamDataType)) {
			items = listStreams(itemFactory, path, prefix, idRadix, lastPrevItem, count);
		} else {
			items = makeEventItems(itemFactory, path, prefix, lastPrevItem, 2);
		}
		return items;
	}

	List<I> listStreams(
		final ItemFactory<I> itemFactory, final String path, final String prefix, final int idRadix,
		final I lastPrevItem, final int count
	) throws EOFException {

		val endpointUri = endpointCache.computeIfAbsent(endpointAddrs[0], this::createEndpointUri);
		val clientConfig = clientConfigCache.computeIfAbsent(endpointUri, this::createClientConfig);
		val controller = controllerCache.computeIfAbsent(clientConfig, this::createController);

		if(streamIterator == null) {
			val scopeName = path.startsWith(SLASH) ? path.substring(1) : path;
			streamIterator = controller.listStreams(scopeName);
		}

		final int prefixLength = (prefix == null || prefix.isEmpty()) ? 0 : prefix.length();
		final List<I> streamItems = new ArrayList<>(count);
		var i = 0;
		try {
			while(i < count) {
				val stream = streamIterator.getNext().get(controlApiTimeoutMillis, MILLISECONDS);
				if(null == stream) {
					if(i == 0) {
						streamIterator = null;
						throw new EOFException("End of stream listing");
					} else {
						break;
					}
				} else {
					val streamName = stream.getStreamName();
					if(prefixLength > 0) {
						if(streamName.startsWith(prefix)) {
							val streamItem = makeStreamItem(
								clientConfig, controller, streamName, idRadix, stream.getScope(), itemFactory
							);
							streamItems.add(streamItem);
							i ++;
						}
					} else {
						val streamItem = makeStreamItem(
							clientConfig, controller, streamName, idRadix, stream.getScope(), itemFactory
						);
						streamItems.add(streamItem);
						i ++;
					}
				}
			}
		} catch(final InterruptedException e) {
			throwUnchecked(e);
		} catch(final ExecutionException e) {
			LogUtil.exception(Level.WARN, e, "{}: scope \"{}\" streams listing failure", stepId, scopeName);
		} catch(final TimeoutException e) {
			LogUtil.exception(Level.WARN, e, "{}: scope \"{}\" streams listing timeout", stepId, scopeName);
		}
		return streamItems;
	}

	I makeStreamItem(
		final ClientConfig clientConfig, final Controller controller, final String streamName, final int idRadix,
		final String scopeName, final ItemFactory<I> itemFactory
	) {
		val connFactory = connFactoryCache.computeIfAbsent(clientConfig, this::createConnectionFactory);
		val clientFactoryCreateFunc = byteStreamClientCreateFuncCache.computeIfAbsent(
			connFactory, ByteStreamClientFactoryCreateFunctionImpl::new
		);
		val byteStreamReaderPool = byteStreamReaderPoolCache.computeIfAbsent(
			streamName, this::createInstancePool
		);
		var byteStreamReader_ = byteStreamReaderPool.poll();
		if(null == byteStreamReader_) {
			val clientFactory = byteStreamClientFactoryCache.computeIfAbsent(controller, clientFactoryCreateFunc);
			val byteStreamReaderCreateFunc = byteStreamReaderCreateFuncCache.computeIfAbsent(
				clientFactory, ByteStreamReaderCreateFunctionImpl::new
			);
			byteStreamReader_ = byteStreamReaderCreateFunc.apply(streamName);
		}
		val byteStreamReader = byteStreamReader_;
		val byteStreamSize = byteStreamReader.fetchTailOffset();
		val offset = Long.parseLong(streamName, idRadix);
		return itemFactory.getItem(SLASH + scopeName + SLASH + streamName, offset, byteStreamSize);
	}

	List<I> makeEventItems(
		final ItemFactory<I> itemFactory, final String path, final String prefix, final I lastPrevItem, final int count
	) throws EOFException {
		if(null != lastPrevItem) {
			throw new EOFException();
		}
		val items = new ArrayList<I>();
		for(var i = 0; i < count; i ++) {
			items.add(itemFactory.getItem(path + SLASH + (prefix == null ? i : prefix + i), 0, 0));
		}
		return items;
	}

	/**
	 * Not used in this driver implementation
	 */
	@Override
	public void adjustIoBuffers(final long avgTransferSize, final OpType opType) {}

	@Override
	protected boolean prepare(final O operation) {
		super.prepare(operation);
		var endpointAddr = operation.nodeAddr();
		if (endpointAddr == null) {
			endpointAddr = nextEndpointAddr();
			operation.nodeAddr(endpointAddr);
		}
		return true;
	}

	@Override
	public final boolean put(final O op)
					throws IllegalStateException {
		if (prepare(op) && concurrencyThrottle.tryAcquire()) {
			val opType = op.type();
			if (NOOP.equals(opType)) {
				submitNoop(op);
			} else {
				final String nodeAddr = op.nodeAddr();
				switch (streamDataType) {
				case EVENTS:
					submitEventOperation(op, nodeAddr);
					break;
				case BYTES:
					submitByteStreamOperation(op, nodeAddr);
					break;
				default:
					throw new AssertionError("Unexpected stream data type: " + streamDataType);
				}
			}
			scheduledOpCount.increment();
			return true;
		} else {
			return false;
		}
	}

	@Override
	public final int put(final List<O> ops, final int from, final int to)
					throws IllegalStateException {
		if(createBatchMode) {
			val op = ops.get(from);
			val opType = op.type();
			if(CREATE.equals(opType)) {
				return submitBatchEventCreate(ops, from, to);
			} else {
				return submitEach(ops, from, to);
			}
		} else {
			return submitEach(ops, from, to);
		}
	}

	@Override
	public final int put(final List<O> ops)
					throws IllegalStateException {
		return put(ops, 0, ops.size());
	}

	void submitNoop(final O op) {
		op.startRequest();
		completeOperation(op, SUCC);
	}

	void submitEventOperation(final O op, final String nodeAddr) {
		val type = op.type();
		switch (type) {
		case CREATE:
			submitEventCreateOperation(op, nodeAddr);
			break;
		case READ:
			submitEventReadOperation(op, nodeAddr);
			break;
		default:
			throw new AssertionError("Unsupported event operation type: " + type);
		}
	}

	void submitByteStreamOperation(final O op, final String nodeAddr) {
		val type = op.type();
		switch (type) {
		case CREATE:
			submitStreamCreateOperation(op, nodeAddr);
			break;
		case READ:
			submitStreamReadOperation(op, nodeAddr);
			break;
		case UPDATE:
			submitStreamAppendOperation(op, nodeAddr);
			break;
		case DELETE:
			submitStreamDeleteOperation(op, nodeAddr);
			break;
		default:
			throw new AssertionError("Unsupported byte stream operation type: " + type);
		}
	}

	int submitEach(final List<O> ops, final int from, final int to) {
		for (var i = from; i < to; i++) {
			if (!put(ops.get(i))) {
				return i - from;
			}
		}
		return to - from;
	}

	int submitBatchEventCreate(final List<O> ops, final int from, final int to) {
		var submitCount = 0;
		if (from < to && concurrencyThrottle.tryAcquire()) {
			try {
				val anyEvtOp = ops.get(from);
				val nodeAddr = anyEvtOp.nodeAddr();
				// prepare
				val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
				val clientConfig = clientConfigCache.computeIfAbsent(endpointUri, this::createClientConfig);
				val controller = controllerCache.computeIfAbsent(clientConfig, this::createController);
				val scopeCreateFunc = scopeCreateFuncCache.computeIfAbsent(controller, ScopeCreateFunctionImpl::new);
				// create the scope if necessary
				val streamCreateFunc = streamCreateFuncCache.computeIfAbsent(scopeName, scopeCreateFunc);
				val streamName = extractStreamName(anyEvtOp.dstPath());
				scopeStreamsCache
					.computeIfAbsent(scopeName, this::createInstanceCache)
					.computeIfAbsent(streamName, streamCreateFunc);
				// create the client factory create function if necessary
				val clientFactoryCreateFunc = clientFactoryCreateFuncCache.computeIfAbsent(clientConfig,
					EventStreamClientFactoryCreateFunctionImpl::new
				);
				// create the client factory if necessary
				val clientFactory = clientFactoryCache.computeIfAbsent(scopeName, clientFactoryCreateFunc);
				// create the batch event stream writer if necessary
				val batchEvtWriterPool = batchEvtWriterPoolCache.computeIfAbsent(clientFactory, this::createInstancePool);
				var batchEvtWriter_ = batchEvtWriterPool.poll();
				if(null == batchEvtWriter_) {
					batchEvtWriter_ =
						clientFactory.createTransactionalEventWriter(streamName, evtSerializer, evtWriterConfig);
				}
				val batchEvtWriter = batchEvtWriter_;
				val txn = batchEvtWriter.beginTxn();
				O evtOp;
				I evtItem;
				var routingKey = (String) null;
				try {
					if(null == routingKeyFunc) {
						for(var i = from; i < to; i++) {
							evtOp = ops.get(i);
							prepare(evtOp);
							evtOp.startRequest();
							txn.writeEvent(evtOp.item());
						}
					} else {
						for(var i = from; i < to; i++) {
							evtOp = ops.get(i);
							prepare(evtOp);
							evtItem = evtOp.item();
							routingKey = routingKeyFunc.apply(evtItem);
							evtOp.startRequest();
							txn.writeEvent(routingKey, evtItem);
						}
					}
					scheduledOpCount.add(to - from);
					txn.commit();
					completeOperations(ops, from, to, SUCC);
					submitCount = to - from;
				} catch(final TxnFailedException e) {
					LogUtil.exception(
						Level.DEBUG, e, "{}: transaction failure, aborting {} events write", stepId, to - from
					);
					completeOperations(ops, from, to, RESP_FAIL_UNKNOWN);
					txn.abort();
				} finally {
					batchEvtWriterPool.offer(batchEvtWriter);
				}
			} catch(final Throwable e) {
				LogUtil.exception(
					Level.DEBUG, e, "{}: unexpected failure while trying to write {} events", stepId, to - from
				);
				completeOperations(ops, from, to, FAIL_UNKNOWN);
			}
		}
		return submitCount;
	}

	void submitEventCreateOperation(final O evtOp, final String nodeAddr) {
		try {
			// prepare
			val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
			val clientConfig = clientConfigCache.computeIfAbsent(endpointUri, this::createClientConfig);
			val controller = controllerCache.computeIfAbsent(clientConfig, this::createController);
			val scopeCreateFunc = scopeCreateFuncCache.computeIfAbsent(controller, ScopeCreateFunctionImpl::new);
			// create the scope if necessary
			val streamCreateFunc = streamCreateFuncCache.computeIfAbsent(scopeName, scopeCreateFunc);
			val streamName = extractStreamName(evtOp.dstPath());
			scopeStreamsCache
							.computeIfAbsent(scopeName, this::createInstanceCache)
							.computeIfAbsent(streamName, streamCreateFunc);
			// create the client factory create function if necessary
			val clientFactoryCreateFunc = clientFactoryCreateFuncCache.computeIfAbsent(
							clientConfig, EventStreamClientFactoryCreateFunctionImpl::new);
			// create the client factory if necessary
			val clientFactory = clientFactoryCache.computeIfAbsent(scopeName, clientFactoryCreateFunc);
			// create the event stream writer if necessary
			val evtWriterPool = evtWriterPoolCache.computeIfAbsent(streamName, this::createInstancePool);
			var evtWriter_ = evtWriterPool.poll();
			if (null == evtWriter_) {
				evtWriter_ = clientFactory.createEventWriter(streamName, evtSerializer, evtWriterConfig);
			}
			val evtWriter = evtWriter_;
			val evtItem = evtOp.item();
			// submit the event writing
			final CompletionStage<Void> writeEvtFuture;
			if(null == routingKeyFunc) {
				evtOp.startRequest();
				writeEvtFuture = evtWriter.writeEvent(evtItem);
				try {
					evtOp.finishRequest();
				} catch(final IllegalStateException ignored) {
				}
			} else {
				val routingKey = routingKeyFunc.apply(evtItem);
				evtOp.startRequest();
				writeEvtFuture = evtWriter.writeEvent(routingKey, evtItem);
				try {
					evtOp.finishRequest();
				} catch(final IllegalStateException ignored) {
				}
			}
			writeEvtFuture.handle(
							(returned, thrown) -> {
								evtWriterPool.offer(evtWriter);
								if (null == thrown) {
									evtOp.startResponse();
									evtOp.finishResponse();
									try {
										evtOp.countBytesDone(evtItem.size());
									} catch (final IOException ignored) {}
									return completeOperation(evtOp, SUCC);
								} else {
									return completeFailedOperation(evtOp, thrown);
								}
							});
		} catch (final NullPointerException e) {
			if (!isStarted()) { // occurs on manual interruption which is normal so should be handled
				completeOperation(evtOp, INTERRUPTED);
			} else {
				completeFailedOperation(evtOp, e);
			}
		} catch (final Throwable thrown) {
			throwUncheckedIfInterrupted(thrown);
			completeFailedOperation(evtOp, thrown);
		}
	}

	void submitEventReadOperation(final O evtOp, final String nodeAddr) {
		try {
			val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
			val streamName = extractStreamName(evtOp.dstPath());
			val readerGroupConfigBuilder = evtReaderGroupConfigBuilder.get();
			val readerGroupConfig = evtReaderGroupConfigCache.computeIfAbsent(
				scopeName + SLASH + streamName, key -> readerGroupConfigBuilder.stream(key).build()
			);
			val readerGroupManagerCreateFunc = evtReaderGroupManagerCreateFuncCache.computeIfAbsent(
							endpointUri, ReaderGroupManagerCreateFunctionImpl::new);
			evtReaderGroupManagerCache.computeIfAbsent(
				scopeName, key -> {
					val readerGroupManager = readerGroupManagerCreateFunc.apply(key);
					readerGroupManager.createReaderGroup(evtReaderGroupName, readerGroupConfig);
					return readerGroupManager;
				}
			);
			val clientConfig = clientConfigCache.computeIfAbsent(endpointUri, this::createClientConfig);
			val clientFactoryCreateFunc = clientFactoryCreateFuncCache.computeIfAbsent(
							clientConfig, EventStreamClientFactoryCreateFunctionImpl::new);
			val clientFactory = clientFactoryCache.computeIfAbsent(scopeName, clientFactoryCreateFunc);
			val readerCreateFunc = eventStreamReaderCreateFuncCache.computeIfAbsent(
							clientFactory, ReaderCreateFunctionImpl::new);
			val evtReader = eventStreamReaderCache.computeIfAbsent(evtReaderGroupName, readerCreateFunc);
			evtOp.startRequest();
			evtOp.finishRequest();
			val evtRead = evtReader.readNextEvent(readTimeoutMillis);
			evtOp.startResponse();
			evtOp.finishResponse();
			if(null == evtRead) {
				Loggers.MSG.info(
					"{}: no more events in the stream \"{}\" @ the scope \"{}\"", stepId, streamName, scopeName
				);
				completeOperation(evtOp, FAIL_TIMEOUT);
			} else {
				val evtData = evtRead.getEvent();
				if(null == evtData) {
					val streamPos = evtRead.getPosition();
					lastFailedStreamPosLock.lock();
					try {
						if(streamPos.equals(lastFailedStreamPos)) {
							Loggers.MSG.info("{}: no more events @ position {}", stepId, streamPos);
							completeOperation(evtOp, FAIL_TIMEOUT);
						} else {
							lastFailedStreamPos = streamPos;
							Loggers.ERR.warn("{}: corrupted event @ position {}", stepId, streamPos);
							completeOperation(evtOp, RESP_FAIL_CORRUPT);
						}
					} finally {
						lastFailedStreamPosLock.unlock();
					}
				} else {
					val bytesDone = evtData.remaining();
					val evtItem = evtOp.item();
					evtItem.size(bytesDone);
					evtOp.countBytesDone(evtItem.size());
					completeOperation(evtOp, SUCC);
				}
			}
		} catch (final Throwable thrown) {
			throwUncheckedIfInterrupted(thrown);
			completeFailedOperation(evtOp, thrown);
		}
	}

	void submitStreamCreateOperation(final O streamOp, final String nodeAddr) {
		try {
			val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
			val clientConfig = clientConfigCache.computeIfAbsent(endpointUri, this::createClientConfig);
			val controller = controllerCache.computeIfAbsent(clientConfig, this::createController);
			val scopeCreateFuncForStreamConfig = scopeCreateFuncForStreamConfigCache.computeIfAbsent(
							controller, ScopeCreateFunctionForStreamConfigImpl::new);
			val streamConfig = scopeStreamConfigsCache.computeIfAbsent(scopeName, scopeCreateFuncForStreamConfig);
			val streamName = extractStreamName(streamOp.item().name());
			streamOp.startRequest();
			controller
				.createStream(scopeName, streamName, streamConfig)
				.handle(
					(createdFlag, thrown) -> {
						streamOp.startResponse();
						if (null != thrown) {
							LogUtil.exception(
											Level.DEBUG,
											thrown,
											"Failed to create the stream {} in the scope {}",
											streamName,
											scopeName);
						}
						if(!createdFlag) {
							Loggers.ERR.warn(
								"{}: failed to create the stream \"{}\" in the scope \"{}\", may be existing before",
								stepId, streamName, scopeName
							);
							completeOperation(streamOp, RESP_FAIL_UNKNOWN);
						} else {
							handleByteStreamWrite(controller, clientConfig, streamName, streamOp);
						}
						return createdFlag;
					}
				);
			try {
				streamOp.finishRequest();
			} catch(final IllegalStateException ignored) {
			}
		} catch (final NullPointerException e) {
			if (!isStarted()) {
				completeOperation(streamOp, INTERRUPTED);
			} else {
				completeFailedOperation(streamOp, e);
			}
		} catch (final Throwable thrown) {
			throwUncheckedIfInterrupted(thrown);
			completeFailedOperation(streamOp, thrown);
		}
	}

	void submitStreamReadOperation(final O streamOp, final String nodeAddr) {
		val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
		val clientConfig = clientConfigCache.computeIfAbsent(endpointUri, this::createClientConfig);
		val controller = controllerCache.computeIfAbsent(clientConfig, this::createController);
		val streamItem = streamOp.item();
		val streamName = extractStreamName(streamItem.name());
		try {
			var remainingBytes = streamItem.size();
			if (remainingBytes > 0) {
				val connFactory = connFactoryCache.computeIfAbsent(clientConfig, this::createConnectionFactory);
				val clientFactoryCreateFunc = byteStreamClientCreateFuncCache.computeIfAbsent(
					connFactory, ByteStreamClientFactoryCreateFunctionImpl::new
				);
				val byteStreamReaderPool = byteStreamReaderPoolCache.computeIfAbsent(
					streamName, this::createInstancePool
				);
				var byteStreamReader_ = byteStreamReaderPool.poll();
				if(null == byteStreamReader_) {
					val clientFactory = byteStreamClientFactoryCache.computeIfAbsent(controller, clientFactoryCreateFunc);
					val byteStreamReaderCreateFunc = byteStreamReaderCreateFuncCache.computeIfAbsent(
						clientFactory, ByteStreamReaderCreateFunctionImpl::new
					);
					byteStreamReader_ = byteStreamReaderCreateFunc.apply(streamName);
				}
				val byteStreamReader = byteStreamReader_;
				streamOp.startRequest();
				byteStreamReader
					.onDataAvailable()
					.handle(
						(availableByteCount, thrown) -> {
							streamOp.startResponse();
							streamOp.startDataResponse();
							return handleByteStreamRead(
								streamOp, byteStreamReaderPool, byteStreamReader, availableByteCount, thrown
							);
						}
					);
				try {
					streamOp.finishRequest();
				} catch(final IllegalStateException ignored) {
				}
			}
		} catch (final IOException e) {
			LogUtil.exception(Level.DEBUG, e, "Failed to read the bytes stream {}", streamName);
			completeOperation(streamOp, FAIL_IO);
		} catch(final Throwable e) {
			throwUncheckedIfInterrupted(e);
			LogUtil.exception(Level.WARN, e, "Failed to read the bytes stream {}", streamName);
			completeOperation(streamOp, FAIL_UNKNOWN);
		}
	}

	void submitStreamAppendOperation(final O streamOp, final String nodeAddr) {
		// TODO
	}

	void submitStreamDeleteOperation(final O streamOp, final String nodeAddr) {
		try {
			val streamName = extractStreamName(streamOp.item().name());
			val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
			val clientConfig = clientConfigCache.computeIfAbsent(endpointUri, this::createClientConfig);
			val controller = controllerCache.computeIfAbsent(clientConfig, this::createController);
			controller
				.sealStream(scopeName, streamName)
				.handle(
					(result, thrown) -> {
						if (null != thrown) {
							LogUtil.exception(
											Level.DEBUG,
											thrown,
											"Failed to seal the stream {} in the scope {}",
											streamName,
											scopeName);
						}
						if (!result) {
							Loggers.ERR.debug(
											"Failed to seal the stream {} in the scope {}", streamName, scopeName);
						}
						return handleStreamSealBeforeDeletion(streamName, controller, streamOp);
					}
				);
		} catch (final NullPointerException e) {
			if (!isStarted()) {
				completeOperation(streamOp, INTERRUPTED);
			} else {
				completeFailedOperation(streamOp, e);
			}
		} catch (final Throwable cause) {
			throwUncheckedIfInterrupted(cause);
			completeFailedOperation(streamOp, cause);
		}
	}

	void completeOperations(final List<O> ops, final int from, final int to, final Status status) {
		concurrencyThrottle.release();
		completedOpCount.add(to - from);
		I item;
		O op;
		try {
			for(var i = from; i < to; i++) {
				op = ops.get(i);
				op.status(status);
				item = op.item();
				op.countBytesDone(item.size());
				op.finishRequest();
				op.startResponse();
				op.finishResponse();
				handleCompleted(op);
			}
		} catch(final IOException ignored) {
		}
	}

	boolean completeOperation(final O op, final Status status) {
		concurrencyThrottle.release();
		completedOpCount.increment();
		op.status(status);
		return handleCompleted(op);
	}

	boolean completeFailedOperation(final O op, final Throwable thrown) {
		LogUtil.exception(Level.DEBUG, thrown, "{}: unexpected load operation failure", stepId);
		return completeOperation(op, FAIL_UNKNOWN);
	}

	boolean handleStreamSealBeforeDeletion(
					final String streamName, final Controller controller, final O streamOp) {
		streamOp.startRequest();
		val deleteFuture = controller.deleteStream(scopeName, streamName);
		try {
			streamOp.finishRequest();
		} catch(final IllegalStateException ignored) {
		}
		deleteFuture.handle(
						(result, thrown) -> {
							if (null == thrown) {
								streamOp.startResponse();
								streamOp.finishResponse();
								completeOperation(streamOp, SUCC);
							} else {
								completeFailedOperation(streamOp, thrown);
							}
							return result;
						});
		return true;
	}

	/**
	 * @param controller  pravega controller instance
	 * @param clientConfig pravega client config instance
	 * @param streamName  stream name
	 * @param streamOp    stream operation instance
	 */
	void handleByteStreamWrite(
		final Controller controller, final ClientConfig clientConfig, final String streamName, final O streamOp
	) {
		val streamItem = streamOp.item();
		try {
			var remainingBytes = streamItem.size();
			if (remainingBytes > 0) {
				val connFactory = connFactoryCache.computeIfAbsent(clientConfig, this::createConnectionFactory);
				val clientFactoryCreateFunc = byteStreamClientCreateFuncCache.computeIfAbsent(
					connFactory, ByteStreamClientFactoryCreateFunctionImpl::new
				);
				val clientFactory = byteStreamClientFactoryCache.computeIfAbsent(controller, clientFactoryCreateFunc);
				val byteStreamWriteChanPool = byteStreamWriteChanPoolCache.computeIfAbsent(
					clientFactory, this::createInstancePool
				);
				var countBytesDone = 0L;
				var n = 0L;
				val byteStreamWriteChan = ByteStreamWriteChannel.newOrReuseInstance(
					clientFactory, streamName, byteStreamWriteChanPool.poll()
				);
				try {
					while (remainingBytes > 0) {
						n = streamItem.writeToSocketChannel(byteStreamWriteChan, remainingBytes);
						if(0 < countBytesDone) {
							streamOp.startDataResponse();
						}
						countBytesDone += n;
						remainingBytes -= n;
					}
				} finally {
					streamOp.finishResponse();
					byteStreamWriteChan.close();
					byteStreamWriteChanPool.offer(byteStreamWriteChan);
					streamOp.countBytesDone(countBytesDone);
					streamItem.size(countBytesDone);
				}
			}
			completeOperation(streamOp, SUCC);
		} catch (final IOException e) {
			LogUtil.exception(Level.DEBUG, e, "Failed to write the bytes stream {}", streamName);
			completeOperation(streamOp, FAIL_IO);
		} catch(final Throwable e) {
			throwUncheckedIfInterrupted(e);
			LogUtil.exception(Level.WARN, e, "Failed to write the bytes stream {}", streamName);
			completeOperation(streamOp, FAIL_UNKNOWN);
		}
	}

	int handleByteStreamRead(
		final O streamOp, final Queue<ByteStreamReader> readerPool, final ByteStreamReader reader,
		final int availableByteCount, final Throwable thrown
	) {
		if(thrown == null) {
			val buff = DirectMemUtil.getThreadLocalReusableBuff(availableByteCount);
			try {
				val n = reader.read(buff);
				if(n > 0) {
					val transferredByteCount = streamOp.countBytesDone() + n;
					streamOp.countBytesDone(transferredByteCount);
					val expectedStreamSize = streamOp.item().size();
					if(expectedStreamSize > transferredByteCount) {
						reader
							.onDataAvailable()
							.handle(
								(availableByteCount_, thrown_) ->
									handleByteStreamRead(streamOp, readerPool, reader, availableByteCount_, thrown_)
							);
					} else {
						completeByteStreamRead(streamOp, readerPool, reader, null);
					}
				} else { // end of byte stream
					completeByteStreamRead(streamOp, readerPool, reader, null);
				}
			} catch(final Throwable e) {
				completeByteStreamRead(streamOp, readerPool, reader, e);
			}
		} else {
			completeByteStreamRead(streamOp, readerPool, reader, thrown);
		}
		return availableByteCount;
	}

	void completeByteStreamRead(
		final O streamOp, final Queue<ByteStreamReader> readerPool, final ByteStreamReader reader, final Throwable e
	) {
		streamOp.finishResponse();
		readerPool.offer(reader);
		if(null == e) {
			completeOperation(streamOp, SUCC);
		} else if(e instanceof IOException) {
			LogUtil.exception(Level.DEBUG, e, "{}: failure", streamOp);
			completeOperation(streamOp, FAIL_IO);
		} else if(e instanceof InterruptedException) {
			completeOperation(streamOp, INTERRUPTED);
			throwUnchecked(e);
		} else {
			LogUtil.exception(Level.WARN, e, "{}: failure", streamOp);
			completeOperation(streamOp, FAIL_UNKNOWN);
		}
	}

	@Override
	protected void doClose()
					throws IOException {
		super.doClose();
		// clear all caches & pools
		val allEvtWriters = new ArrayList<AutoCloseable>();
		evtWriterPoolCache
			.values()
			.forEach(
				pool -> {
					allEvtWriters.addAll(pool);
					pool.clear();
				}
			);
		closeAllWithTimeout(allEvtWriters);
		evtWriterPoolCache.clear();
		evtReaderGroupManagerCreateFuncCache.clear();
		closeAllWithTimeout(evtReaderGroupManagerCache.values());
		evtReaderGroupManagerCache.clear();
		closeAllWithTimeout(eventStreamReaderCreateFuncCache.keySet());
		eventStreamReaderCreateFuncCache.clear();
		closeAllWithTimeout(eventStreamReaderCache.values());
		eventStreamReaderCache.clear();
		scopeStreamConfigsCache.clear();
		closeAllWithTimeout(connFactoryCache.values());
		connFactoryCache.clear();
		byteStreamClientCreateFuncCache.clear();
		closeAllWithTimeout(byteStreamClientFactoryCache.values());
		byteStreamClientFactoryCache.clear();
		val allByteStreamWriteChans = new ArrayList<AutoCloseable>();
		byteStreamWriteChanPoolCache
			.values()
			.forEach(
				pool -> {
					allByteStreamWriteChans.addAll(pool);
					pool.clear();
				}
			);
		byteStreamWriteChanPoolCache.clear();
		closeAllWithTimeout(allByteStreamWriteChans);
		byteStreamReaderCreateFuncCache.clear();
		val allByteStreamReaders = new ArrayList<AutoCloseable>();
		byteStreamReaderPoolCache
			.values()
			.forEach(
				pool -> {
					allByteStreamReaders.addAll(pool);
					pool.clear();
				}
			);
		byteStreamReaderPoolCache.clear();
		//
		scopeCreateFuncCache.clear();
		streamCreateFuncCache.clear();
		scopeStreamsCache.values().forEach(Map::clear);
		scopeStreamsCache.clear();
		clientFactoryCreateFuncCache.clear();
		closeAllWithTimeout(clientFactoryCache.values());
		clientFactoryCache.clear();
		//
		closeAllWithTimeout(allByteStreamReaders);
		clientConfigCache.clear();
		//
		closeAllWithTimeout(controllerCache.values());
		controllerCache.clear();
		//
		endpointCache.clear();
		//
		executor.shutdownNow();
	}

	void closeAllWithTimeout(final Collection<? extends AutoCloseable> closeables) {
		if (null != closeables && closeables.size() > 0) {
			final ExecutorService closeExecutor = Executors.newFixedThreadPool(closeables.size());
			closeables.forEach(
							closeable -> closeExecutor.submit(
											() -> {
												try {
													closeable.close();
												} catch (final Exception e) {
													throwUncheckedIfInterrupted(e);
													LogUtil.exception(
																	Level.WARN,
																	e,
																	"{}: storage driver failed to close \"{}\"",
																	stepId,
																	closeable);
												}
											}));
			try {
				if (!closeExecutor.awaitTermination(controlApiTimeoutMillis, MILLISECONDS)) {
					Loggers.ERR.warn(
									"{}: storage driver timeout while closing one of \"{}\"",
									stepId,
									closeables.stream().findFirst().get().getClass().getSimpleName());
				}
			} catch (final InterruptedException e) {
				throwUnchecked(e);
			} finally {
				closeExecutor.shutdownNow();
			}
		}
	}

	static String extractStreamName(final String itemPath) {
		String result = itemPath;
		if (result.startsWith(SLASH)) {
			result = result.substring(1);
		}
		if (result.endsWith(SLASH) && result.length() > 1) {
			result = result.substring(0, result.length() - 1);
		}
		return result;
	}

	@Override
	public String toString() {
		return String.format(super.toString(), DRIVER_NAME);
	}
}
